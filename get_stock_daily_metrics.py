import argparse
import json
import random
import sys
import time
from datetime import datetime
from pathlib import Path

import akshare as ak
import pandas as pd
import requests
from openpyxl import Workbook


DEFAULT_START_DATE = "20260511"
DEFAULT_END_DATE = "20260512"
FULL_START_DATE = "20050101"
FULL_END_DATE = "20260512"

DATE_COLUMN = "日期"
CODE_COLUMN = "股票代码"
NAME_COLUMN = "股票名称"

PRICE_METRICS = {
    "成交额": "成交额",
    "开盘价": "开盘",
    "收盘价": "收盘",
    "最高价": "最高",
    "最低价": "最低",
}
VALUE_METRICS = {
    # 东方财富历史估值接口字段名为“流通市值”；这里按用户需求输出为“流动市值”。
    "流动市值": "流通市值",
    # 东方财富实时快照有“市盈率-动态”，历史估值接口可用字段为 PE(TTM)。
    "市盈率-动态": "PE(TTM)",
}
METRIC_CONFIG = {
    **{name: ("price", source) for name, source in PRICE_METRICS.items()},
    **{name: ("value", source) for name, source in VALUE_METRICS.items()},
}


def configure_stdout():
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass


def format_duration(seconds):
    seconds = int(max(0, seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def parse_args():
    parser = argparse.ArgumentParser(
        description="按个股清单下载日行情和估值指标，并生成横向矩阵 Excel。"
    )
    parser.add_argument("--stock-list", default="清单2.xlsx", help="个股清单 Excel 路径")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="开始日期, 例如 20050101")
    parser.add_argument("--end-date", default=DEFAULT_END_DATE, help="结束日期, 例如 20260512")
    parser.add_argument(
        "--cache-dir",
        default="stock_daily_metrics_cache",
        help="个股日行情和估值缓存目录",
    )
    parser.add_argument(
        "--output-dir",
        default="",
        help="输出目录；为空时自动创建带时间戳的目录",
    )
    parser.add_argument(
        "--min-wait",
        type=float,
        default=1.0,
        help="未命中缓存时，每次接口请求后的最小随机等待秒数",
    )
    parser.add_argument(
        "--max-wait",
        type=float,
        default=3.0,
        help="未命中缓存时，每次接口请求后的最大随机等待秒数",
    )
    parser.add_argument(
        "--chunk-min-wait",
        type=float,
        default=0.05,
        help="腾讯行情分块请求之间的最小等待秒数；不要和股票级等待混用",
    )
    parser.add_argument(
        "--chunk-max-wait",
        type=float,
        default=0.2,
        help="腾讯行情分块请求之间的最大等待秒数；不要和股票级等待混用",
    )
    parser.add_argument("--max-attempts", type=int, default=3, help="接口失败重试次数")
    parser.add_argument("--timeout", type=float, default=15, help="单次 HTTP 请求超时时间")
    parser.add_argument(
        "--price-source",
        choices=["auto", "eastmoney", "tencent"],
        default="tencent",
        help="日行情来源；默认腾讯，auto 表示优先东财，失败后切到腾讯",
    )
    parser.add_argument("--force-refresh", action="store_true", help="忽略已有缓存重新请求")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="仅处理清单前 N 只股票；0 表示处理全部，用于小范围验证",
    )
    return parser.parse_args()


def normalize_stock_code(value):
    if pd.isna(value):
        return ""

    code = str(value).strip().upper()
    if not code:
        return ""

    if code.endswith(".0") and code[:-2].isdigit():
        code = code[:-2]

    if "." in code:
        number, suffix = code.split(".", 1)
        if number.isdigit():
            return f"{number.zfill(6)}.{suffix}"
        return code

    if code.isdigit():
        number = code.zfill(6)
        if number.startswith(("60", "68", "51", "52", "56", "58")):
            return f"{number}.SH"
        if number.startswith(("00", "30", "15", "16", "18")):
            return f"{number}.SZ"
        if number.startswith(("8", "9", "4")):
            return f"{number}.BJ"
        return number

    return code


def code_to_symbol(code):
    return code.split(".", 1)[0]


def load_stock_codes(stock_list_path, limit):
    row = pd.read_excel(stock_list_path, header=None, nrows=1).iloc[0]
    codes = [normalize_stock_code(value) for value in row.tolist()]
    codes = [code for code in codes if code]
    if limit and limit > 0:
        codes = codes[:limit]
    if not codes:
        raise ValueError(f"{stock_list_path} 第一行没有读取到有效证券代码")
    return codes


def get_trade_dates(start_date, end_date):
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    calendar_df = ak.tool_trade_date_hist_sina()
    dates = pd.to_datetime(calendar_df["trade_date"])
    dates = [date for date in dates if start <= date <= end]

    if start == end and not dates:
        dates = [start]
    if not dates:
        raise ValueError(f"{start_date}-{end_date} 内没有交易日")
    return dates


def sleep_after_request(args):
    sleep_random(args.min_wait, args.max_wait)


def sleep_between_chunks(args):
    sleep_random(args.chunk_min_wait, args.chunk_max_wait)


def sleep_random(min_value, max_value):
    min_wait = max(0.0, min_value)
    max_wait = max(min_wait, max_value)
    if max_wait <= 0:
        return
    time.sleep(random.uniform(min_wait, max_wait))


def fetch_with_retry(fetcher, label, args):
    last_error = None
    for attempt in range(1, args.max_attempts + 1):
        try:
            return fetcher()
        except Exception as exc:
            last_error = exc
            print(f"{label} 第 {attempt} 次失败: {exc}", flush=True)
            if attempt < args.max_attempts:
                sleep_after_request(args)
    raise RuntimeError(f"{label} 获取失败，最后错误: {last_error}")


def name_cache_path(cache_dir):
    return Path(cache_dir) / "names" / "stock_name_lookup.pkl"


def build_name_map_from_lookup(codes, lookup):
    a_lookup = lookup.get("a", {})
    bj_lookup = lookup.get("bj", {})
    name_map = {}
    for code in codes:
        symbol = code_to_symbol(code)
        if code.endswith(".BJ"):
            name = bj_lookup.get(symbol) or a_lookup.get(symbol)
        else:
            name = a_lookup.get(symbol)
        if name:
            name_map[code] = name
    return name_map


def load_or_fetch_name_map(codes, args, failures):
    cache_path = name_cache_path(args.cache_dir)
    if cache_path.exists() and not args.force_refresh:
        return build_name_map_from_lookup(codes, pd.read_pickle(cache_path))

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    lookup = {"a": {}, "bj": {}}

    def add_names(df, code_col, name_col, market):
        if df is None or df.empty or code_col not in df.columns or name_col not in df.columns:
            return
        for raw_code, raw_name in zip(df[code_col], df[name_col]):
            symbol = str(raw_code).strip().zfill(6)
            name = str(raw_name).strip()
            if not name or name.lower() == "nan":
                continue
            lookup[market][symbol] = name

    try:
        df = fetch_with_retry(ak.stock_info_a_code_name, "A股代码名称表", args)
        add_names(df, "code", "name", market="a")
        sleep_after_request(args)
    except Exception as exc:
        failures.append({"代码": "ALL", "接口": "stock_info_a_code_name", "错误": str(exc)})

    try:
        bj_df = fetch_with_retry(ak.stock_info_bj_name_code, "北交所代码名称表", args)
        add_names(bj_df, "证券代码", "证券简称", market="bj")
        sleep_after_request(args)
    except Exception as exc:
        failures.append({"代码": "ALL", "接口": "stock_info_bj_name_code", "错误": str(exc)})

    pd.to_pickle(lookup, cache_path)
    return build_name_map_from_lookup(codes, lookup)


def price_cache_path(cache_dir, code, start_date, end_date):
    return Path(cache_dir) / "price" / f"{code}_{start_date}_{end_date}.pkl"


def value_cache_path(cache_dir, code):
    return Path(cache_dir) / "value" / f"{code}.pkl"


def normalize_price_df(df):
    output_columns = [DATE_COLUMN] + list(PRICE_METRICS.values())
    if df is None or df.empty:
        return pd.DataFrame(columns=output_columns)

    missing_columns = [col for col in output_columns if col not in df.columns]
    if missing_columns:
        raise KeyError(f"日行情缺少字段: {', '.join(missing_columns)}")

    work_df = df[output_columns].copy()
    work_df[DATE_COLUMN] = pd.to_datetime(work_df[DATE_COLUMN], errors="coerce")
    work_df = work_df.dropna(subset=[DATE_COLUMN])
    for col in PRICE_METRICS.values():
        work_df[col] = pd.to_numeric(work_df[col], errors="coerce")
    work_df = (
        work_df.sort_values(DATE_COLUMN)
        .drop_duplicates(DATE_COLUMN, keep="last")
        .reset_index(drop=True)
    )
    return work_df


def fetch_price_eastmoney(code, args):
    symbol = code_to_symbol(code)
    raw_df = ak.stock_zh_a_hist(
        symbol=symbol,
        period="daily",
        start_date=args.start_date,
        end_date=args.end_date,
        adjust="",
        timeout=args.timeout,
    )
    return normalize_price_df(raw_df)


def tencent_symbol(code):
    symbol = code_to_symbol(code)
    if code.endswith(".SH"):
        return f"sh{symbol}"
    if code.endswith(".SZ"):
        return f"sz{symbol}"
    if code.endswith(".BJ"):
        return f"bj{symbol}"
    if symbol.startswith(("6", "5")):
        return f"sh{symbol}"
    if symbol.startswith(("8", "9", "4")):
        return f"bj{symbol}"
    return f"sz{symbol}"


def fetch_price_tencent_period(symbol, start_year, end_year, args):
    url = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
    params = {
        "_var": f"kline_day{start_year}",
        "param": f"{symbol},day,{start_year}-01-01,{end_year}-12-31,640,",
        "r": str(random.random()),
    }
    response = requests.get(url, params=params, timeout=args.timeout)
    response.raise_for_status()
    text = response.text
    json_text = text[text.find("={") + 1 :]
    data_json = json.loads(json_text)
    symbol_data = (data_json.get("data") or {}).get(symbol) or {}
    return symbol_data.get("day") or []


def fetch_price_tencent(code, args):
    symbol = tencent_symbol(code)
    start = pd.to_datetime(args.start_date)
    end = pd.to_datetime(args.end_date)
    rows = []

    for start_year in range(start.year, end.year + 1, 2):
        end_year = min(start_year + 1, end.year)
        period_rows = fetch_with_retry(
            lambda sy=start_year, ey=end_year: fetch_price_tencent_period(symbol, sy, ey, args),
            f"{code} 腾讯日行情 {start_year}-{end_year}",
            args,
        )
        rows.extend(period_rows)
        sleep_between_chunks(args)

    if not rows:
        return pd.DataFrame(columns=[DATE_COLUMN] + list(PRICE_METRICS.values()))

    parsed_rows = []
    for row in rows:
        if len(row) < 9:
            continue
        parsed_rows.append(
            {
                DATE_COLUMN: row[0],
                "开盘": row[1],
                "收盘": row[2],
                "最高": row[3],
                "最低": row[4],
                # 腾讯字段第 9 位是成交额，单位万元；统一换算为元。
                "成交额": pd.to_numeric(row[8], errors="coerce") * 10000,
            }
        )

    work_df = pd.DataFrame(parsed_rows)
    work_df = normalize_price_df(work_df)
    return filter_date_range(work_df, args.start_date, args.end_date)


def load_or_fetch_price(code, args, failures):
    cache_path = price_cache_path(args.cache_dir, code, args.start_date, args.end_date)
    if cache_path.exists() and not args.force_refresh:
        cached_df = normalize_price_df(pd.read_pickle(cache_path))
        pd.to_pickle(cached_df, cache_path)
        return cached_df, "缓存"

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    errors = []

    if args.price_source in ("auto", "eastmoney"):
        try:
            df = fetch_with_retry(lambda: fetch_price_eastmoney(code, args), f"{code} 东财日行情", args)
            pd.to_pickle(df, cache_path)
            sleep_after_request(args)
            return df, "请求-eastmoney"
        except Exception as exc:
            errors.append(f"eastmoney: {exc}")
            if args.price_source == "eastmoney":
                failures.append({"代码": code, "接口": "stock_zh_a_hist", "错误": "; ".join(errors)})
                return pd.DataFrame(columns=[DATE_COLUMN] + list(PRICE_METRICS.values())), "失败"
            print(f"{code} 东财日行情失败，切换腾讯行情源。", flush=True)

    if args.price_source in ("auto", "tencent"):
        try:
            df = fetch_with_retry(lambda: fetch_price_tencent(code, args), f"{code} 腾讯日行情", args)
            pd.to_pickle(df, cache_path)
            sleep_after_request(args)
            return df, "请求-tencent"
        except Exception as exc:
            errors.append(f"tencent: {exc}")

    failures.append({"代码": code, "接口": "日行情", "错误": "; ".join(errors)})
    return pd.DataFrame(columns=[DATE_COLUMN] + list(PRICE_METRICS.values())), "失败"


def fetch_value_page(symbol, page_number, args):
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "TRADE_DATE",
        "sortTypes": "-1",
        "pageSize": "5000",
        "pageNumber": str(page_number),
        "reportName": "RPT_VALUEANALYSIS_DET",
        "columns": "ALL",
        "quoteColumns": "",
        "source": "WEB",
        "client": "WEB",
        "filter": f'(SECURITY_CODE="{symbol}")',
    }
    response = requests.get(url, params=params, timeout=args.timeout)
    response.raise_for_status()
    return response.json()


def normalize_value_json(rows):
    columns_map = {
        "TRADE_DATE": DATE_COLUMN,
        "NOTLIMITED_MARKETCAP_A": "流通市值",
        "PE_TTM": "PE(TTM)",
    }
    if not rows:
        return pd.DataFrame(columns=[DATE_COLUMN] + list(VALUE_METRICS.values()))

    work_df = pd.DataFrame(rows)
    missing_columns = [col for col in columns_map if col not in work_df.columns]
    if missing_columns:
        raise KeyError(f"估值数据缺少字段: {', '.join(missing_columns)}")

    work_df = work_df[list(columns_map)].rename(columns=columns_map)
    work_df[DATE_COLUMN] = pd.to_datetime(work_df[DATE_COLUMN], errors="coerce")
    work_df = work_df.dropna(subset=[DATE_COLUMN])
    for col in VALUE_METRICS.values():
        work_df[col] = pd.to_numeric(work_df[col], errors="coerce")
    work_df = work_df.sort_values(DATE_COLUMN).drop_duplicates(DATE_COLUMN).reset_index(drop=True)
    return work_df


def fetch_value_all_pages(code, args):
    symbol = code_to_symbol(code)
    all_rows = []
    page_number = 1
    total_pages = None

    while True:
        data_json = fetch_with_retry(
            lambda: fetch_value_page(symbol, page_number, args),
            f"{code} 估值第 {page_number} 页",
            args,
        )
        result = data_json.get("result") or {}
        rows = result.get("data") or []
        if not rows:
            break

        all_rows.extend(rows)
        total_pages = result.get("pages") or total_pages
        if total_pages and page_number >= int(total_pages):
            break

        page_number += 1
        sleep_after_request(args)

    return normalize_value_json(all_rows)


def load_or_fetch_value(code, args, failures):
    cache_path = value_cache_path(args.cache_dir, code)
    if cache_path.exists() and not args.force_refresh:
        return pd.read_pickle(cache_path), "缓存"

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df = fetch_value_all_pages(code, args)
        pd.to_pickle(df, cache_path)
        sleep_after_request(args)
        return df, "请求"
    except Exception as exc:
        failures.append({"代码": code, "接口": "stock_value_em", "错误": str(exc)})
        return pd.DataFrame(columns=[DATE_COLUMN] + list(VALUE_METRICS.values())), "失败"


def filter_date_range(df, start_date, end_date):
    if df.empty or DATE_COLUMN not in df.columns:
        return df
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    return df[(df[DATE_COLUMN] >= start) & (df[DATE_COLUMN] <= end)].copy()


def load_metric_series(code, metric, args, date_labels):
    source, source_col = METRIC_CONFIG[metric]
    if source == "price":
        cache_path = price_cache_path(args.cache_dir, code, args.start_date, args.end_date)
    else:
        cache_path = value_cache_path(args.cache_dir, code)

    if not cache_path.exists():
        return pd.Series(index=date_labels, dtype="float64")

    df = pd.read_pickle(cache_path)
    df = filter_date_range(df, args.start_date, args.end_date)
    if df.empty or source_col not in df.columns:
        return pd.Series(index=date_labels, dtype="float64")

    date_text = df[DATE_COLUMN].dt.strftime("%Y-%m-%d")
    series = pd.Series(df[source_col].values, index=date_text)
    series = series[~series.index.duplicated(keep="last")]
    return series.reindex(date_labels)


def write_metric_workbook(metric, codes, date_labels, name_map, args, output_dir):
    output_path = Path(output_dir) / f"{metric}.xlsx"
    wb = Workbook(write_only=True)
    ws = wb.create_sheet(title=metric[:31])
    ws.append([DATE_COLUMN] + codes)
    ws.append([NAME_COLUMN] + [name_map.get(code, "") for code in codes])

    code_series = [
        load_metric_series(code, metric, args, date_labels).tolist()
        for code in codes
    ]

    for row_idx, date_text in enumerate(date_labels):
        ws.append([date_text] + [series[row_idx] for series in code_series])

    wb.save(output_path)
    return output_path


def save_run_summary(output_dir, failures, code_summary):
    output_path = Path(output_dir) / "run_summary.xlsx"
    field_notes = pd.DataFrame(
        [
            {"输出文件": metric, "数据源": source, "源字段": source_col}
            for metric, (source, source_col) in METRIC_CONFIG.items()
        ]
    )
    with pd.ExcelWriter(output_path) as writer:
        field_notes.to_excel(writer, sheet_name="字段说明", index=False)
        pd.DataFrame(code_summary).to_excel(writer, sheet_name="个股获取概览", index=False)
        pd.DataFrame(failures).to_excel(writer, sheet_name="接口失败记录", index=False)
    return output_path


def main():
    configure_stdout()
    args = parse_args()
    codes = load_stock_codes(args.stock_list, args.limit)
    dates = get_trade_dates(args.start_date, args.end_date)
    date_labels = [date.strftime("%Y-%m-%d") for date in dates]

    output_dir = Path(args.output_dir) if args.output_dir else Path(
        datetime.now().strftime("stock_daily_metrics_report_%Y%m%d_%H%M%S")
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"清单代码数: {len(codes)}")
    print(f"交易日数量: {len(dates)}")
    print(f"日期范围: {date_labels[0]} 至 {date_labels[-1]}")
    print(f"缓存目录: {args.cache_dir}")
    print(f"输出目录: {output_dir}")
    print(f"行情源: {args.price_source}")
    print(
        f"股票级等待: {args.min_wait}-{args.max_wait} 秒 | "
        f"腾讯分块等待: {args.chunk_min_wait}-{args.chunk_max_wait} 秒"
    )

    failures = []
    name_map = load_or_fetch_name_map(codes, args, failures)
    code_summary = []
    run_start = time.monotonic()

    for idx, code in enumerate(codes, start=1):
        progress = idx / len(codes)
        elapsed = time.monotonic() - run_start
        eta = (elapsed / (idx - 1) * (len(codes) - idx + 1)) if idx > 1 else 0
        price_cached = price_cache_path(args.cache_dir, code, args.start_date, args.end_date).exists() and not args.force_refresh
        value_cached = value_cache_path(args.cache_dir, code).exists() and not args.force_refresh
        print(
            f"[{idx}/{len(codes)} {progress:.1%}] 开始 {code} | "
            f"elapsed {format_duration(elapsed)} | ETA {format_duration(eta)} | "
            f"行情 {'缓存' if price_cached else '请求'} | "
            f"估值 {'缓存' if value_cached else '请求'}",
            flush=True,
        )

        price_df, price_source = load_or_fetch_price(code, args, failures)
        value_df, value_source = load_or_fetch_value(code, args, failures)
        price_in_range = filter_date_range(price_df, args.start_date, args.end_date)
        value_in_range = filter_date_range(value_df, args.start_date, args.end_date)

        code_summary.append(
            {
                "代码": code,
                "名称": name_map.get(code, ""),
                "行情来源": price_source,
                "行情行数": len(price_in_range),
                "估值来源": value_source,
                "估值行数": len(value_in_range),
            }
        )

        elapsed_after = time.monotonic() - run_start
        eta_after = elapsed_after / idx * (len(codes) - idx) if idx < len(codes) else 0
        print(
            f"[{idx}/{len(codes)} {progress:.1%}] 完成 {code} | "
            f"行情 {len(price_in_range)} 行 | 估值 {len(value_in_range)} 行 | "
            f"elapsed {format_duration(elapsed_after)} | ETA {format_duration(eta_after)}",
            flush=True,
        )

    output_paths = []
    for metric in METRIC_CONFIG:
        print(f"正在生成 {metric}.xlsx", flush=True)
        output_paths.append(write_metric_workbook(metric, codes, date_labels, name_map, args, output_dir))

    summary_path = save_run_summary(output_dir, failures, code_summary)

    print("生成完成:")
    for path in output_paths:
        print(f"- {path}")
    print(f"- {summary_path}")
    print(
        "全量运行命令示例: "
        f"python -u get_stock_daily_metrics.py --start-date {FULL_START_DATE} "
        f"--end-date {FULL_END_DATE} --min-wait 0.2 --max-wait 0.6 "
        f"--chunk-min-wait 0.02 --chunk-max-wait 0.08 --price-source tencent"
    )


if __name__ == "__main__":
    main()
