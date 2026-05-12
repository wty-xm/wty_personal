import argparse
import random
import sys
import time
from datetime import datetime
from pathlib import Path

import akshare as ak
import pandas as pd
from openpyxl import Workbook


DEFAULT_START_DATE = "20260511"
DEFAULT_END_DATE = "20260511"
FULL_START_DATE = "20130101"
FULL_END_DATE = "20260511"

DATE_COLUMN = "日期"
CODE_COLUMN = "代码"
NAME_COLUMN = "证券简称"
METRICS = ["融资买入额", "融资余额"]


def configure_stdout():
    """尽量让长任务日志实时显示。"""
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
        description="按个股清单下载沪深融资融券明细，并生成横向矩阵 Excel。"
    )
    parser.add_argument("--stock-list", default="个股清单.xlsx", help="个股清单 Excel 路径")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="开始日期, 例如 20130101")
    parser.add_argument("--end-date", default=DEFAULT_END_DATE, help="结束日期, 例如 20260511")
    parser.add_argument(
        "--cache-dir",
        default="stock_margin_detail_cache",
        help="每日明细缓存目录",
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
    parser.add_argument("--max-attempts", type=int, default=3, help="接口失败重试次数")
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="忽略已有缓存，重新请求接口",
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


def load_stock_codes(stock_list_path):
    row = pd.read_excel(stock_list_path, header=None, nrows=1).iloc[0]
    codes = [normalize_stock_code(value) for value in row.tolist()]
    codes = [code for code in codes if code]
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
        # 单日验证时允许用户指定一个交易日历没有覆盖到的日期，由接口结果决定是否有数据。
        dates = [start]

    if not dates:
        raise ValueError(f"{start_date}-{end_date} 内没有交易日")

    return dates


def sleep_after_request(args):
    min_wait = max(0.0, args.min_wait)
    max_wait = max(min_wait, args.max_wait)
    if max_wait <= 0:
        return
    delay = random.uniform(min_wait, max_wait)
    time.sleep(delay)


def cache_path_for(cache_dir, market, date_text):
    return Path(cache_dir) / market / f"{date_text}.pkl"


def normalize_market_df(df, market):
    if df is None or df.empty:
        return pd.DataFrame(columns=[CODE_COLUMN, NAME_COLUMN] + METRICS)

    if market == "sh":
        code_col = "标的证券代码"
        name_col = "标的证券简称"
        suffix = ".SH"
    elif market == "sz":
        code_col = "证券代码"
        name_col = "证券简称"
        suffix = ".SZ"
    else:
        raise ValueError(f"未知市场: {market}")

    required_columns = [code_col, name_col] + METRICS
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise KeyError(f"{market} 数据缺少字段: {', '.join(missing_columns)}")

    work_df = df[required_columns].copy()
    work_df.rename(columns={code_col: CODE_COLUMN, name_col: NAME_COLUMN}, inplace=True)
    work_df[CODE_COLUMN] = (
        work_df[CODE_COLUMN]
        .astype(str)
        .str.extract(r"(\d+)", expand=False)
        .str.zfill(6)
        + suffix
    )
    work_df[NAME_COLUMN] = (
        work_df[NAME_COLUMN].astype(str).str.replace("&nbsp;", "", regex=False).str.strip()
    )

    for metric in METRICS:
        work_df[metric] = pd.to_numeric(work_df[metric], errors="coerce")

    return work_df[[CODE_COLUMN, NAME_COLUMN] + METRICS]


def fetch_market_detail(date_text, market):
    if market == "sh":
        return ak.stock_margin_detail_sse(date=date_text)
    if market == "sz":
        return ak.stock_margin_detail_szse(date=date_text)
    raise ValueError(f"未知市场: {market}")


def load_or_fetch_market(date_text, market, args, failures):
    cache_path = cache_path_for(args.cache_dir, market, date_text)
    if cache_path.exists() and not args.force_refresh:
        return pd.read_pickle(cache_path)

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    last_error = None

    for attempt in range(1, args.max_attempts + 1):
        try:
            raw_df = fetch_market_detail(date_text, market)
            df = normalize_market_df(raw_df, market)
            df.to_pickle(cache_path)
            sleep_after_request(args)
            return df
        except Exception as exc:
            last_error = exc
            print(f"{date_text} {market.upper()} 第 {attempt} 次获取失败: {exc}")
            if attempt < args.max_attempts:
                sleep_after_request(args)

    failures.append(
        {
            "日期": date_text,
            "市场": market.upper(),
            "错误": str(last_error),
        }
    )
    return pd.DataFrame(columns=[CODE_COLUMN, NAME_COLUMN] + METRICS)


def load_cached_market(date_text, market, cache_dir):
    cache_path = cache_path_for(cache_dir, market, date_text)
    if not cache_path.exists():
        return pd.DataFrame(columns=[CODE_COLUMN, NAME_COLUMN] + METRICS)
    return pd.read_pickle(cache_path)


def update_name_map(name_map, df):
    if df.empty:
        return
    for code, name in zip(df[CODE_COLUMN], df[NAME_COLUMN]):
        if code not in name_map and isinstance(name, str) and name:
            name_map[code] = name


def build_daily_metric_row(date_text, codes, metric, cache_dir):
    sh_df = load_cached_market(date_text, "sh", cache_dir)
    sz_df = load_cached_market(date_text, "sz", cache_dir)
    day_df = pd.concat([sh_df, sz_df], ignore_index=True)
    if day_df.empty or metric not in day_df.columns:
        return [date_text] + [None for _ in codes]

    metric_map = day_df.drop_duplicates(CODE_COLUMN).set_index(CODE_COLUMN)[metric]
    return [date_text] + [metric_map.get(code, None) for code in codes]


def write_metric_workbook(metric, codes, dates, name_map, cache_dir, output_dir):
    output_path = Path(output_dir) / f"{metric}.xlsx"
    wb = Workbook(write_only=True)
    ws = wb.create_sheet(title=metric)

    ws.append([DATE_COLUMN] + codes)
    ws.append([NAME_COLUMN] + [name_map.get(code, "") for code in codes])

    for date in dates:
        date_text = date.strftime("%Y%m%d")
        row_date = date.strftime("%Y-%m-%d")
        row = build_daily_metric_row(date_text, codes, metric, cache_dir)
        row[0] = row_date
        ws.append(row)

    wb.save(output_path)
    return output_path


def save_run_summary(output_dir, unsupported_codes, failures, daily_summary):
    output_path = Path(output_dir) / "run_summary.xlsx"
    with pd.ExcelWriter(output_path) as writer:
        pd.DataFrame({"代码": unsupported_codes}).to_excel(
            writer,
            sheet_name="暂不支持_BJ",
            index=False,
        )
        pd.DataFrame(failures).to_excel(
            writer,
            sheet_name="接口失败日期",
            index=False,
        )
        pd.DataFrame(daily_summary).to_excel(
            writer,
            sheet_name="每日匹配概览",
            index=False,
        )
    return output_path


def main():
    configure_stdout()
    args = parse_args()
    codes = load_stock_codes(args.stock_list)
    dates = get_trade_dates(args.start_date, args.end_date)

    output_dir = Path(args.output_dir) if args.output_dir else Path(
        datetime.now().strftime("stock_margin_detail_report_%Y%m%d_%H%M%S")
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    unsupported_codes = [code for code in codes if code.endswith(".BJ")]
    supported_codes = [code for code in codes if code.endswith((".SH", ".SZ"))]
    supported_code_set = set(supported_codes)

    print(f"清单代码数: {len(codes)}")
    print(f"沪深代码数: {len(supported_codes)}")
    print(f"暂不支持 BJ 代码数: {len(unsupported_codes)}")
    print(f"交易日数量: {len(dates)}")
    print(f"缓存目录: {args.cache_dir}")
    print(f"输出目录: {output_dir}")

    name_map = {}
    failures = []
    daily_summary = []
    run_start = time.monotonic()

    for idx, date in enumerate(dates, start=1):
        date_text = date.strftime("%Y%m%d")
        progress = idx / len(dates)
        elapsed = time.monotonic() - run_start
        eta = (elapsed / (idx - 1) * (len(dates) - idx + 1)) if idx > 1 else 0
        sh_cached = cache_path_for(args.cache_dir, "sh", date_text).exists() and not args.force_refresh
        sz_cached = cache_path_for(args.cache_dir, "sz", date_text).exists() and not args.force_refresh

        print(
            f"[{idx}/{len(dates)} {progress:.1%}] 开始 {date_text} | "
            f"elapsed {format_duration(elapsed)} | "
            f"ETA {format_duration(eta)} | "
            f"SH {'缓存' if sh_cached else '请求'} | "
            f"SZ {'缓存' if sz_cached else '请求'}",
            flush=True,
        )

        sh_df = load_or_fetch_market(date_text, "sh", args, failures)
        sz_df = load_or_fetch_market(date_text, "sz", args, failures)
        update_name_map(name_map, sh_df)
        update_name_map(name_map, sz_df)

        matched_codes = set(pd.concat([sh_df, sz_df], ignore_index=True)[CODE_COLUMN])
        matched_supported_count = len(supported_code_set & matched_codes)
        daily_summary.append(
            {
                "日期": date.strftime("%Y-%m-%d"),
                "上海行数": len(sh_df),
                "深圳行数": len(sz_df),
                "沪深清单匹配数": matched_supported_count,
                "沪深清单未匹配数": len(supported_codes) - matched_supported_count,
            }
        )

        elapsed_after = time.monotonic() - run_start
        eta_after = (
            elapsed_after / idx * (len(dates) - idx)
            if idx < len(dates)
            else 0
        )
        print(
            f"[{idx}/{len(dates)} {progress:.1%}] 完成 {date_text} | "
            f"SH {len(sh_df)} 行 | SZ {len(sz_df)} 行 | "
            f"匹配 {matched_supported_count}/{len(supported_codes)} | "
            f"elapsed {format_duration(elapsed_after)} | "
            f"ETA {format_duration(eta_after)}",
            flush=True,
        )

    output_paths = []
    for metric in METRICS:
        print(f"正在生成 {metric}.xlsx")
        output_paths.append(
            write_metric_workbook(metric, codes, dates, name_map, args.cache_dir, output_dir)
        )

    summary_path = save_run_summary(output_dir, unsupported_codes, failures, daily_summary)

    print("生成完成:")
    for path in output_paths:
        print(f"- {path}")
    print(f"- {summary_path}")
    print(
        "全量运行命令示例: "
        f"conda run --no-capture-output -n akshare-env python -u get_stock_margin_detail.py "
        f"--start-date {FULL_START_DATE} --end-date {FULL_END_DATE}"
    )


if __name__ == "__main__":
    main()
