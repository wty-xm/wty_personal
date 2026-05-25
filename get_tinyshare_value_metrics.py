import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl import Workbook

import tinyshare as ts


DEFAULT_START_DATE = "20050101"
DEFAULT_END_DATE = "20260521"
DEFAULT_TINYSHARE_TOKEN = "YKCAX57foT5NS8t2Qhh2gtf1ni3BH7v1F6u60f2dDMbhF6YbvnmBaiRIb2a427c8"

DATE_COLUMN = "日期"
NAME_COLUMN = "股票名称"

METRIC_COLUMNS = {
    "收盘价": "close",
    "换手率": "turnover_rate",
    "换手率-自由流通股": "turnover_rate_f",
    "量比": "volume_ratio",
    "市盈率": "pe",
    "市盈率-动态": "pe_ttm",
    "市净率": "pb",
    "市销率": "ps",
    "市销率-TTM": "ps_ttm",
    "股息率": "dv_ratio",
    "股息率-TTM": "dv_ttm",
    "总股本": "total_share",
    "流通股本": "float_share",
    "自由流通股本": "free_share",
    "总市值": "total_mv",
    "流动市值": "circ_mv",
}
DEFAULT_METRICS = ",".join(METRIC_COLUMNS)
METRIC_UNITS = {
    "换手率": "%",
    "换手率-自由流通股": "%",
    "股息率": "%",
    "股息率-TTM": "%",
    "总股本": "万股",
    "流通股本": "万股",
    "自由流通股本": "万股",
    "总市值": "万元",
    "流动市值": "万元",
}
OUTPUT_COLUMNS = [DATE_COLUMN] + list(METRIC_COLUMNS.values())


def configure_stdout():
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass


def parse_args():
    parser = argparse.ArgumentParser(
        description="使用 tinyshare daily_basic 下载每日指标，并生成横向矩阵 Excel。"
    )
    parser.add_argument("--stock-list", default="个股基本信息.xlsx", help="个股清单 Excel 路径")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="开始日期, 例如 20050101")
    parser.add_argument("--end-date", default=DEFAULT_END_DATE, help="结束日期, 例如 20260521")
    parser.add_argument("--cache-dir", default="tinyshare_value_metrics_cache", help="缓存目录")
    parser.add_argument(
        "--output-dir",
        default="",
        help="输出目录；为空时自动创建带时间戳的目录",
    )
    parser.add_argument(
        "--metrics",
        default=DEFAULT_METRICS,
        help=f"逗号分隔的输出指标；为空或默认表示全部。可选: {DEFAULT_METRICS}",
    )
    parser.add_argument("--limit", type=int, default=0, help="仅处理前 N 只股票；0 表示全部")
    parser.add_argument("--min-wait", type=float, default=0.2, help="每只股票请求后的最小等待秒数")
    parser.add_argument("--max-wait", type=float, default=0.6, help="每只股票请求后的最大等待秒数")
    parser.add_argument("--workers", type=int, default=1, help="并发请求线程数；建议先用 4，小心过高触发限流")
    parser.add_argument("--max-attempts", type=int, default=3, help="接口失败重试次数")
    parser.add_argument("--token", default="", help="tinyshare 授权码；为空时读取 TINYSHARE_TOKEN")
    parser.add_argument("--force-refresh", action="store_true", help="忽略已有缓存重新请求")
    return parser.parse_args()


def parse_selected_metrics(metrics_text):
    if not metrics_text:
        return list(METRIC_COLUMNS)

    metrics = [item.strip() for item in metrics_text.split(",") if item.strip()]
    unknown_metrics = [metric for metric in metrics if metric not in METRIC_COLUMNS]
    if unknown_metrics:
        raise ValueError(
            "未知指标: "
            f"{', '.join(unknown_metrics)}；可选指标: {', '.join(METRIC_COLUMNS)}"
        )
    if not metrics:
        raise ValueError("至少选择一个指标")
    return metrics


def normalize_stock_code(value):
    if pd.isna(value):
        return ""

    code = str(value).strip().upper()
    if not code or code in {"股票代码", "证券代码", "代码"}:
        return ""

    if code.endswith(".0") and code[:-2].isdigit():
        code = code[:-2]

    if "." in code:
        number, suffix = code.split(".", 1)
        suffix = suffix.upper()
        if number.isdigit() and suffix in {"SH", "SZ", "BJ"}:
            return f"{number.zfill(6)}.{suffix}"
        return ""

    if code[:2] in {"SH", "SZ", "BJ"} and code[2:].isdigit():
        return normalize_stock_code(f"{code[2:]}.{code[:2]}")

    if code.isdigit():
        number = code.zfill(6)
        if number.startswith(("60", "68", "51", "52", "56", "58")):
            return f"{number}.SH"
        if number.startswith(("00", "30", "15", "16", "18")):
            return f"{number}.SZ"
        if number.startswith(("8", "9", "4")):
            return f"{number}.BJ"

    return ""


def load_stock_codes_and_names(stock_list_path, limit):
    df = pd.read_excel(stock_list_path, header=None, nrows=2)
    codes = []
    name_map = {}
    for idx, value in enumerate(df.iloc[0].tolist()):
        code = normalize_stock_code(value)
        if not code:
            continue
        codes.append(code)
        if len(df) > 1:
            raw_name = df.iat[1, idx]
            if not pd.isna(raw_name):
                name_map[code] = str(raw_name).strip()

    if limit and limit > 0:
        codes = codes[:limit]
        name_map = {code: name_map.get(code, "") for code in codes}

    if not codes:
        raise ValueError(f"{stock_list_path} 第一行没有读取到有效证券代码")

    return codes, name_map


def sleep_random(min_value, max_value):
    min_wait = max(0.0, min_value)
    max_wait = max(min_wait, max_value)
    if max_wait > 0:
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
                sleep_random(args.min_wait, args.max_wait)
    raise RuntimeError(f"{label} 获取失败，最后错误: {last_error}")


def normalize_value_df(df):
    if df is None or df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    missing_columns = [col for col in ["trade_date", *METRIC_COLUMNS.values()] if col not in df.columns]
    if missing_columns:
        raise KeyError(f"daily_basic 缺少字段: {', '.join(missing_columns)}")

    work_df = df[["trade_date", *METRIC_COLUMNS.values()]].copy()
    work_df = work_df.rename(columns={"trade_date": DATE_COLUMN})
    work_df[DATE_COLUMN] = pd.to_datetime(work_df[DATE_COLUMN], format="%Y%m%d", errors="coerce")
    work_df = work_df.dropna(subset=[DATE_COLUMN])
    for col in METRIC_COLUMNS.values():
        work_df[col] = pd.to_numeric(work_df[col], errors="coerce")
    work_df = (
        work_df.sort_values(DATE_COLUMN)
        .drop_duplicates(DATE_COLUMN, keep="last")
        .reset_index(drop=True)
    )
    return work_df


def cache_path(cache_dir, code, start_date, end_date):
    return Path(cache_dir) / "daily_basic" / f"{code}_{start_date}_{end_date}.pkl"


def fetch_daily_basic(pro, code, args):
    raw_df = pro.daily_basic(
        ts_code=code,
        start_date=args.start_date,
        end_date=args.end_date,
        fields="ts_code,trade_date," + ",".join(METRIC_COLUMNS.values()),
    )
    return normalize_value_df(raw_df)


def load_or_fetch_daily_basic(pro, code, args, failures):
    path = cache_path(args.cache_dir, code, args.start_date, args.end_date)
    if path.exists() and not args.force_refresh:
        cached_df = pd.read_pickle(path)
        if all(column in cached_df.columns for column in OUTPUT_COLUMNS):
            return cached_df, "缓存"

    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df = fetch_with_retry(lambda: fetch_daily_basic(pro, code, args), f"{code} daily_basic", args)
        pd.to_pickle(df, path)
        sleep_random(args.min_wait, args.max_wait)
        return df, "请求"
    except Exception as exc:
        failures.append({"代码": code, "接口": "daily_basic", "错误": str(exc)})
        return pd.DataFrame(columns=OUTPUT_COLUMNS), "失败"


def load_or_fetch_daily_basic_task(code, args):
    failures = []
    pro = ts.pro_api()
    df, source = load_or_fetch_daily_basic(pro, code, args, failures)
    return code, df, source, failures


def build_code_summary_row(code, name_map, df, source):
    return {
        "代码": code,
        "名称": name_map.get(code, ""),
        "来源": source,
        "行数": len(df),
        "最早日期": df[DATE_COLUMN].min().strftime("%Y-%m-%d") if not df.empty else "",
        "最晚日期": df[DATE_COLUMN].max().strftime("%Y-%m-%d") if not df.empty else "",
    }


def collect_date_labels(codes, args):
    dates = set()
    for code in codes:
        path = cache_path(args.cache_dir, code, args.start_date, args.end_date)
        if not path.exists():
            continue
        df = pd.read_pickle(path)
        if DATE_COLUMN in df.columns and not df.empty:
            dates.update(df[DATE_COLUMN].dt.strftime("%Y-%m-%d"))
    return sorted(dates)


def load_metric_series(code, metric, args, date_labels):
    path = cache_path(args.cache_dir, code, args.start_date, args.end_date)
    if not path.exists():
        return pd.Series(index=date_labels, dtype="float64")

    df = pd.read_pickle(path)
    source_col = METRIC_COLUMNS[metric]
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


def save_run_summary(output_dir, failures, code_summary, selected_metrics, date_labels):
    output_path = Path(output_dir) / "run_summary.xlsx"
    field_notes = pd.DataFrame(
        [
            {
                "输出文件": metric,
                "接口": "tinyshare daily_basic",
                "源字段": METRIC_COLUMNS[metric],
                "单位": METRIC_UNITS.get(metric, ""),
            }
            for metric in selected_metrics
        ]
    )
    date_summary = pd.DataFrame(
        [
            {
                "输出交易日数量": len(date_labels),
                "最早日期": date_labels[0] if date_labels else "",
                "最晚日期": date_labels[-1] if date_labels else "",
            }
        ]
    )
    with pd.ExcelWriter(output_path) as writer:
        field_notes.to_excel(writer, sheet_name="字段说明", index=False)
        date_summary.to_excel(writer, sheet_name="日期概览", index=False)
        pd.DataFrame(code_summary).to_excel(writer, sheet_name="个股获取概览", index=False)
        pd.DataFrame(failures).to_excel(writer, sheet_name="接口失败记录", index=False)
    return output_path


def format_duration(seconds):
    seconds = int(max(0, seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def main():
    configure_stdout()
    args = parse_args()
    selected_metrics = parse_selected_metrics(args.metrics)
    token = args.token or os.environ.get("TINYSHARE_TOKEN", "") or DEFAULT_TINYSHARE_TOKEN
    if not token:
        raise ValueError("请通过 --token 或环境变量 TINYSHARE_TOKEN 提供 tinyshare 授权码")

    ts.set_token(token)
    pro = ts.pro_api()

    codes, name_map = load_stock_codes_and_names(args.stock_list, args.limit)
    output_dir = Path(args.output_dir) if args.output_dir else Path(
        datetime.now().strftime("tinyshare_value_metrics_%Y%m%d_%H%M%S")
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"清单代码数: {len(codes)}")
    print(f"日期范围: {args.start_date} 至 {args.end_date}")
    print(f"缓存目录: {args.cache_dir}")
    print(f"输出目录: {output_dir}")
    print(f"输出指标: {', '.join(selected_metrics)}")
    print(f"请求等待: {args.min_wait}-{args.max_wait} 秒")
    print(f"并发线程: {args.workers}")

    failures = []
    code_summary_by_code = {}
    run_start = time.monotonic()

    if args.workers <= 1:
        for idx, code in enumerate(codes, start=1):
            progress = idx / len(codes)
            elapsed = time.monotonic() - run_start
            eta = (elapsed / (idx - 1) * (len(codes) - idx + 1)) if idx > 1 else 0
            cached = cache_path(args.cache_dir, code, args.start_date, args.end_date).exists() and not args.force_refresh
            print(
                f"[{idx}/{len(codes)} {progress:.1%}] 开始 {code} | "
                f"elapsed {format_duration(elapsed)} | ETA {format_duration(eta)} | "
                f"{'缓存' if cached else '请求'}",
                flush=True,
            )

            df, source = load_or_fetch_daily_basic(pro, code, args, failures)
            code_summary_by_code[code] = build_code_summary_row(code, name_map, df, source)

            elapsed_after = time.monotonic() - run_start
            eta_after = elapsed_after / idx * (len(codes) - idx) if idx < len(codes) else 0
            print(
                f"[{idx}/{len(codes)} {progress:.1%}] 完成 {code} | "
                f"{len(df)} 行 | elapsed {format_duration(elapsed_after)} | "
                f"ETA {format_duration(eta_after)}",
                flush=True,
            )
    else:
        workers = max(1, args.workers)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_code = {
                executor.submit(load_or_fetch_daily_basic_task, code, args): code
                for code in codes
            }
            for idx, future in enumerate(as_completed(future_to_code), start=1):
                code = future_to_code[future]
                progress = idx / len(codes)
                try:
                    _, df, source, task_failures = future.result()
                except Exception as exc:
                    df = pd.DataFrame(columns=OUTPUT_COLUMNS)
                    source = "失败"
                    task_failures = [{"代码": code, "接口": "daily_basic", "错误": str(exc)}]
                failures.extend(task_failures)
                code_summary_by_code[code] = build_code_summary_row(code, name_map, df, source)

                elapsed = time.monotonic() - run_start
                eta = elapsed / idx * (len(codes) - idx) if idx < len(codes) else 0
                print(
                    f"[{idx}/{len(codes)} {progress:.1%}] 完成 {code} | "
                    f"{source} | {len(df)} 行 | elapsed {format_duration(elapsed)} | "
                    f"ETA {format_duration(eta)}",
                    flush=True,
                )

    code_summary = [code_summary_by_code[code] for code in codes if code in code_summary_by_code]

    date_labels = collect_date_labels(codes, args)
    if not date_labels:
        raise RuntimeError("没有获取到任何日期数据，无法生成矩阵文件")

    print(f"输出交易日数量: {len(date_labels)} | {date_labels[0]} 至 {date_labels[-1]}")

    output_paths = []
    for metric in selected_metrics:
        print(f"正在生成 {metric}.xlsx", flush=True)
        output_paths.append(write_metric_workbook(metric, codes, date_labels, name_map, args, output_dir))

    summary_path = save_run_summary(output_dir, failures, code_summary, selected_metrics, date_labels)

    print("生成完成:")
    for path in output_paths:
        print(f"- {path}")
    print(f"- {summary_path}")


if __name__ == "__main__":
    main()
