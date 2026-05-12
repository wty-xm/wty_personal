import time
from datetime import datetime
from pathlib import Path

import akshare as ak
import pandas as pd


# --- 1. 配置区 ---
START_DATE = "20130101"
END_DATE = "20260511"

DATE_COLUMN = "日期"
OUTPUT_COLUMNS = [
    "日期",
    "融资余额",
    "融券余额",
    "融资买入额",
    "融券卖出额",
    "证券公司数量",
    "营业部数量",
    "个人投资者数量",
    "机构投资者数量",
    "参与交易的投资者数量",
    "有融资融券负债的投资者数量",
    "担保物总价值",
    "平均维持担保比例",
]

MAX_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 3


def fetch_with_retry():
    """拉取东方财富两融账户信息，失败时重试。"""
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            df = ak.stock_margin_account_info()
            if df is not None and not df.empty:
                if attempt > 1:
                    print(f"第 {attempt} 次尝试成功。")
                return df

            last_error = ValueError("接口返回空数据")
            print(f"第 {attempt} 次返回空数据。")
        except Exception as e:
            last_error = e
            print(f"第 {attempt} 次尝试失败: {e}")

        if attempt < MAX_ATTEMPTS:
            time.sleep(RETRY_DELAY_SECONDS)

    raise RuntimeError(f"东方财富两融账户信息获取失败，最后错误: {last_error}")


def normalize_df(df):
    missing_columns = [col for col in OUTPUT_COLUMNS if col not in df.columns]
    if missing_columns:
        raise KeyError(f"接口返回数据缺少字段: {', '.join(missing_columns)}")

    work_df = df[OUTPUT_COLUMNS].copy()
    work_df[DATE_COLUMN] = pd.to_datetime(work_df[DATE_COLUMN], errors="coerce")
    work_df = work_df.dropna(subset=[DATE_COLUMN])

    for col in OUTPUT_COLUMNS:
        if col != DATE_COLUMN:
            work_df[col] = pd.to_numeric(work_df[col], errors="coerce")

    start_date = pd.to_datetime(START_DATE)
    end_date = pd.to_datetime(END_DATE)
    work_df = work_df[
        (work_df[DATE_COLUMN] >= start_date) & (work_df[DATE_COLUMN] <= end_date)
    ].copy()

    work_df = work_df.sort_values(DATE_COLUMN).reset_index(drop=True)
    if work_df.empty:
        raise ValueError(f"{START_DATE}-{END_DATE} 区间内没有数据")

    return work_df


def write_excel(df, output_path):
    output_df = df.copy()
    output_df[DATE_COLUMN] = output_df[DATE_COLUMN].dt.strftime("%Y-%m-%d")

    with pd.ExcelWriter(output_path) as writer:
        output_df.to_excel(writer, sheet_name="两融账户信息", index=False)

        try:
            worksheet = writer.sheets["两融账户信息"]
            for idx, col_name in enumerate(output_df.columns, start=1):
                width = max(14, min(28, len(str(col_name)) + 4))
                worksheet.column_dimensions[
                    worksheet.cell(row=1, column=idx).column_letter
                ].width = width
        except Exception:
            pass


def get_and_save_margin_account_info():
    print("正在获取东方财富两融账户信息...")
    raw_df = fetch_with_retry()
    df = normalize_df(raw_df)

    timestamp_folder = datetime.now().strftime("margin_account_report_%Y%m%d_%H%M%S")
    output_dir = Path(timestamp_folder)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"margin_account_info_{START_DATE}_{END_DATE}.xlsx"

    write_excel(df, output_path)

    print(f"数据行数: {len(df)} 行")
    print(
        "日期范围: "
        f"{df[DATE_COLUMN].min().strftime('%Y-%m-%d')} 至 "
        f"{df[DATE_COLUMN].max().strftime('%Y-%m-%d')}"
    )
    print(f"Excel 已保存为: {output_path}")


if __name__ == "__main__":
    get_and_save_margin_account_info()
