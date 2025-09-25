# -*- coding: utf-8 -*-
# @Author: TY (Your Investment Advisor)
# @Date: 2025-09-25
# @Version: 1.0
# @Description: Fetch comprehensive real-time, company, and historical datasets for a Hong Kong stock
#               using multiple AkShare interfaces, and persist the outputs to structured reports that
#               include the stock code and name in their filenames.

import os
import re
import warnings
from datetime import datetime, timedelta

import akshare as ak
import pandas as pd

warnings.filterwarnings("ignore")

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

DATE_COLUMNS_PRIORITY = [
    '日期时间', 'date', '日期', '时间', 'datetime', '报告期', '公告日期', '公告时间',
    '统计截止日', '截止日期', '发放日', '除净日', '最新公告日期'
]

REPORT_ROOT = "hk_stock_reports"


# --- Configuration (edit HK_STOCK_CODE to fetch another stock) ---
HK_STOCK_CODE = '00700'
HISTORY_START_DATE = None  # e.g. '2024-01-01'
HISTORY_END_DATE = None    # e.g. '2024-12-31'
MINUTE_PERIOD = '1'
MINUTE_ADJUST = 'qfq'
MINUTE_START = None        # e.g. '2025-09-25 09:30:00'
MINUTE_END = None          # e.g. '2025-09-25 16:00:00'
SKIP_REALTIME = False
SKIP_COMPANY = False
SKIP_HISTORY = False


def ensure_symbol_format(symbol: str) -> str:
    symbol = str(symbol).strip()
    if symbol.endswith('.HK'):
        symbol = symbol[:-3]
    return symbol.zfill(5)


def sanitize_filename_component(value: str) -> str:
    if not value:
        return ''
    value = str(value).strip()
    value = re.sub(r"[\\/:*?\"<>|]", "_", value)
    value = re.sub(r"\s+", "_", value)
    return value.strip('_')


def build_report_filename(prefix: str, symbol: str, stock_name: str, extension: str) -> str:
    today_str = datetime.now().strftime('%Y-%m-%d')
    code_part = sanitize_filename_component(symbol)
    name_part = sanitize_filename_component(stock_name) or code_part
    return f"{prefix}_{code_part}_{name_part}_{today_str}.{extension}"


def sort_dataframe_by_date(df: pd.DataFrame) -> pd.DataFrame:
    for column in DATE_COLUMNS_PRIORITY:
        if column in df.columns:
            df_sorted = df.copy()
            df_sorted[column] = pd.to_datetime(df_sorted[column], errors='coerce')
            if df_sorted[column].notna().any():
                return df_sorted.sort_values(by=column, ascending=False)
    return df


def fetch_stock_name(symbol: str) -> str:
    symbol = ensure_symbol_format(symbol)
    try:
        spot_df = ak.stock_hk_spot_em()
        if spot_df is not None and not spot_df.empty:
            spot_df['代码'] = spot_df['代码'].astype(str).str.zfill(5)
            match = spot_df[spot_df['代码'] == symbol]
            if not match.empty:
                return str(match['名称'].iloc[0]).strip()
    except Exception:
        pass

    try:
        profile_df = ak.stock_hk_security_profile_em(symbol=symbol)
        if profile_df is not None and not profile_df.empty and '证券简称' in profile_df.columns:
            return str(profile_df['证券简称'].iloc[0]).strip()
    except Exception:
        pass

    return symbol


# -------------------------- Real-time datasets --------------------------

def filter_by_symbol(df: pd.DataFrame, symbol: str, code_col: str = '代码') -> pd.DataFrame:
    if df is None or df.empty or code_col not in df.columns:
        return pd.DataFrame()
    copy_df = df.copy()
    copy_df[code_col] = copy_df[code_col].astype(str).str.strip()
    copy_df[code_col] = copy_df[code_col].str.replace('.HK', '', regex=False)
    copy_df[code_col] = copy_df[code_col].str.zfill(5)
    return copy_df[copy_df[code_col] == symbol]


def get_realtime_data(symbol: str) -> dict:
    symbol = ensure_symbol_format(symbol)
    realtime_data = {}

    try:
        df = ak.stock_hk_spot_em()
        realtime_data['实时行情-东财全市场'] = filter_by_symbol(df, symbol)
    except Exception as exc:
        print(f"  - [警告] 获取 [实时行情-东财全市场] 失败: {exc}")

    try:
        df = ak.stock_hk_main_board_spot_em()
        realtime_data['实时行情-东财主板'] = filter_by_symbol(df, symbol)
    except Exception as exc:
        print(f"  - [警告] 获取 [实时行情-东财主板] 失败: {exc}")

    try:
        df = ak.stock_hk_famous_spot_em()
        realtime_data['实时行情-东财知名港股'] = filter_by_symbol(df, symbol)
    except Exception as exc:
        print(f"  - [警告] 获取 [实时行情-东财知名港股] 失败: {exc}")

    try:
        df = ak.stock_hk_spot()
        realtime_data['实时行情-新浪'] = filter_by_symbol(df, symbol, code_col='代码')
    except Exception as exc:
        print(f"  - [警告] 获取 [实时行情-新浪] 失败: {exc}")

    return realtime_data


def save_realtime_outputs(symbol: str, stock_name: str, data_dict: dict) -> None:
    os.makedirs(REPORT_ROOT, exist_ok=True)
    excel_path = os.path.join(
        REPORT_ROOT,
        build_report_filename("hk_realtime_report", symbol, stock_name, "xlsx"),
    )
    summary_path = os.path.join(
        REPORT_ROOT,
        build_report_filename("hk_realtime_summary", symbol, stock_name, "txt"),
    )

    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            for sheet_name, df in data_dict.items():
                if df is not None and not df.empty:
                    safe_sheet = ''.join(c for c in sheet_name if c.isalnum() or c in (' ', '_'))[:31]
                    df.to_excel(writer, sheet_name=safe_sheet, index=False)
        print(f"--- 实时行情数据已保存至: {excel_path}")
    except Exception as exc:
        print(f"--- [错误] 实时行情 Excel 保存失败: {exc}")

    try:
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(f"港股代码: {symbol}\n")
            f.write(f"港股名称: {stock_name}\n")
            f.write(f"报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("==================================================\n\n")

            for name, df in data_dict.items():
                if df is not None and not df.empty:
                    f.write(f"--------- {name} ---------\n")
                    f.write(df.to_string(index=False))
                    f.write("\n\n")
        print(f"--- 实时行情摘要已保存至: {summary_path}")
    except Exception as exc:
        print(f"--- [错误] 实时行情摘要保存失败: {exc}")


# -------------------------- Company & fundamentals --------------------------

def get_company_data(symbol: str) -> dict:
    symbol = ensure_symbol_format(symbol)
    company_data = {}

    try:
        df = ak.stock_individual_basic_info_hk_xq(symbol=symbol)
        company_data['个股信息-雪球'] = df
    except Exception as exc:
        print(f"  - [警告] 获取 [个股信息-雪球] 失败: {exc}")

    try:
        df = ak.stock_hk_security_profile_em(symbol=symbol)
        company_data['证券资料-东财'] = df
    except Exception as exc:
        print(f"  - [警告] 获取 [证券资料-东财] 失败: {exc}")

    try:
        df = ak.stock_hk_company_profile_em(symbol=symbol)
        company_data['公司资料-东财'] = df
    except Exception as exc:
        print(f"  - [警告] 获取 [公司资料-东财] 失败: {exc}")

    try:
        df = ak.stock_hk_financial_indicator_em(symbol=symbol)
        if df is not None and not df.empty:
            df['港股代码'] = symbol
        company_data['财务指标-东财'] = df
    except Exception as exc:
        print(f"  - [警告] 获取 [财务指标-东财] 失败: {exc}")

    try:
        df = ak.stock_hk_dividend_payout_em(symbol=symbol)
        company_data['分红派息-东财'] = df
    except Exception as exc:
        print(f"  - [警告] 获取 [分红派息-东财] 失败: {exc}")

    return company_data


def save_company_outputs(symbol: str, stock_name: str, data_dict: dict) -> None:
    os.makedirs(REPORT_ROOT, exist_ok=True)
    excel_path = os.path.join(
        REPORT_ROOT,
        build_report_filename("hk_company_report", symbol, stock_name, "xlsx"),
    )
    summary_path = os.path.join(
        REPORT_ROOT,
        build_report_filename("hk_company_summary", symbol, stock_name, "txt"),
    )

    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            for sheet_name, df in data_dict.items():
                if df is not None and not df.empty:
                    safe_sheet = ''.join(c for c in sheet_name if c.isalnum() or c in (' ', '_'))[:31]
                    df.to_excel(writer, sheet_name=safe_sheet, index=False)
        print(f"--- 公司资料数据已保存至: {excel_path}")
    except Exception as exc:
        print(f"--- [错误] 公司资料 Excel 保存失败: {exc}")

    summary_priority = ['证券资料-东财', '公司资料-东财', '财务指标-东财', '分红派息-东财', '个股信息-雪球']

    try:
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(f"港股代码: {symbol}\n")
            f.write(f"港股名称: {stock_name}\n")
            f.write(f"报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("==================================================\n\n")

            for name in summary_priority:
                if name in data_dict and data_dict[name] is not None and not data_dict[name].empty:
                    df = data_dict[name].copy()
                    f.write(f"--------- {name} ---------\n")
                    df_sorted = sort_dataframe_by_date(df)
                    if name == '分红派息-东财':
                        f.write(df_sorted.head(5).to_string(index=False))
                    else:
                        f.write(df_sorted.head(1).to_string(index=False))
                    f.write("\n\n")
        print(f"--- 公司资料摘要已保存至: {summary_path}")
    except Exception as exc:
        print(f"--- [错误] 公司资料摘要保存失败: {exc}")


# -------------------------- Historical datasets --------------------------

def get_history_data(symbol: str, start_date: str, end_date: str, minute_period: str, minute_adjust: str,
                     minute_start: str, minute_end: str) -> dict:
    symbol = ensure_symbol_format(symbol)
    history_data = {}

    now_dt = datetime.now()
    start_date_em = start_date.replace('-', '') if start_date else (now_dt - timedelta(days=365)).strftime('%Y%m%d')
    end_date_em = end_date.replace('-', '') if end_date else now_dt.strftime('%Y%m%d')

    try:
        df = ak.stock_hk_hist(symbol=symbol, period='daily', start_date=start_date_em, end_date=end_date_em, adjust='')
        history_data['历史行情-东财-未复权'] = df
    except Exception as exc:
        print(f"  - [警告] 获取 [历史行情-东财-未复权] 失败: {exc}")

    try:
        df = ak.stock_hk_hist(symbol=symbol, period='daily', start_date=start_date_em, end_date=end_date_em, adjust='qfq')
        history_data['历史行情-东财-前复权'] = df
    except Exception as exc:
        print(f"  - [警告] 获取 [历史行情-东财-前复权] 失败: {exc}")

    try:
        df = ak.stock_hk_hist(symbol=symbol, period='daily', start_date=start_date_em, end_date=end_date_em, adjust='hfq')
        history_data['历史行情-东财-后复权'] = df
    except Exception as exc:
        print(f"  - [警告] 获取 [历史行情-东财-后复权] 失败: {exc}")

    try:
        df = ak.stock_hk_daily(symbol=symbol, adjust='')
        history_data['历史行情-新浪-未复权'] = df
    except Exception as exc:
        print(f"  - [警告] 获取 [历史行情-新浪-未复权] 失败: {exc}")

    try:
        df = ak.stock_hk_daily(symbol=symbol, adjust='hfq')
        history_data['历史行情-新浪-后复权'] = df
    except Exception as exc:
        print(f"  - [警告] 获取 [历史行情-新浪-后复权] 失败: {exc}")

    minute_period = minute_period or '1'
    minute_adjust = minute_adjust or 'qfq'

    minute_start_param = minute_start
    minute_end_param = minute_end
    market_open = now_dt.replace(hour=9, minute=30, second=0, microsecond=0)

    skip_minute = False
    if not minute_start_param:
        minute_start_param = market_open.strftime('%Y-%m-%d %H:%M:%S')
        if now_dt < market_open:
            print("  - [信息] 当前时间尚未到开盘时间，默认分钟行情跳过。若需获取历史分钟数据请指定 --minute-start。")
            skip_minute = True
    if not minute_end_param:
        minute_end_param = now_dt.strftime('%Y-%m-%d %H:%M:%S')

    if not skip_minute:
        minute_kwargs = {
            'symbol': symbol,
            'period': minute_period,
            'adjust': minute_adjust,
        }
        if minute_start_param:
            minute_kwargs['start_date'] = minute_start_param
        if minute_end_param:
            minute_kwargs['end_date'] = minute_end_param

        try:
            minute_df = ak.stock_hk_hist_min_em(**minute_kwargs)
            history_data['分钟行情-东财'] = minute_df
        except Exception as exc:
            print(f"  - [警告] 获取 [分钟行情-东财] 失败: {exc}")

    return history_data


def save_history_outputs(symbol: str, stock_name: str, data_dict: dict) -> None:
    os.makedirs(REPORT_ROOT, exist_ok=True)
    excel_path = os.path.join(
        REPORT_ROOT,
        build_report_filename("hk_history_report", symbol, stock_name, "xlsx"),
    )
    summary_path = os.path.join(
        REPORT_ROOT,
        build_report_filename("hk_history_summary", symbol, stock_name, "txt"),
    )

    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            for sheet_name, df in data_dict.items():
                if df is not None and not df.empty:
                    safe_sheet = ''.join(c for c in sheet_name if c.isalnum() or c in (' ', '_'))[:31]
                    df.to_excel(writer, sheet_name=safe_sheet, index=False)
        print(f"--- 历史行情数据已保存至: {excel_path}")
    except Exception as exc:
        print(f"--- [错误] 历史行情 Excel 保存失败: {exc}")

    try:
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(f"港股代码: {symbol}\n")
            f.write(f"港股名称: {stock_name}\n")
            f.write(f"报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("==================================================\n\n")

            for name, df in data_dict.items():
                if df is not None and not df.empty:
                    df_sorted = sort_dataframe_by_date(df)
                    f.write(f"--------- {name} ---------\n")
                    if name.startswith('分钟行情'):
                        f.write(df_sorted.head(20).to_string(index=False))
                    else:
                        f.write(df_sorted.head(5).to_string(index=False))
                    f.write("\n\n")
        print(f"--- 历史行情摘要已保存至: {summary_path}")
    except Exception as exc:
        print(f"--- [错误] 历史行情摘要保存失败: {exc}")


# -------------------------- Orchestration --------------------------

def preview_data(title: str, data_dict: dict) -> None:
    print("\n========================= 数据预览 (仅显示头部) =========================")
    print(title)
    for name, df in data_dict.items():
        print(f"\n--------- {name} ---------")
        if df is not None and not df.empty:
            print(df.head())
        else:
            print("未能获取到数据或数据为空。")
    print("====================================================================")


def main(symbol: str, start_date: str, end_date: str, minute_period: str, minute_adjust: str,
         minute_start: str, minute_end: str, skip_realtime: bool, skip_company: bool, skip_history: bool) -> None:
    symbol_formatted = ensure_symbol_format(symbol)
    stock_name = fetch_stock_name(symbol_formatted)
    print(f"目标港股: {symbol_formatted} ({stock_name})")

    if not skip_realtime:
        realtime_data = get_realtime_data(symbol_formatted)
        save_realtime_outputs(symbol_formatted, stock_name, realtime_data)
        preview_data("实时行情数据", realtime_data)
    else:
        realtime_data = {}

    if not skip_company:
        company_data = get_company_data(symbol_formatted)
        save_company_outputs(symbol_formatted, stock_name, company_data)
        preview_data("公司资料数据", company_data)
    else:
        company_data = {}

    if not skip_history:
        history_data = get_history_data(symbol_formatted, start_date, end_date, minute_period, minute_adjust,
                                        minute_start, minute_end)
        save_history_outputs(symbol_formatted, stock_name, history_data)
        preview_data("历史行情数据", history_data)
    else:
        history_data = {}


if __name__ == '__main__':
    main(
        symbol=HK_STOCK_CODE,
        start_date=HISTORY_START_DATE,
        end_date=HISTORY_END_DATE,
        minute_period=MINUTE_PERIOD,
        minute_adjust=MINUTE_ADJUST,
        minute_start=MINUTE_START,
        minute_end=MINUTE_END,
        skip_realtime=SKIP_REALTIME,
        skip_company=SKIP_COMPANY,
        skip_history=SKIP_HISTORY,
    )
