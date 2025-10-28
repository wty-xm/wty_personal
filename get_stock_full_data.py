# -*- coding: utf-8 -*-
# @Author: TY (Your Investment Advisor)
# @Date: 2025-09-24
# @Version: 1.0
# @Description: Unified script that consolidates fundamental, technical, and risk data
#               collection for a given A-share stock code while producing report files
#               whose names contain both the stock code and the stock name.

import argparse
import os
import re
import warnings
from datetime import datetime

import akshare as ak
import pandas as pd

warnings.filterwarnings("ignore")

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

DATE_COLUMNS_PRIORITY = [
    '报告期', '报告日', '报告日期', '公告日期', '公告时间', '股东户数公告日期',
    '股东户数统计截止日', '统计截止日', '日期', '交易日期', '变动日期', '截止日期',
    '发布时间', '数据日期'
]

REPORT_ROOT = "stock_reports"


def get_stock_code_prefix(stock_code: str) -> str:
    """Detect the exchange prefix for a mainland stock code."""
    if stock_code.startswith('6'):
        return 'sh'
    if stock_code.startswith('0') or stock_code.startswith('3'):
        return 'sz'
    if stock_code.startswith('8') or stock_code.startswith('4'):
        return 'bj'
    return ''


def sanitize_filename_component(value: str) -> str:
    """Remove characters that are unsafe for filenames."""
    if not value:
        return ''
    value = str(value).strip()
    # Replace characters that cannot appear in filenames on common OSes.
    value = re.sub(r"[\\/:*?\"<>|]", "_", value)
    # Collapse whitespace to a single underscore for readability.
    value = re.sub(r"\s+", "_", value)
    return value.strip('_')


def build_report_filename(prefix: str, stock_code: str, stock_name: str, extension: str) -> str:
    """Construct a filename that embeds stock code and name."""
    today_str = datetime.now().strftime('%Y-%m-%d')
    code_part = sanitize_filename_component(stock_code)
    name_part = sanitize_filename_component(stock_name) or code_part
    return f"{prefix}_{code_part}_{name_part}_{today_str}.{extension}"


def fetch_stock_name(stock_code: str) -> str:
    """Attempt to retrieve the stock's short name from multiple data sources."""
    try:
        info_df = ak.stock_individual_info_em(symbol=stock_code)
        if info_df is not None and not info_df.empty:
            candidates = info_df[info_df['item'].isin(['证券简称', '股票简称', '公司简称', '公司名称'])]
            if not candidates.empty:
                return str(candidates['value'].iloc[0]).strip()
    except Exception:
        pass

    try:
        spot_df = ak.stock_zh_a_spot_em()
        if spot_df is not None and not spot_df.empty:
            match = spot_df[spot_df['代码'] == stock_code]
            if not match.empty:
                return str(match['名称'].iloc[0]).strip()
    except Exception:
        pass

    return stock_code


# -------------------------- Fundamental data section --------------------------

def get_latest_report_date() -> list:
    """动态获取最近的财报日期列表，用于需要日期的接口"""
    today = datetime.now()
    year = today.year
    report_dates = [
        f"{year-1}1231", f"{year}0930", f"{year}0630", f"{year}0331",
    ]
    return [date for date in report_dates if datetime.strptime(date, '%Y%m%d') < today]


def get_fundamental_data(stock_code: str) -> dict:
    print(f"--- 开始获取股票 {stock_code} 的基本面数据 ---")
    fundamental_data = {}
    market_prefix = get_stock_code_prefix(stock_code)
    stock_code_with_market_prefix = f"{market_prefix}{stock_code}"
    stock_code_with_market_prefix_dot = f"{stock_code}.{market_prefix.upper()}"

    print("\n[1/6] 正在获取公司概况...")
    try:
        df = ak.stock_individual_info_em(symbol=stock_code)
        fundamental_data['公司基本信息-东财'] = df
        print("  - 成功获取 [公司基本信息-东财]")
    except Exception as exc:
        print(f"  - [警告] 获取 [公司基本信息-东财] 失败: {exc}")

    try:
        df = ak.stock_zygc_em(symbol=stock_code_with_market_prefix.upper())
        fundamental_data['主营构成-东财'] = df
        print("  - 成功获取 [主营构成-东财]")
    except Exception as exc:
        print(f"  - [警告] 获取 [主营构成-东财] 失败: {exc}")

    print("\n[2/6] 正在获取财务报表...")
    try:
        df = ak.stock_financial_report_sina(stock=stock_code_with_market_prefix, symbol="资产负债表")
        fundamental_data['资产负债表'] = df
        print("  - 成功获取 [资产负债表]")
    except Exception as exc:
        print(f"  - [警告] 获取 [资产负债表] 失败: {exc}")

    try:
        df = ak.stock_financial_report_sina(stock=stock_code_with_market_prefix, symbol="利润表")
        fundamental_data['利润表'] = df
        print("  - 成功获取 [利润表]")
    except Exception as exc:
        print(f"  - [警告] 获取 [利润表] 失败: {exc}")

    try:
        df = ak.stock_financial_report_sina(stock=stock_code_with_market_prefix, symbol="现金流量表")
        fundamental_data['现金流量表'] = df
        print("  - 成功获取 [现金流量表]")
    except Exception as exc:
        print(f"  - [警告] 获取 [现金流量表] 失败: {exc}")

    print("\n[3/6] 正在获取核心财务指标...")
    try:
        df = ak.stock_financial_analysis_indicator_em(symbol=stock_code_with_market_prefix_dot)
        fundamental_data['主要财务指标-东财'] = df
        print("  - 成功获取 [主要财务指标-东财]")
    except Exception as exc:
        print(f"  - [警告] 获取 [主要财务指标-东财] 失败: {exc}")

    try:
        df = ak.stock_financial_abstract_ths(symbol=stock_code, indicator="按报告期")
        fundamental_data['财务摘要-同花顺'] = df
        print("  - 成功获取 [财务摘要-同花顺]")
    except Exception as exc:
        print(f"  - [警告] 获取 [财务摘要-同花顺] 失败: {exc}")

    print("\n[4/6] 正在获取股东研究数据...")
    latest_dates = sorted(get_latest_report_date(), reverse=True)
    shareholder_data_fetched = False
    for date in latest_dates:
        try:
            df_top10 = ak.stock_gdfx_top_10_em(symbol=stock_code_with_market_prefix, date=date)
            df_free_top10 = ak.stock_gdfx_free_top_10_em(symbol=stock_code_with_market_prefix, date=date)
            if df_top10 is not None and df_free_top10 is not None and not df_top10.empty and not df_free_top10.empty:
                fundamental_data['十大股东'] = df_top10
                fundamental_data['十大流通股东'] = df_free_top10
                print(f"  - 成功获取 [十大股东与流通股东] (报告期: {date})")
                shareholder_data_fetched = True
                break
        except Exception:
            continue
    if not shareholder_data_fetched:
        print("  - [警告] 获取 [十大股东与流通股东] 失败，已尝试多个报告期。")

    try:
        df = ak.stock_zh_a_gdhs_detail_em(symbol=stock_code)
        fundamental_data['股东户数变化'] = df
        print("  - 成功获取 [股东户数变化]")
    except Exception as exc:
        print(f"  - [警告] 获取 [股东户数变化] 失败: {exc}")

    print("\n[5/6] 正在获取分红历史...")
    try:
        df = ak.stock_history_dividend_detail(symbol=stock_code, indicator="分红")
        fundamental_data['历史分红详情'] = df
        print("  - 成功获取 [历史分红详情]")
    except Exception as exc:
        print(f"  - [警告] 获取 [历史分红详情] 失败: {exc}")

    print("\n[6/6] 正在获取盈利预测与研报...")
    try:
        df = ak.stock_profit_forecast_ths(symbol=stock_code, indicator="业绩预测详表-机构")
        fundamental_data['盈利预测'] = df
        print("  - 成功获取 [盈利预测]")
    except Exception as exc:
        print(f"  - [警告] 获取 [盈利预测] 失败: {exc}")

    try:
        df = ak.stock_research_report_em(symbol=stock_code)
        fundamental_data['个股研报'] = df
        print("  - 成功获取 [个股研报]")
    except Exception as exc:
        print(f"  - [警告] 获取 [个股研报] 失败: {exc}")

    print(f"\n--- 股票 {stock_code} 基本面数据获取完成 ---")
    return fundamental_data


def sort_dataframe_by_date(df: pd.DataFrame) -> pd.DataFrame:
    """Sort dataframe by common date columns descending; return original if none available."""
    for column in DATE_COLUMNS_PRIORITY:
        if column in df.columns:
            df_sorted = df.copy()
            df_sorted[column] = pd.to_datetime(df_sorted[column], errors='coerce')
            if df_sorted[column].notna().any():
                return df_sorted.sort_values(by=column, ascending=False)
    return df


def clean_and_format_df(df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    df_cleaned = df.dropna(axis=1, how='all')
    if not df_cleaned.empty and df_cleaned.columns[0] == '报告日' and any(keyword in sheet_name for keyword in ['资产负债表', '利润表', '现金流量表']):
        df_transposed = df_cleaned.set_index('报告日').transpose()
        df_transposed = df_transposed.reset_index().rename(columns={'index': '项目'})
        return df_transposed
    return df_cleaned


def save_fundamental_outputs(stock_code: str, stock_name: str, data_dict: dict) -> None:
    os.makedirs(REPORT_ROOT, exist_ok=True)

    excel_path = os.path.join(
        REPORT_ROOT,
        build_report_filename("fundamental_report", stock_code, stock_name, "xlsx"),
    )
    summary_path = os.path.join(
        REPORT_ROOT,
        build_report_filename("fundamental_summary", stock_code, stock_name, "txt"),
    )

    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            for sheet_name, df in data_dict.items():
                if df is not None and not df.empty:
                    formatted_df = clean_and_format_df(df.copy(), sheet_name)
                    safe_sheet_name = ''.join(c for c in sheet_name if c.isalnum() or c in (' ', '_'))[:31]
                    formatted_df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
        print(f"\n--- 基本面数据已成功保存至 Excel 文件: {excel_path} ---")
    except Exception as exc:
        print(f"\n--- [错误] 基本面 Excel 数据保存失败: {exc} ---")

    sheets_to_summarize = {
        '主营构成-东财': 'latest_date_table', '资产负债表': 'latest_row',
        '利润表': 'latest_row', '现金流量表': 'latest_row',
        '主要财务指标-东财': 'latest_row', '财务摘要-同花顺': 'latest_row',
        '十大股东': 'full_table', '十大流通股东': 'full_table',
        '股东户数变化': 'latest_row', '历史分红详情': 'latest_row',
        '盈利预测': 'full_table', '个股研报': 'last_month'
    }

    try:
        with open(summary_path, 'w', encoding='utf-8') as f:
            report_date = datetime.now().strftime('%Y-%m-%d')
            f.write(f"股票代码: {stock_code}\n")
            f.write(f"股票名称: {stock_name}\n")
            f.write(f"报告生成日期: {report_date}\n")
            f.write("==================================================\n\n")

            for name, summary_type in sheets_to_summarize.items():
                if name in data_dict and data_dict[name] is not None and not data_dict[name].empty:
                    df = data_dict[name].copy()
                    f.write(f"--------- {name} ---------\n")

                    summary_df = None
                    if summary_type == 'latest_date_table' and '报告日期' in df.columns:
                        df['报告日期'] = pd.to_datetime(df['报告日期'], errors='coerce')
                        df = df.sort_values(by='报告日期', ascending=False)
                        latest_date = df['报告日期'].dropna().max()
                        if pd.notna(latest_date):
                            summary_df = df[df['报告日期'] == latest_date]
                        else:
                            summary_df = df.head(1)
                    elif summary_type == 'full_table':
                        summary_df = sort_dataframe_by_date(df)
                    elif summary_type == 'latest_row':
                        summary_df = sort_dataframe_by_date(df).head(1)
                    elif summary_type == 'last_month' and '日期' in df.columns:
                        try:
                            df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
                            df = df.sort_values(by='日期', ascending=False)
                            one_month_ago = datetime.now() - pd.DateOffset(months=1)
                            summary_df = df[df['日期'] >= one_month_ago]
                            if summary_df.empty:
                                f.write("最近一个月内无相关研报，以下为最新的5条记录：\n")
                                summary_df = df.head(5)
                        except Exception:
                            summary_df = sort_dataframe_by_date(df).head(5)

                    if summary_df is not None and not summary_df.empty:
                        f.write(summary_df.to_string(index=False))
                    else:
                        f.write("无可用数据。")
                    f.write("\n\n")
        print(f"--- 基本面摘要已成功保存至 TXT 文件: {summary_path} ---")
    except Exception as exc:
        print(f"\n--- [错误] 基本面 TXT 摘要保存失败: {exc} ---")


# -------------------------- Technical & sentiment section --------------------------

def get_sentiment_data(stock_code: str) -> dict:
    print(f"--- 开始获取股票 {stock_code} 的市场博弈与技术分析数据 ---")
    sentiment_data = {}
    market_prefix = get_stock_code_prefix(stock_code)

    print("\n[1/5] 正在获取历史行情数据...")
    try:
        start_date_hist = (datetime.now() - pd.Timedelta(days=3 * 365)).strftime('%Y%m%d')
        end_date_hist = datetime.now().strftime('%Y%m%d')

        df_hfq = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date_hist, end_date=end_date_hist, adjust="hfq")
        sentiment_data['日K线-后复权'] = df_hfq
        print("  - 成功获取 [日K线-后复权] 数据")

        df_qfq = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date_hist, end_date=end_date_hist, adjust="qfq")
        sentiment_data['日K线-前复权'] = df_qfq
        print("  - 成功获取 [日K线-前复权] 数据")

        df_min = ak.stock_zh_a_hist_min_em(symbol=stock_code, period='5', adjust="qfq")
        sentiment_data['5分钟K线'] = df_min
        print("  - 成功获取 [5分钟K线] 数据")
    except Exception as exc:
        print(f"  - [警告] 获取 [历史行情数据] 失败: {exc}")

    print("\n[2/5] 正在获取资金流向数据...")
    try:
        df = ak.stock_individual_fund_flow(stock=stock_code, market=market_prefix)
        sentiment_data['个股资金流'] = df
        print("  - 成功获取 [个股资金流]")
    except Exception as exc:
        print(f"  - [警告] 获取 [个股资金流] 失败: {exc}")

    try:
        df = ak.stock_hsgt_individual_em(symbol=stock_code)
        sentiment_data['北向资金持股历史'] = df
        print("  - 成功获取 [北向资金持股历史]")
    except Exception as exc:
        print(f"  - [警告] 获取 [北向资金持股历史] 失败: {exc}")

    print("\n[3/5] 正在获取龙虎榜数据...")
    try:
        start_date_lhb = (datetime.now() - pd.Timedelta(days=365)).strftime('%Y%m%d')
        end_date_lhb = datetime.now().strftime('%Y%m%d')
        df_all = ak.stock_lhb_detail_em(start_date=start_date_lhb, end_date=end_date_lhb)
        if df_all is not None:
            df_filtered = df_all[df_all['代码'] == stock_code]
            if not df_filtered.empty:
                sentiment_data['龙虎榜详情'] = df_filtered
                print("  - 成功获取 [龙虎榜详情]")
            else:
                print(f"  - [信息] 股票 {stock_code} 近一年未上龙虎榜")
        else:
            print("  - [信息] 近一年未获取到任何龙虎榜数据。")
    except Exception as exc:
        print(f"  - [警告] 获取 [龙虎榜详情] 失败: {exc}")

    print("\n[4/5] 正在获取杠杆资金数据...")
    try:
        trade_date_df = ak.tool_trade_date_hist_sina()
        trade_date_df['trade_date'] = pd.to_datetime(trade_date_df['trade_date'])
        today = datetime.now().date()
        trade_date_df = trade_date_df[trade_date_df['trade_date'].dt.date <= today]

        margin_data_fetched = False
        for i in range(1, 6):
            if len(trade_date_df) < i:
                break
            trade_date = trade_date_df['trade_date'].iloc[-i]
            date_str = trade_date.strftime('%Y%m%d')
            print(f"  - 正在尝试获取 {date_str} 的融资融券数据...")
            try:
                if market_prefix == 'sh':
                    df_all = ak.stock_margin_detail_sse(date=date_str)
                    df = df_all[df_all['标的证券代码'] == stock_code]
                elif market_prefix == 'sz':
                    df_all = ak.stock_margin_detail_szse(date=date_str)
                    df = df_all[df_all['证券代码'] == stock_code]
                else:
                    df = pd.DataFrame()

                if df is not None and not df.empty:
                    sentiment_data['融资融券详情'] = df
                    print(f"  - 成功获取 [融资融券详情] (数据日期: {date_str})")
                    margin_data_fetched = True
                    break
                else:
                    print(f"  - [信息] {date_str} 数据为空，尝试前一个交易日...")
            except Exception:
                print(f"  - [信息] {date_str} 数据获取失败，尝试前一个交易日...")
                continue

        if not margin_data_fetched:
            print(f"  - [警告] 未能在最近5个交易日内找到股票 {stock_code} 的融资融券数据。")
    except Exception as exc:
        print(f"  - [严重警告] 获取 [融资融券详情] 失败: {exc}")

    print("\n[5/5] 正在获取市场热度数据...")
    try:
        df = ak.stock_comment_em()
        stock_comment_df = df[df['代码'] == stock_code]
        sentiment_data['千股千评'] = stock_comment_df
        print("  - 成功获取 [千股千评]")
    except Exception as exc:
        print(f"  - [警告] 获取 [千股千评] 失败: {exc}")

    try:
        df = ak.stock_hot_rank_em()
        stock_hot_rank_df = df[df['代码'] == stock_code]
        if not stock_hot_rank_df.empty:
            sentiment_data['A股人气榜'] = stock_hot_rank_df
            print("  - 成功获取 [A股人气榜]")
        else:
            print(f"  - [信息] 股票 {stock_code} 今日未进入人气榜")
    except Exception as exc:
        print(f"  - [警告] 获取 [A股人气榜] 失败: {exc}")

    print(f"\n--- 股票 {stock_code} 市场博弈数据获取完成 ---")
    return sentiment_data


def save_sentiment_outputs(stock_code: str, stock_name: str, data_dict: dict) -> None:
    os.makedirs(REPORT_ROOT, exist_ok=True)

    excel_path = os.path.join(
        REPORT_ROOT,
        build_report_filename("sentiment_report", stock_code, stock_name, "xlsx"),
    )
    summary_path = os.path.join(
        REPORT_ROOT,
        build_report_filename("sentiment_summary", stock_code, stock_name, "txt"),
    )

    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            for sheet_name, df in data_dict.items():
                if df is not None and not df.empty:
                    safe_sheet_name = ''.join(c for c in sheet_name if c.isalnum() or c in (' ', '_'))[:31]
                    df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
        print(f"\n--- 市场博弈数据已成功保存至 Excel 文件: {excel_path} ---")
    except Exception as exc:
        print(f"\n--- [错误] 市场博弈 Excel 数据保存失败: {exc} ---")

    sheets_to_summarize = [
        '日K线-前复权', '个股资金流', '北向资金持股历史',
        '融资融券详情', '龙虎榜详情', '千股千评', 'A股人气榜'
    ]

    try:
        with open(summary_path, 'w', encoding='utf-8') as f:
            report_date = datetime.now().strftime('%Y-%m-%d')
            f.write(f"股票代码: {stock_code}\n")
            f.write(f"股票名称: {stock_name}\n")
            f.write(f"报告生成日期: {report_date}\n")
            f.write("==================================================\n\n")

            for name in sheets_to_summarize:
                if name in data_dict and data_dict[name] is not None and not data_dict[name].empty:
                    df = data_dict[name]
                    f.write(f"--------- {name} ---------\n")
                    if '龙虎榜' in name:
                        summary_df = sort_dataframe_by_date(df)
                    else:
                        summary_df = sort_dataframe_by_date(df).head(1)
                    f.write(summary_df.to_string(index=False))
                    f.write("\n\n")
        print(f"--- 市场博弈摘要已成功保存至 TXT 文件: {summary_path} ---")
    except Exception as exc:
        print(f"\n--- [错误] 市场博弈 TXT 摘要保存失败: {exc} ---")


# -------------------------- Risk section --------------------------

def get_latest_trade_date() -> str:
    try:
        trade_date_df = ak.tool_trade_date_hist_sina()
        return trade_date_df['trade_date'].iloc[-1].strftime('%Y%m%d')
    except Exception:
        return (datetime.now() - pd.Timedelta(days=1)).strftime('%Y%m%d')


def get_risk_event_data(stock_code: str) -> dict:
    print(f"--- 开始获取股票 {stock_code} 的风险排查与特殊事件数据 ---")
    risk_data = {}

    print("\n[1/5] 正在获取股权质押数据...")
    try:
        latest_trade_date = get_latest_trade_date()
        df_all = ak.stock_gpzy_pledge_ratio_em(date=latest_trade_date)
        if df_all is not None:
            df_stock = df_all[df_all['股票代码'] == stock_code]
            if not df_stock.empty:
                risk_data['上市公司质押比例'] = df_stock
                print(f"  - 成功获取 [上市公司质押比例] (日期: {latest_trade_date})")
            else:
                print(f"  - [信息] 在 {latest_trade_date} 未查询到该股票的质押信息。")
        else:
            print(f"  - [信息] 在 {latest_trade_date} 未获取到任何股权质押数据，可能是节假日或数据源暂未更新。")
    except Exception as exc:
        print(f"  - [警告] 获取 [上市公司质押比例] 失败: {exc}")

    print("\n[2/5] 正在检查风险警示状态...")
    try:
        df_st = ak.stock_zh_a_st_em()
        if stock_code in df_st['代码'].values:
            risk_data['风险警示'] = df_st[df_st['代码'] == stock_code]
            print("  - [注意] 该股票在风险警示板中！")
        else:
            print("  - [信息] 该股票不在风险警示板中。")
    except Exception as exc:
        print(f"  - [警告] 获取 [风险警示] 数据失败: {exc}")

    print("\n[3/5] 正在获取限售解禁数据...")
    try:
        df = ak.stock_restricted_release_queue_em(symbol=stock_code)
        if df is not None and not df.empty:
            risk_data['限售解禁'] = df
            print("  - 成功获取 [限售解禁] 时间表")
        else:
            print("  - [信息] 未查询到该股票的限售解禁安排。")
    except Exception as exc:
        print(f"  - [警告] 获取 [限售解禁] 失败: {exc}")

    print("\n[4/5] 正在获取高管与股东交易数据...")
    print("  - [提示] 正在下载全市场数据进行匹配，此过程可能需要1-2分钟，请稍候...")
    try:
        df_all_trades = ak.stock_ggcg_em(symbol="全部")
        df_stock_trades = df_all_trades[df_all_trades['代码'] == stock_code]
        if not df_stock_trades.empty:
            risk_data['高管股东交易'] = df_stock_trades
            print(f"  - 成功获取 [高管股东交易] 数据，共 {len(df_stock_trades)} 条记录")
        else:
            print("  - [信息] 未查询到该股票的高管股东交易记录。")
    except Exception as exc:
        print(f"  - [警告] 获取 [高管股东交易] 失败: {exc}")

    print("\n[5/5] 正在获取近期公司公告...")
    try:
        start_date_announce = (datetime.now() - pd.Timedelta(days=90)).strftime('%Y%m%d')
        end_date_announce = datetime.now().strftime('%Y%m%d')
        df = ak.stock_zh_a_disclosure_report_cninfo(symbol=stock_code, market="沪深京", start_date=start_date_announce, end_date=end_date_announce)
        if df is not None and not df.empty:
            risk_data['近期公司公告'] = df
            print("  - 成功获取 [近期公司公告]")
        else:
            print("  - [信息] 近90天未查询到公司公告。")
    except Exception as exc:
        print(f"  - [警告] 获取 [近期公司公告] 失败: {exc}")

    print(f"\n--- 股票 {stock_code} 风险排查数据获取完成 ---")
    return risk_data


def save_risk_outputs(stock_code: str, stock_name: str, data_dict: dict) -> None:
    os.makedirs(REPORT_ROOT, exist_ok=True)

    excel_path = os.path.join(
        REPORT_ROOT,
        build_report_filename("risk_report", stock_code, stock_name, "xlsx"),
    )
    summary_path = os.path.join(
        REPORT_ROOT,
        build_report_filename("risk_summary", stock_code, stock_name, "txt"),
    )

    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            for sheet_name, df in data_dict.items():
                if df is not None and not df.empty:
                    safe_sheet_name = ''.join(c for c in sheet_name if c.isalnum() or c in (' ', '_'))[:31]
                    df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
        print(f"\n--- 风险数据已成功保存至 Excel 文件: {excel_path} ---")
    except Exception as exc:
        print(f"\n--- [错误] 风险 Excel 数据保存失败: {exc} ---")

    try:
        with open(summary_path, 'w', encoding='utf-8') as f:
            report_date = datetime.now().strftime('%Y-%m-%d')
            f.write(f"股票代码: {stock_code}\n")
            f.write(f"股票名称: {stock_name}\n")
            f.write(f"报告生成日期: {report_date}\n")
            f.write("==================================================\n\n")

            if '风险警示' in data_dict and data_dict['风险警示'] is not None and not data_dict['风险警示'].empty:
                f.write("--------- !!! 风险警示 !!! ---------\n")
                f.write("该股票在风险警示板中，请高度注意风险！\n")
                f.write(data_dict['风险警示'].to_string(index=False))
                f.write("\n\n")

            if '上市公司质押比例' in data_dict and data_dict['上市公司质押比例'] is not None and not data_dict['上市公司质押比例'].empty:
                f.write("--------- 最新股权质押情况 ---------\n")
                f.write(data_dict['上市公司质押比例'].to_string(index=False))
                f.write("\n\n")

            if '限售解禁' in data_dict and data_dict['限售解禁'] is not None and not data_dict['限售解禁'].empty:
                f.write("--------- 未来限售解禁安排 ---------\n")
                f.write(data_dict['限售解禁'].to_string(index=False))
                f.write("\n\n")

            if '高管股东交易' in data_dict and data_dict['高管股东交易'] is not None and not data_dict['高管股东交易'].empty:
                f.write("--------- 近期高管股东交易 (最多显示10条) ---------\n")
                f.write(data_dict['高管股东交易'].head(10).to_string(index=False))
                f.write("\n\n")

            if '近期公司公告' in data_dict and data_dict['近期公司公告'] is not None and not data_dict['近期公司公告'].empty:
                f.write("--------- 近期公司公告 (最多显示10条) ---------\n")
                f.write(data_dict['近期公司公告'][['公告标题', '公告时间']].head(10).to_string(index=False))
                f.write("\n\n")
        print(f"--- 风险摘要已成功保存至 TXT 文件: {summary_path} ---")
    except Exception as exc:
        print(f"\n--- [错误] 风险 TXT 摘要保存失败: {exc} ---")


# -------------------------- Orchestration --------------------------

def preview_data(title: str, data_dict: dict) -> None:
    print("\n\n========================= 数据预览 (仅显示头部) =========================")
    print(title)
    for name, df in data_dict.items():
        print(f"\n--------- {name} ---------")
        if df is not None and not df.empty:
            print(df.head())
        else:
            print("未能获取到数据或数据为空。")
    print("\n========================================================================")


def main(stock_code: str) -> None:
    stock_code = stock_code.strip()
    stock_name = fetch_stock_name(stock_code)
    print(f"目标股票: {stock_code} ({stock_name})")

    fundamental_data = get_fundamental_data(stock_code)
    save_fundamental_outputs(stock_code, stock_name, fundamental_data)
    preview_data("基本面数据", fundamental_data)

    sentiment_data = get_sentiment_data(stock_code)
    save_sentiment_outputs(stock_code, stock_name, sentiment_data)
    preview_data("市场博弈数据", sentiment_data)

    risk_data = get_risk_event_data(stock_code)
    save_risk_outputs(stock_code, stock_name, risk_data)
    preview_data("风险排查数据", risk_data)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="获取单只A股的基本面、技术面与风险数据并保存报告")
    parser.add_argument('-c', '--code', dest='stock_code', default='600519', help='6位A股股票代码，例如 600519')
    return parser.parse_args()


if __name__ == '__main__':
    STOCK_CODES = [
    # '603019',   # 中科曙光
    # '000032',   # 深桑达A
    # '603279',   # 景津装备
    # '600395',   # 盘江股份
    # '601918',   # 新集能源

    # # 关注股票
    # '601138',       # 工业富联
    # '300308',      # 中际旭创
    # '002230',      # 科大讯飞
    # '002463',      # 沪电股份
    # '600938',     # 中海油
    # '600519',     # 贵州茅台
    # '000977',     # 浪潮信息
    '603398',     # st沐邦 
    ]
    for code in STOCK_CODES:
        main(code)
        print("\n\n" + "="*80 + "\n\n")

    # stock_code = '603019'   # 中科曙光
    # main(stock_code)
