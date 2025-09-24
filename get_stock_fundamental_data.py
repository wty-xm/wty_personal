# -*- coding: utf-8 -*-
# @Author: TY (Your Investment Advisor)
# @Date: 2025-09-24
# @Version: 6.0
# @Description: A script to fetch comprehensive fundamental data for a specific stock,
#               with data cleaning and formatting for better readability in Excel,
#               and generate both a detailed Excel report and an enhanced summary TXT file.

import akshare as ak
import pandas as pd
from datetime import datetime
import os
import warnings

# --- 忽略一些 akshare 可能产生的警告信息 ---
warnings.filterwarnings("ignore")

# --- 配置 Pandas 显示 ---
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def get_stock_code_prefix(stock_code: str) -> str:
    """判断股票代码的市场前缀"""
    if stock_code.startswith('6'):
        return 'sh'
    elif stock_code.startswith('0') or stock_code.startswith('3'):
        return 'sz'
    elif stock_code.startswith('8') or stock_code.startswith('4'):
        return 'bj'
    return ''

def get_latest_report_date() -> list:
    """动态获取最近的财报日期列表，用于需要日期的接口"""
    today = datetime.now()
    year = today.year
    report_dates = [
        f"{year-1}1231", f"{year}0930", f"{year}0630", f"{year}0331",
    ]
    valid_dates = [date for date in report_dates if datetime.strptime(date, '%Y%m%d') < today]
    return sorted(valid_dates, reverse=True)


def get_fundamental_data(stock_code: str):
    """
    获取指定股票代码的全面基本面数据。

    :param stock_code: 6位A股股票代码，例如 '600519'
    :return: 一个字典，键为数据名称，值为对应的 Pandas DataFrame
    """
    print(f"--- 开始获取股票 {stock_code} 的基本面数据 (优化版 V6) ---")
    fundamental_data = {}
    market_prefix = get_stock_code_prefix(stock_code)
    stock_code_with_market_prefix = f"{market_prefix}{stock_code}"
    stock_code_with_market_prefix_dot = f"{stock_code}.{market_prefix.upper()}"
    
    # --- 1. 公司概况 ---
    print("\n[1/6] 正在获取公司概况...")
    try:
        df = ak.stock_individual_info_em(symbol=stock_code)
        fundamental_data['公司基本信息-东财'] = df
        print("  - 成功获取 [公司基本信息-东财]")
    except Exception as e:
        print(f"  - [警告] 获取 [公司基本信息-东财] 失败: {e}")

    try:
        df = ak.stock_zygc_em(symbol=stock_code_with_market_prefix.upper())
        fundamental_data['主营构成-东财'] = df
        print("  - 成功获取 [主营构成-东财]")
    except Exception as e:
        print(f"  - [警告] 获取 [主营构成-东财] 失败: {e}")

    # --- 2. 财务报表 ---
    print("\n[2/6] 正在获取财务报表...")
    try:
        df = ak.stock_financial_report_sina(stock=stock_code_with_market_prefix, symbol="资产负债表")
        fundamental_data['资产负债表'] = df
        print("  - 成功获取 [资产负债表]")
    except Exception as e:
        print(f"  - [警告] 获取 [资产负债表] 失败: {e}")
        
    try:
        df = ak.stock_financial_report_sina(stock=stock_code_with_market_prefix, symbol="利润表")
        fundamental_data['利润表'] = df
        print("  - 成功获取 [利润表]")
    except Exception as e:
        print(f"  - [警告] 获取 [利润表] 失败: {e}")
        
    try:
        df = ak.stock_financial_report_sina(stock=stock_code_with_market_prefix, symbol="现金流量表")
        fundamental_data['现金流量表'] = df
        print("  - 成功获取 [现金流量表]")
    except Exception as e:
        print(f"  - [警告] 获取 [现金流量表] 失败: {e}")

    # --- 3. 核心财务指标 ---
    print("\n[3/6] 正在获取核心财务指标...")
    try:
        df = ak.stock_financial_analysis_indicator_em(symbol=stock_code_with_market_prefix_dot)
        fundamental_data['主要财务指标-东财'] = df
        print("  - 成功获取 [主要财务指标-东财]")
    except Exception as e:
        print(f"  - [警告] 获取 [主要财务指标-东财] 失败: {e}")

    try:
        df = ak.stock_financial_abstract_ths(symbol=stock_code, indicator="按报告期")
        fundamental_data['财务摘要-同花顺'] = df
        print("  - 成功获取 [财务摘要-同花顺]")
    except Exception as e:
        print(f"  - [警告] 获取 [财务摘要-同花顺] 失败: {e}")
        
    # --- 4. 股东研究 ---
    print("\n[4/6] 正在获取股东研究数据...")
    latest_dates = get_latest_report_date()
    shareholder_data_fetched = False
    for date in latest_dates:
        try:
            df_top10 = ak.stock_gdfx_top_10_em(symbol=stock_code_with_market_prefix, date=date)
            df_free_top10 = ak.stock_gdfx_free_top_10_em(symbol=stock_code_with_market_prefix, date=date)
            if not df_top10.empty and not df_free_top10.empty:
                fundamental_data['十大股东'] = df_top10
                fundamental_data['十大流通股东'] = df_free_top10
                print(f"  - 成功获取 [十大股东与流通股东] (报告期: {date})")
                shareholder_data_fetched = True
                break
        except Exception:
            continue
    if not shareholder_data_fetched:
        print(f"  - [警告] 获取 [十大股东与流通股东] 失败，已尝试多个报告期。")
        
    try:
        df = ak.stock_zh_a_gdhs_detail_em(symbol=stock_code)
        fundamental_data['股东户数变化'] = df
        print("  - 成功获取 [股东户数变化]")
    except Exception as e:
        print(f"  - [警告] 获取 [股东户数变化] 失败: {e}")
        
    # --- 5. 分红历史 ---
    print("\n[5/6] 正在获取分红历史...")
    try:
        df = ak.stock_history_dividend_detail(symbol=stock_code, indicator="分红")
        fundamental_data['历史分红详情'] = df
        print("  - 成功获取 [历史分红详情]")
    except Exception as e:
        print(f"  - [警告] 获取 [历史分红详情] 失败: {e}")

    # --- 6. 盈利预测与研报 ---
    print("\n[6/6] 正在获取盈利预测与研报...")
    try:
        df = ak.stock_profit_forecast_ths(symbol=stock_code, indicator="业绩预测详表-机构")
        fundamental_data['盈利预测'] = df
        print("  - 成功获取 [盈利预测]")
    except Exception as e:
        print(f"  - [警告] 获取 [盈利预测] 失败: {e}")
        
    try:
        df = ak.stock_research_report_em(symbol=stock_code)
        fundamental_data['个股研报'] = df
        print("  - 成功获取 [个股研报]")
    except Exception as e:
        print(f"  - [警告] 获取 [个股研报] 失败: {e}")
        
    print(f"\n--- 股票 {stock_code} 基本面数据获取完成 ---")
    return fundamental_data

def clean_and_format_df(df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    """
    对特定的 DataFrame 进行清洗和格式化，以提高可读性。
    """
    df_cleaned = df.dropna(axis=1, how='all')
    
    if '资产负债表' in sheet_name or '利润表' in sheet_name or '现金流量表' in sheet_name:
        if df_cleaned.columns[0] == '报告日':
            df_transposed = df_cleaned.set_index('报告日').transpose()
            df_transposed = df_transposed.reset_index().rename(columns={'index': '项目'})
            return df_transposed

    return df_cleaned


def save_data_to_excel(stock_code: str, data_dict: dict):
    """
    将获取到的数据清洗、格式化后保存到特定文件夹的 Excel 文件中。
    
    :param stock_code: 股票代码
    :param data_dict: 包含 Pandas DataFrame 的字典
    """
    today_str = datetime.now().strftime('%Y-%m-%d')
    folder_name = "fundamental_data_reports"
    os.makedirs(folder_name, exist_ok=True)
    
    file_name = f"fundamental_report_{stock_code}_{today_str}.xlsx"
    file_path = os.path.join(folder_name, file_name)
    
    try:
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            for sheet_name, df in data_dict.items():
                if df is not None and not df.empty:
                    formatted_df = clean_and_format_df(df.copy(), sheet_name)
                    safe_sheet_name = ''.join(c for c in sheet_name if c.isalnum() or c in (' ', '_'))[:31]
                    formatted_df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
        print(f"\n--- 数据已成功保存至 Excel 文件: {file_path} ---")
    except Exception as e:
        print(f"\n--- [错误] Excel 数据保存失败: {e} ---")

def save_summary_to_txt(stock_code: str, data_dict: dict):
    """
    将关键的最新日期数据提取并保存为 TXT 摘要文件。

    :param stock_code: 股票代码
    :param data_dict: 包含 Pandas DataFrame 的字典
    """
    today_str = datetime.now().strftime('%Y-%m-%d')
    folder_name = "fundamental_data_reports"
    os.makedirs(folder_name, exist_ok=True)
    
    file_name = f"fundamental_summary_{stock_code}_{today_str}.txt"
    file_path = os.path.join(folder_name, file_name)

    sheets_to_summarize = {
        '主营构成-东财': 'latest_date_table', '资产负债表': 'latest_row',
        '利润表': 'latest_row', '现金流量表': 'latest_row',
        '主要财务指标-东财': 'latest_row', '财务摘要-同花顺': 'latest_row',
        '十大股东': 'full_table', '十大流通股东': 'full_table',
        '股东户数变化': 'latest_row', '历史分红详情': 'latest_row',
        '盈利预测': 'full_table', '个股研报': 'last_month'
    }
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"股票代码: {stock_code} - 基本面数据摘要\n")
            f.write(f"报告生成日期: {today_str}\n")
            f.write("==================================================\n\n")

            for name, summary_type in sheets_to_summarize.items():
                if name in data_dict and data_dict[name] is not None and not data_dict[name].empty:
                    df = data_dict[name].copy()
                    f.write(f"--------- {name} ---------\n")
                    
                    summary_df = None
                    if summary_type == 'latest_date_table' and '报告日期' in df.columns:
                        latest_date = df['报告日期'].max()
                        summary_df = df[df['报告日期'] == latest_date]
                    elif summary_type == 'full_table':
                        summary_df = df
                    elif summary_type == 'latest_row':
                        summary_df = df.head(1)
                    elif summary_type == 'last_month' and '日期' in df.columns:
                        try:
                            df['日期'] = pd.to_datetime(df['日期'])
                            one_month_ago = datetime.now() - pd.DateOffset(months=1)
                            summary_df = df[df['日期'] >= one_month_ago]
                            # 如果最近一个月没有数据，则显示最新的5条作为备选
                            if summary_df.empty:
                                f.write("最近一个月内无相关研报，以下为最新的5条记录：\n")
                                summary_df = df.head(5)
                        except Exception:
                            summary_df = df.head(5) # Fallback to show latest 5
                    
                    if summary_df is not None and not summary_df.empty:
                        f.write(summary_df.to_string(index=False))
                    else:
                        f.write("无可用数据。")
                        
                    f.write("\n\n")
        
        print(f"--- 摘要数据已成功保存至 TXT 文件: {file_path} ---")
    except Exception as e:
        print(f"\n--- [错误] TXT 摘要保存失败: {e} ---")


if __name__ == '__main__':
    # --- 使用说明 ---
    # 1. 确保已安装所需库: pip install akshare pandas openpyxl
    # 2. 在下方修改为您想查询的股票代码
    target_stock_code = '600519'  # 示例：贵州茅台
    # target_stock_code = '000001'  # 示例：平安银行
    # target_stock_code = '300750'  # 示例：宁德时代
    
    stock_data = get_fundamental_data(target_stock_code)
    save_data_to_excel(target_stock_code, stock_data)
    save_summary_to_txt(target_stock_code, stock_data)

    print("\n\n========================= 数据预览 (仅显示头部) =========================")
    for name, df in stock_data.items():
        print(f"\n--------- {name} ---------")
        if df is not None and not df.empty:
            print(df.head())
        else:
            print("未能获取到数据或数据为空。")
    print("\n========================================================================")

