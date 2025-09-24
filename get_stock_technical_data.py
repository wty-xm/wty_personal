# -*- coding: utf-8 -*-
# @Author: TY (Your Investment Advisor)
# @Date: 2025-09-24
# @Version: 7.0
# @Description: A script to fetch comprehensive market sentiment and technical analysis data
#               for a specific stock. This version fixes a TypeError by ensuring the
#               trading calendar date column is always in datetime format.

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

def get_sentiment_data(stock_code: str):
    """
    获取指定股票代码的市场博弈与技术分析数据。

    :param stock_code: 6位A股股票代码，例如 '600519'
    :return: 一个字典，键为数据名称，值为对应的 Pandas DataFrame
    """
    print(f"--- 开始获取股票 {stock_code} 的市场博弈与技术分析数据 (V7) ---")
    sentiment_data = {}
    market_prefix = get_stock_code_prefix(stock_code)
    
    # --- 1. 历史行情数据 ---
    print("\n[1/5] 正在获取历史行情数据...")
    try:
        start_date_hist = (datetime.now() - pd.Timedelta(days=3*365)).strftime('%Y%m%d')
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
    except Exception as e:
        print(f"  - [警告] 获取 [历史行情数据] 失败: {e}")

    # --- 2. 资金流向 ---
    print("\n[2/5] 正在获取资金流向数据...")
    try:
        df = ak.stock_individual_fund_flow(stock=stock_code, market=market_prefix)
        sentiment_data['个股资金流'] = df
        print("  - 成功获取 [个股资金流]")
    except Exception as e:
        print(f"  - [警告] 获取 [个股资金流] 失败: {e}")

    try:
        df = ak.stock_hsgt_individual_em(symbol=stock_code)
        sentiment_data['北向资金持股历史'] = df
        print("  - 成功获取 [北向资金持股历史]")
    except Exception as e:
        print(f"  - [警告] 获取 [北向资金持股历史] 失败: {e}")
        
    # --- 3. 龙虎榜 ---
    print("\n[3/5] 正在获取龙虎榜数据...")
    try:
        start_date_lhb = (datetime.now() - pd.Timedelta(days=365)).strftime('%Y%m%d')
        end_date_lhb = datetime.now().strftime('%Y%m%d')
        df_all = ak.stock_lhb_detail_em(start_date=start_date_lhb, end_date=end_date_lhb)
        if df_all is not None:
            df_filtered = df_all[df_all['代码'] == stock_code]
            if not df_filtered.empty:
                sentiment_data['龙虎榜详情'] = df_filtered
                print(f"  - 成功获取 [龙虎榜详情]")
            else:
                print(f"  - [信息] 股票 {stock_code} 近一年未上龙虎榜")
        else:
            print(f"  - [信息] 近一年未获取到任何龙虎榜数据。")
    except Exception as e:
        print(f"  - [警告] 获取 [龙虎榜详情] 失败: {e}")

    # --- 4. 杠杆资金 ---
    print("\n[4/5] 正在获取杠杆资金数据...")
    try:
        trade_date_df = ak.tool_trade_date_hist_sina()
        # 修正：强制将日期列转换为datetime对象，防止类型错误
        trade_date_df['trade_date'] = pd.to_datetime(trade_date_df['trade_date'])
        today = datetime.now().date()
        trade_date_df = trade_date_df[trade_date_df['trade_date'].dt.date <= today]

        margin_data_fetched = False
        # 尝试获取最近5个交易日的数据，从最新的一天开始
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
                    df = pd.DataFrame()  # 北交所暂无

                if df is not None and not df.empty:
                    sentiment_data['融资融券详情'] = df
                    print(f"  - 成功获取 [融资融券详情] (数据日期: {date_str})")
                    margin_data_fetched = True
                    break  # 成功获取后即退出循环
                else:
                    print(f"  - [信息] {date_str} 数据为空，尝试前一个交易日...")
            except Exception:
                print(f"  - [信息] {date_str} 数据获取失败，尝试前一个交易日...")
                continue
        
        if not margin_data_fetched:
            print(f"  - [警告] 未能在最近5个交易日内找到股票 {stock_code} 的融资融券数据。")

    except Exception as e:
        print(f"  - [严重警告] 获取 [融资融券详情] 失败: {e}")


    # --- 5. 市场热度 ---
    print("\n[5/5] 正在获取市场热度数据...")
    try:
        df = ak.stock_comment_em()
        stock_comment_df = df[df['代码'] == stock_code]
        sentiment_data['千股千评'] = stock_comment_df
        print("  - 成功获取 [千股千评]")
    except Exception as e:
        print(f"  - [警告] 获取 [千股千评] 失败: {e}")
        
    try:
        df = ak.stock_hot_rank_em()
        stock_hot_rank_df = df[df['代码'] == stock_code]
        if not stock_hot_rank_df.empty:
            sentiment_data['A股人气榜'] = stock_hot_rank_df
            print("  - 成功获取 [A股人气榜]")
        else:
            print(f"  - [信息] 股票 {stock_code} 今日未进入人气榜")
    except Exception as e:
        print(f"  - [警告] 获取 [A股人气榜] 失败: {e}")

    print(f"\n--- 股票 {stock_code} 市场博弈数据获取完成 ---")
    return sentiment_data

def save_data_to_excel(stock_code: str, data_dict: dict):
    """
    将获取到的数据保存到特定文件夹的 Excel 文件中。
    """
    today_str = datetime.now().strftime('%Y-%m-%d')
    folder_name = "market_sentiment_reports"
    os.makedirs(folder_name, exist_ok=True)
    
    file_name = f"sentiment_report_{stock_code}_{today_str}.xlsx"
    file_path = os.path.join(folder_name, file_name)
    
    try:
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            for sheet_name, df in data_dict.items():
                if df is not None and not df.empty:
                    safe_sheet_name = ''.join(c for c in sheet_name if c.isalnum() or c in (' ', '_'))[:31]
                    df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
        print(f"\n--- 数据已成功保存至 Excel 文件: {file_path} ---")
    except Exception as e:
        print(f"\n--- [错误] Excel 数据保存失败: {e} ---")

def save_summary_to_txt(stock_code: str, data_dict: dict):
    """
    将关键的最新数据提取并保存为 TXT 摘要文件。
    """
    today_str = datetime.now().strftime('%Y-%m-%d')
    folder_name = "market_sentiment_reports"
    os.makedirs(folder_name, exist_ok=True)
    
    file_name = f"sentiment_summary_{stock_code}_{today_str}.txt"
    file_path = os.path.join(folder_name, file_name)

    sheets_to_summarize = [
        '日K线-前复权', '个股资金流', '北向资金持股历史', 
        '融资融券详情', '龙虎榜详情', '千股千评', 'A股人气榜'
    ]
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"股票代码: {stock_code} - 市场博弈数据摘要\n")
            f.write(f"报告生成日期: {today_str}\n")
            f.write("==================================================\n\n")

            for name in sheets_to_summarize:
                if name in data_dict and data_dict[name] is not None and not data_dict[name].empty:
                    df = data_dict[name]
                    f.write(f"--------- {name} ---------\n")
                    
                    if '龙虎榜' in name:
                        summary_df = df
                    else:
                        summary_df = df.tail(1)
                    
                    f.write(summary_df.to_string(index=False))
                    f.write("\n\n")
        
        print(f"--- 摘要数据已成功保存至 TXT 文件: {file_path} ---")
    except Exception as e:
        print(f"\n--- [错误] TXT 摘要保存失败: {e} ---")

if __name__ == '__main__':
    # --- 使用说明 ---
    # 1. 确保已安装所需库: pip install akshare pandas openpyxl python-dateutil
    # 2. 在下方修改为您想查询的股票代码
    target_stock_code = '600519'  # 示例：贵州茅台
    # target_stock_code = '000001'  # 示例：平安银行
    # target_stock_code = '300750'  # 示例：宁德时代
    
    sentiment_analysis_data = get_sentiment_data(target_stock_code)
    save_data_to_excel(target_stock_code, sentiment_analysis_data)
    save_summary_to_txt(target_stock_code, sentiment_analysis_data)
    
    print("\n\n========================= 数据预览 (仅显示头部) =========================")
    for name, df in sentiment_analysis_data.items():
        print(f"\n--------- {name} ---------")
        if df is not None and not df.empty:
            print(df.head())
        else:
            print("未能获取到数据或数据为空。")
    print("\n========================================================================")

