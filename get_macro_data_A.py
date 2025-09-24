# -*- coding: utf-8 -*-
# @Author: TY (Your Investment Advisor)
# @Date: 2025-09-24
# @Version: 3.0
# @Description: A script to fetch comprehensive macro and market overview data,
#               save it to a dated folder, and generate a summary text file.

import akshare as ak
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import warnings
import os

# --- 忽略一些 akshare 可能产生的警告信息 ---
warnings.filterwarnings("ignore")

# --- 配置 Pandas 显示 ---
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

def get_macro_market_data():
    """
    获取宏观与市场概览数据，作为我们自上而下分析的基础。

    :return: 一个字典，键为数据名称，值为对应的 Pandas DataFrame
    """
    print("--- 开始获取宏观与市场概览数据 ---")
    macro_data = {}
    
    # 动态计算上个月的日期字符串，格式为 "YYYYMM"
    last_month = datetime.now() - relativedelta(months=1)
    last_month_str = last_month.strftime('%Y%m')
    print(f"将使用 {last_month_str} 作为部分接口的查询月份。")

    # --- 1. 市场整体概览 ---
    print("\n[1/5] 正在获取市场整体概览...")
    try:
        df = ak.stock_sse_summary()
        macro_data['上交所市场总貌'] = df
        print("  - 成功获取 [上交所市场总貌]")
    except Exception as e:
        print(f"  - [警告] 获取 [上交所市场总貌] 失败: {e}")
        
    try:
        # 智能获取最近的交易日
        trade_date_df = ak.tool_trade_date_hist_sina()
        latest_trade_date = trade_date_df['trade_date'].iloc[-1].strftime('%Y%m%d')
        
        df = ak.stock_szse_summary(date=latest_trade_date)
        macro_data['深交所市场总貌'] = df
        print(f"  - 成功获取 [深交所市场总貌] (日期: {latest_trade_date})")
    except Exception as e:
        print(f"  - [警告] 获取 [深交所市场总貌] 失败: {e}")

    # --- 2. 整体估值水平 ---
    print("\n[2/5] 正在获取整体估值水平...")
    try:
        df = ak.stock_ebs_lg()
        macro_data['股债利差'] = df
        print("  - 成功获取 [股债利差]")
    except Exception as e:
        print(f"  - [警告] 获取 [股债利差] 失败: {e}")

    try:
        df = ak.stock_buffett_index_lg()
        macro_data['巴菲特指标'] = df
        print("  - 成功获取 [巴菲特指标]")
    except Exception as e:
        print(f"  - [警告] 获取 [巴菲特指标] 失败: {e}")

    try:
        df = ak.stock_a_ttm_lyr()
        macro_data['A股PE_PB'] = df
        print("  - 成功获取 [A股PE_PB]")
    except Exception as e:
        print(f"  - [警告] 获取 [A股PE_PB] 失败: {e}")

    try:
        df = ak.stock_a_all_pb()
        macro_data['A股PB'] = df
        print("  - 成功获取 [A股PB]")
    except Exception as e:
        print(f"  - [警告] 获取 [A股PB] 失败: {e}")
        
    try:
        df = ak.stock_market_pe_lg(symbol="上证")
        macro_data['上证A股平均市盈率'] = df
        print("  - 成功获取 [上证A股平均市盈率]")
    except Exception as e:
        print(f"  - [警告] 获取 [上证A股平均市盈率] 失败: {e}")
        
    try:
        df = ak.stock_index_pe_lg(symbol="沪深300")
        macro_data['沪深300平均市盈率'] = df
        print("  - 成功获取 [沪深300平均市盈率]")
    except Exception as e:
        print(f"  - [警告] 获取 [沪深300平均市盈率] 失败: {e}")

    # --- 3. 股息率 ---
    print("\n[3/5] 正在获取股息率...")
    try:
        df = ak.stock_a_gxl_lg(symbol="上证A股")
        macro_data['上证A股股息率'] = df
        print("  - 成功获取 [上证A股股息率]")
    except Exception as e:
        print(f"  - [警告] 获取 [上证A股股息率] 失败: {e}")
        
    try:
        df = ak.stock_a_gxl_lg(symbol="深证A股")
        macro_data['深证A股股息率'] = df
        print("  - 成功获取 [深证A股股息率]")
    except Exception as e:
        print(f"  - [警告] 获取 [深证A股股息率] 失败: {e}")
        
    # --- 4. 市场宽度与情绪 ---
    print("\n[4/5] 正在获取市场宽度与情绪指标...")
    try:
        df = ak.stock_a_high_low_statistics(symbol="all")
        macro_data['全部A股-新高新低数'] = df
        print("  - 成功获取 [全部A股-新高新低数]")
    except Exception as e:
        print(f"  - [警告] 获取 [全部A股-新高新低数] 失败: {e}")
        
    try:
        df = ak.stock_a_below_net_asset_statistics(symbol="全部A股")
        macro_data['全部A股-破净股统计'] = df
        print("  - 成功获取 [全部A股-破净股统计]")
    except Exception as e:
        print(f"  - [警告] 获取 [全部A股-破净股统计] 失败: {e}")

    # --- 5. 系统性风险指标 ---
    print("\n[5/5] 正在获取系统性风险指标...")
    try:
        df = ak.stock_gpzy_profile_em()
        macro_data['A股股权质押概况'] = df
        print("  - 成功获取 [A股股权质押概况]")
    except Exception as e:
        print(f"  - [警告] 获取 [A股股权质押概况] 失败: {e}")

    print("\n--- 宏观数据获取完成 ---")
    return macro_data

def save_and_summarize_data(data_dict: dict):
    """
    将数据保存到带日期的文件夹和 Excel 文件中，并生成一份TXT摘要。
    
    :param data_dict: 包含 Pandas DataFrame 的字典
    """
    today_str = datetime.now().strftime('%Y-%m-%d')
    folder_name = "macro_data_reports"
    
    # 确保文件夹存在
    os.makedirs(folder_name, exist_ok=True)
    
    excel_file_path = os.path.join(folder_name, f"macro_report_{today_str}.xlsx")
    txt_file_path = os.path.join(folder_name, f"macro_summary_{today_str}.txt")
    
    # 1. 保存完整的 Excel 文件
    try:
        with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
            for sheet_name, df in data_dict.items():
                if df is not None and not df.empty:
                    safe_sheet_name = ''.join(c for c in sheet_name if c.isalnum() or c in (' ', '_'))[:31]
                    df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
        print(f"\n--- 完整数据已成功保存至文件: {excel_file_path} ---")
    except Exception as e:
        print(f"\n--- [错误] Excel 文件保存失败: {e} ---")

    # 2. 生成并保存 TXT 摘要文件
    exclude_sheets_for_summary = ['上交所市场总貌', '深交所市场总貌']
    try:
        with open(txt_file_path, 'w', encoding='utf-8') as f:
            f.write(f"宏观市场数据最新一日摘要 - {today_str}\n")
            f.write("="*50 + "\n\n")
            
            for name, df in data_dict.items():
                if name not in exclude_sheets_for_summary and df is not None and not df.empty:
                    f.write(f"--------- {name} ---------\n")
                    # 使用 to_string() 来获得更好的格式化输出
                    latest_data_str = df.iloc[[-1]].to_string(index=False)
                    f.write(latest_data_str)
                    f.write("\n\n" + "-"*50 + "\n\n")
        print(f"--- 摘要文件已成功生成: {txt_file_path} ---")
    except Exception as e:
         print(f"\n--- [错误] TXT 摘要文件生成失败: {e} ---")


if __name__ == '__main__':
    # --- 使用说明 ---
    # 1. 确保已安装所需库: pip install akshare pandas python-dateutil openpyxl
    # 2. 直接运行此脚本即可
    
    macro_overview_data = get_macro_market_data()

    # 保存数据并生成摘要
    save_and_summarize_data(macro_overview_data)
    
    print("\n\n========================= 宏观数据报告预览 (仅显示头部数据) =========================")
    for name, df in macro_overview_data.items():
        print(f"\n--------- {name} ---------")
        if df is not None and not df.empty:
            print(df.head())
        else:
            print("未能获取到数据或数据为空。")
    print("\n==================================================================================")
