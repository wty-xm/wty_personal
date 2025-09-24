# -*- coding: utf-8 -*-
# @Author: TY (Your Investment Advisor)
# @Date: 2025-09-24
# @Version: 2.0
# @Description: A script to fetch risk and special event data for a specific stock,
#               acting as a "mine detector" for our investment decisions. This version
#               adds a check to handle None responses from data sources gracefully.

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


def get_latest_trade_date() -> str:
    """智能获取最近的交易日"""
    try:
        trade_date_df = ak.tool_trade_date_hist_sina()
        return trade_date_df['trade_date'].iloc[-1].strftime('%Y%m%d')
    except Exception:
        return (datetime.now() - pd.Timedelta(days=1)).strftime('%Y%m%d')

def get_risk_event_data(stock_code: str):
    """
    获取指定股票代码的风险排查与特殊事件数据。

    :param stock_code: 6位A股股票代码，例如 '600519'
    :return: 一个字典，键为数据名称，值为对应的 Pandas DataFrame
    """
    print(f"--- 开始获取股票 {stock_code} 的风险排查与特殊事件数据 ---")
    risk_data = {}

    # --- 1. 股权质押 ---
    print("\n[1/5] 正在获取股权质押数据...")
    try:
        latest_trade_date = get_latest_trade_date()
        df_all = ak.stock_gpzy_pledge_ratio_em(date=latest_trade_date)
        
        # 修正：增加对接口返回 None 值的判断
        if df_all is not None:
            df_stock = df_all[df_all['股票代码'] == stock_code]
            if not df_stock.empty:
                risk_data['上市公司质押比例'] = df_stock
                print(f"  - 成功获取 [上市公司质押比例] (日期: {latest_trade_date})")
            else:
                print(f"  - [信息] 在 {latest_trade_date} 未查询到该股票的质押信息。")
        else:
            print(f"  - [信息] 在 {latest_trade_date} 未获取到任何股权质押数据，可能是节假日或数据源暂未更新。")
            
    except Exception as e:
        print(f"  - [警告] 获取 [上市公司质押比例] 失败: {e}")

    # --- 2. 风险警示与退市 ---
    print("\n[2/5] 正在检查风险警示状态...")
    try:
        df_st = ak.stock_zh_a_st_em()
        if stock_code in df_st['代码'].values:
            risk_data['风险警示'] = df_st[df_st['代码'] == stock_code]
            print(f"  - [注意] 该股票在风险警示板中！")
        else:
            print(f"  - [信息] 该股票不在风险警示板中。")
    except Exception as e:
        print(f"  - [警告] 获取 [风险警示] 数据失败: {e}")

    # --- 3. 限售解禁 ---
    print("\n[3/5] 正在获取限售解禁数据...")
    try:
        df = ak.stock_restricted_release_queue_em(symbol=stock_code)
        if not df.empty:
            risk_data['限售解禁'] = df
            print("  - 成功获取 [限售解禁] 时间表")
        else:
            print(f"  - [信息] 未查询到该股票的限售解禁安排。")
    except Exception as e:
        print(f"  - [警告] 获取 [限售解禁] 失败: {e}")

    # --- 4. 高管与股东交易 ---
    print("\n[4/5] 正在获取高管与股东交易数据...")
    print("  - [提示] 正在下载全市场数据进行匹配，此过程可能需要1-2分钟，请稍候...")
    try:
        df_all_trades = ak.stock_ggcg_em(symbol="全部")
        df_stock_trades = df_all_trades[df_all_trades['代码'] == stock_code]
        if not df_stock_trades.empty:
            risk_data['高管股东交易'] = df_stock_trades
            print(f"  - 成功获取 [高管股东交易] 数据，共 {len(df_stock_trades)} 条记录")
        else:
            print(f"  - [信息] 未查询到该股票的高管股东交易记录。")
    except Exception as e:
        print(f"  - [警告] 获取 [高管股东交易] 失败: {e}")

    # --- 5. 公司公告 ---
    print("\n[5/5] 正在获取近期公司公告...")
    try:
        start_date_announce = (datetime.now() - pd.Timedelta(days=90)).strftime('%Y%m%d')
        end_date_announce = datetime.now().strftime('%Y%m%d')
        df = ak.stock_zh_a_disclosure_report_cninfo(symbol=stock_code, market="沪深京", start_date=start_date_announce, end_date=end_date_announce)
        if not df.empty:
            risk_data['近期公司公告'] = df
            print("  - 成功获取 [近期公司公告]")
        else:
            print(f"  - [信息] 近90天未查询到公司公告。")
    except Exception as e:
        print(f"  - [警告] 获取 [近期公司公告] 失败: {e}")

    print(f"\n--- 股票 {stock_code} 风险排查数据获取完成 ---")
    return risk_data

def save_data_to_excel(stock_code: str, data_dict: dict):
    """
    将获取到的数据保存到特定文件夹的 Excel 文件中。
    """
    today_str = datetime.now().strftime('%Y-%m-%d')
    folder_name = "risk_event_reports"
    os.makedirs(folder_name, exist_ok=True)
    
    file_name = f"risk_report_{stock_code}_{today_str}.xlsx"
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
    将关键的最新风险数据提取并保存为 TXT 摘要文件。
    """
    today_str = datetime.now().strftime('%Y-%m-%d')
    folder_name = "risk_event_reports"
    os.makedirs(folder_name, exist_ok=True)
    
    file_name = f"risk_summary_{stock_code}_{today_str}.txt"
    file_path = os.path.join(folder_name, file_name)

    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"股票代码: {stock_code} - 风险与事件摘要\n")
            f.write(f"报告生成日期: {today_str}\n")
            f.write("==================================================\n\n")

            # 风险警示
            if '风险警示' in data_dict and not data_dict['风险警示'].empty:
                f.write("--------- !!! 风险警示 !!! ---------\n")
                f.write("该股票在风险警示板中，请高度注意风险！\n")
                f.write(data_dict['风险警示'].to_string(index=False))
                f.write("\n\n")

            # 股权质押
            if '上市公司质押比例' in data_dict and not data_dict['上市公司质押比例'].empty:
                f.write("--------- 最新股权质押情况 ---------\n")
                f.write(data_dict['上市公司质押比例'].to_string(index=False))
                f.write("\n\n")

            # 限售解禁
            if '限售解禁' in data_dict and not data_dict['限售解禁'].empty:
                f.write("--------- 未来限售解禁安排 ---------\n")
                f.write(data_dict['限售解禁'].to_string(index=False))
                f.write("\n\n")

            # 高管股东交易
            if '高管股东交易' in data_dict and not data_dict['高管股东交易'].empty:
                f.write("--------- 近期高管股东交易 (最多显示10条) ---------\n")
                f.write(data_dict['高管股东交易'].head(10).to_string(index=False))
                f.write("\n\n")

            # 公司公告
            if '近期公司公告' in data_dict and not data_dict['近期公司公告'].empty:
                f.write("--------- 近期公司公告 (最多显示10条) ---------\n")
                f.write(data_dict['近期公司公告'][['公告标题', '公告时间']].head(10).to_string(index=False))
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
    # target_stock_code = '300315'  # 示例：掌趣科技 (一个有较多交易和质押的例子)
    
    risk_event_analysis_data = get_risk_event_data(target_stock_code)
    save_data_to_excel(target_stock_code, risk_event_analysis_data)
    save_summary_to_txt(target_stock_code, risk_event_analysis_data)
    
    print("\n\n========================= 数据预览 (仅显示头部) =========================")
    for name, df in risk_event_analysis_data.items():
        print(f"\n--------- {name} ---------")
        if df is not None and not df.empty:
            print(df.head())
        else:
            print("未能获取到数据或数据为空。")
    print("\n========================================================================")

