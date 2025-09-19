import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
import os

# --- 1. 配置区 (Configuration Area) ---
# 请在这里输入你需要获取数据的港股代码列表 (注意: 港股代码是数字字符串, 如 '00700')
HK_STOCK_CODES = [
    # '00700',   # 腾讯控股
    # '09988',   # 阿里巴巴-SW
    # '03690',   # 美团-W
    # '01211',   # 比亚迪股份
    # '00941',   # 中国移动
    # '00005',   # 汇丰控股
    # '01024',   # 快手-W
    '01810',     # 小米科技
]

# --- 数据获取开关 ---
# True 表示获取, False 表示跳过
GET_SNAPSHOT_DATA = True # 是否获取实时快照
GET_DAILY_DATA = True    # 是否获取日线K线

# --- 日线数据配置 ---
# 复权类型: '' (不复权), 'qfq' (前复权), 'hfq' (后复权)
ADJUST_TYPE = 'qfq'

# --- 2. 主功能函数 ---
def get_and_save_hk_stock_data():
    """
    根据脚本中预设的港股代码列表和开关，获取实时快照和历史日线K线数据，并分类保存。
    """
    print("TY助手 港股版 V1.0：正在连接数据接口，获取多维度港股情报...")

    # --- 1. 检查预设的股票代码 ---
    if not HK_STOCK_CODES:
        print("未在脚本中设置任何有效的港股代码，脚本退出。")
        return

    print(f"\n准备处理 {len(HK_STOCK_CODES)} 个代码: {', '.join(HK_STOCK_CODES)}")

    # --- 2. 创建报告文件夹 ---
    timestamp_folder = datetime.now().strftime("hk_stock_report_%Y%m%d_%H%M%S")
    os.makedirs(timestamp_folder, exist_ok=True)
    print(f"所有报告将保存在文件夹: {timestamp_folder}/")

    try:
        # --- A. 获取所有代码的实时快照 (合并) ---
        if GET_SNAPSHOT_DATA:
            print("\n--- 正在获取盘面快照 (所有代码) ---")
            try:
                # 使用港股实时行情接口
                snapshot_df_raw = ak.stock_hk_spot_em()
                # 筛选我们关注的代码
                snapshot_df = snapshot_df_raw[snapshot_df_raw['代码'].isin(HK_STOCK_CODES)].copy()

                if not snapshot_df.empty:
                    # 港股快照核心字段
                    core_columns = [
                        '代码', '名称', '最新价', '涨跌额', '涨跌幅', '今开', '最高',
                        '最低', '昨收', '成交量', '成交额', '市盈率', '市净率', '总市值',
                        '振幅', '换手率'
                    ]
                    # 筛选出实际存在的列，避免因接口变动导致列名不存在而报错
                    existing_columns = [col for col in core_columns if col in snapshot_df.columns]
                    snapshot_df = snapshot_df[existing_columns]
                    print(f"成功获取 {len(snapshot_df)} 只港股的盘面快照。")

                    # 保存快照文件
                    snapshot_path = os.path.join(timestamp_folder, "snapshot_report_all_hk.txt")
                    with open(snapshot_path, 'w', encoding='utf-8') as f:
                        f.write(f"--- 港股盘面实时快照 ---\n")
                        f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                        f.write(snapshot_df.to_string(index=False))
                    print(f"[快照报告] 已保存为: {snapshot_path}")
                else:
                    print("未能获取到任何指定港股的盘面快照。")
            except Exception as e:
                print(f"获取港股盘面快照时发生错误: {e}")

        # --- B. 循环获取每只股票的日线数据 (独立保存) ---
        if GET_DAILY_DATA:
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d") # 获取近1年数据

            for code in HK_STOCK_CODES:
                print(f"\n--- 正在处理港股: {code} ---")
                
                # 获取日线K线
                try:
                    daily_df = ak.stock_hk_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date, adjust=ADJUST_TYPE)
                    if not daily_df.empty:
                        daily_df['代码'] = code
                        # 根据接口文档，成交量和成交额单位已经是 股 和 港元，无需转换
                        print(f"成功获取 {code} 的日线数据 ({len(daily_df)} 条)。")
                        daily_path = os.path.join(timestamp_folder, f"hk_daily_data_{code}.csv")
                        daily_df.to_csv(daily_path, index=False, encoding='utf-8-sig')
                        print(f"[日线数据] 已保存为: {daily_path}")
                    else:
                        print(f"获取到 {code} 的日线数据为空。")
                except Exception as e:
                    print(f"获取 {code} 日线数据失败: {e}")
                
                time.sleep(1) # 增加延时，防止访问过于频繁

        print("\n--- 所有任务执行完毕 ---")

    except Exception as e:
        print(f"\n脚本运行发生严重错误！")
        print(f"错误信息: {e}")

# --- 3. 运行脚本 ---
if __name__ == "__main__":
    # 设置Pandas显示格式，确保数据对齐
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    pd.set_option('display.width', 180)
    
    get_and_save_hk_stock_data()