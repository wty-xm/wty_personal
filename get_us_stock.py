import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
# V1.3 修复: 导入 datetime.time 并重命名为 dt_time, 避免与 time 模块冲突
from datetime import time as dt_time
import time
import os

# --- 1. 配置区 (Configuration Area) ---
# 请在这里输入你需要获取数据的美股代码列表 (科技七巨头)
US_STOCK_SYMBOLS = [
    'AAPL',  # 苹果
    'MSFT',  # 微软
    'GOOGL', # 谷歌
    'AMZN',  # 亚马逊
    'NVDA',  # 英伟达
    'META',  # Meta Platforms
    'TSLA',  # 特斯拉
]

# --- 数据获取开关 ---
# True 表示获取, False 表示跳过
GET_DAILY_DATA = True   # 是否获取日线K线

# --- 2. 辅助功能函数 ---
def is_us_market_open():
    """
    检查当前北京时间是否在美国股市的常规交易时间内。
    注意：此函数未精确处理夏令时，使用一个较宽泛的时间窗口(21:30-05:00)以覆盖。
    """
    try:
        now_cst = datetime.now() # 假设脚本运行环境为北京时间 (UTC+8)
        weekday = now_cst.weekday() # Monday is 0, Sunday is 6
        current_time = now_cst.time()
        
        # 周日全天休市
        if weekday == 6:
            return False
        # V1.3 修复: 使用重命名后的 dt_time
        # 周六, 在凌晨5点收盘后, 全天休市
        if weekday == 5 and current_time >= dt_time(5, 0):
            return False
        # 周一, 在晚上9点半开盘前, 处于休市状态
        if weekday == 0 and current_time < dt_time(21, 30):
            return False

        # V1.3 修复: 使用重命名后的 dt_time
        # 如果时间在交易时段内，则认为是开市
        if current_time >= dt_time(21, 30) or current_time < dt_time(5, 0):
            return True

        return False
    except Exception as e:
        print(f"检查美股交易时间出错: {e}, 默认尝试获取数据。")
        return True

# --- 3. 主功能函数 ---
def get_and_save_us_stock_data():
    """
    根据脚本中预设的美股代码列表和开关，获取快照和日线K线数据，并分类保存。
    V1.3 更新: 更换日线接口为 ak.stock_us_daily (新浪), 并修复 time 模块冲突。
    """
    print("TY助手 美股版V1.3：正在连接数据接口，获取美股情报...")

    # --- 1. 检查预设的股票代码 ---
    if not US_STOCK_SYMBOLS:
        print("未在脚本中设置任何有效的美股代码，脚本退出。")
        return

    print(f"\n准备处理 {len(US_STOCK_SYMBOLS)} 个代码: {', '.join(US_STOCK_SYMBOLS)}")

    # --- 2. 创建报告文件夹 ---
    timestamp_folder = datetime.now().strftime("us_stock_report_%Y%m%d_%H%M%S")
    os.makedirs(timestamp_folder, exist_ok=True)
    print(f"所有报告将保存在文件夹: {timestamp_folder}/")

    try:
        # --- A. 获取所有代码的实时快照 (合并) ---
        print("\n--- 正在获取盘面快照 (所有代码) ---")
        if not is_us_market_open():
            print("当前非美股交易时间，已跳过获取实时盘面快照。")
        else:
            try:
                snapshot_df_raw = ak.stock_us_spot_em()
                if snapshot_df_raw is not None:
                    snapshot_df = snapshot_df_raw[snapshot_df_raw['代码'].isin(US_STOCK_SYMBOLS)].copy()
                    
                    if not snapshot_df.empty:
                        core_columns = [
                            '代码', '名称', '最新价', '涨跌额', '涨跌幅', '开盘价', '最高价', 
                            '最低价', '昨收价', '总市值', '市盈率', '成交量', '成交额', 
                            '振幅', '换手率'
                        ]
                        existing_columns = [col for col in core_columns if col in snapshot_df.columns]
                        snapshot_df = snapshot_df[existing_columns]
                        print(f"成功获取 {len(snapshot_df)} 只美股的盘面快照。")

                        snapshot_path = os.path.join(timestamp_folder, "snapshot_report_all.txt")
                        with open(snapshot_path, 'w', encoding='utf-8') as f:
                            f.write(f"--- 美股盘面实时快照 ---\n")
                            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                            f.write(snapshot_df.to_string(index=False))
                        print(f"[快照报告] 已保存为: {snapshot_path}")
                    else:
                        print("未能获取到任何指定美股的盘面快照。")
                else:
                     print("未能获取到任何盘面快照数据(接口返回None)。")
            except Exception as e:
                print(f"获取盘面快照时发生错误: {e}")

        # --- B. 循环获取每只股票的日线数据 (独立保存) ---
        for symbol in US_STOCK_SYMBOLS:
            print(f"\n--- 正在处理: {symbol} ---")
            
            if GET_DAILY_DATA:
                daily_df = None
                for i in range(3):
                    try:
                        # V1.3 更新: 使用新浪财经接口
                        daily_df_raw = ak.stock_us_daily(symbol=symbol, adjust="qfq")
                        
                        if daily_df_raw is not None and not daily_df_raw.empty:
                            # 新接口返回全部历史数据, 我们需要手动筛选最近一年
                            daily_df_raw['date'] = pd.to_datetime(daily_df_raw['date'])
                            start_date = datetime.now() - timedelta(days=365)
                            daily_df = daily_df_raw[daily_df_raw['date'] >= start_date].copy()
                            if not daily_df.empty:
                                break 
                    except Exception as e:
                        print(f"第 {i+1} 次尝试获取 {symbol} 日线数据时发生异常: {e}")
                    
                    if i < 2:
                         print(f"第 {i+1} 次尝试失败，2秒后重试...")
                         # V1.3 修复: 这里的 time.sleep 现在可以正确工作
                         time.sleep(2)

                if daily_df is not None and not daily_df.empty:
                    print(f"成功获取 {symbol} 的日线数据。")
                    daily_df['代码'] = symbol
                    daily_path = os.path.join(timestamp_folder, f"daily_data_{symbol}.csv")
                    # 保存时将 date 列格式化为字符串，避免时区问题
                    daily_df['date'] = daily_df['date'].dt.strftime('%Y-%m-%d')
                    daily_df.to_csv(daily_path, index=False, encoding='utf-8-sig')
                    print(f"[日线数据] 已保存为: {daily_path}")
                else:
                    print(f"最终未能获取到 {symbol} 的日线数据。")

        print("\n--- 所有任务执行完毕 ---")

    except Exception as e:
        print(f"\n脚本运行发生严重错误！")
        print(f"错误信息: {e}")

# --- 4. 运行脚本 ---
if __name__ == "__main__":
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)
    pd.set_option('display.width', 180)
    
    get_and_save_us_stock_data()

