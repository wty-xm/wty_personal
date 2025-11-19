import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
import os

# --- 1. 配置区 (Configuration Area) ---
# 请在这里输入你需要获取数据的指数代码列表
INDEX_CODES = [
    'sh000001',   # 上证指数
    'sz399001',   # 深证成指
    'sz399006',   # 创业板指
]

# --- 数据获取开关 ---
# True 表示获取, False 表示跳过
GET_MINUTE_DATA = False  # 是否获取分钟K线 (已改为默认开启以演示新功能)
GET_DAILY_DATA = True   # 是否获取日线K线

# 分钟K线周期: 可选 '1', '5', '15', '30', '60'
MINUTE_PERIOD = '1'

# --- 重试/容错配置 ---
SNAPSHOT_MAX_ATTEMPTS = 3          # 快照接口最大重试次数
SNAPSHOT_RETRY_DELAY_SECONDS = 3   # 快照接口重试间隔(秒)
DATA_MAX_ATTEMPTS = 3              # 分钟/日线数据重试次数
DATA_RETRY_DELAY_SECONDS = 2       # 分钟/日线数据重试间隔(秒)


def fetch_with_retry(fetcher, label, max_attempts=DATA_MAX_ATTEMPTS, delay=DATA_RETRY_DELAY_SECONDS):
    """通用重试逻辑，返回DataFrame或空表，不抛异常。"""
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            df = fetcher()
            if df is not None and not df.empty:
                if attempt > 1:
                    print(f"{label} 第 {attempt} 次尝试成功。")
                return df
            last_error = ValueError("接口返回空数据")
            print(f"{label} 第 {attempt} 次返回空数据。")
        except Exception as e:
            last_error = e
            print(f"{label} 第 {attempt} 次尝试失败: {e}")

        if attempt < max_attempts:
            time.sleep(delay)

    if last_error:
        print(f"{label} 多次尝试仍失败，最后错误: {last_error}")
    else:
        print(f"{label} 多次尝试仍失败，接口一直返回空数据。")
    return pd.DataFrame()


def make_fetcher_if_exists(attr_name, *args, **kwargs):
    """存在则返回可调用fetcher，不存在返回None，避免版本兼容报错。"""
    func = getattr(ak, attr_name, None)
    if func is None:
        return None
    return lambda: func(*args, **kwargs)


# --- 2. 主功能函数 ---
def get_and_save_index_data():
    """
    根据脚本中预设的指数代码列表和开关，获取快照、分钟K线和日线K线数据，并分类保存。
    V5.0 核心更新: 分钟K线获取逻辑优化为“今天从开盘到当前时间”。
    """
    print("TY助手 V5.0：正在连接数据接口，获取多维度指数情报...")

    # --- 1. 检查预设的指数代码 ---
    if not INDEX_CODES:
        print("未在脚本中设置任何有效的指数代码，脚本退出。")
        return

    print(f"\n准备处理 {len(INDEX_CODES)} 个指数: {', '.join(INDEX_CODES)}")

    # --- 2. 创建报告文件夹 ---
    timestamp_folder = datetime.now().strftime("index_report_%Y%m%d_%H%M%S")
    os.makedirs(timestamp_folder, exist_ok=True)
    print(f"所有报告将保存在文件夹: {timestamp_folder}/")

    try:
        # --- A. 获取所有指数的实时快照 (合并) ---
        print("\n--- 正在获取盘面快照 (所有指数) ---")
        try:
            snapshot_df_raw = pd.DataFrame()
            snapshot_source_used = None
            last_snapshot_error = None

            snapshot_sources = []
            # 东财接口（常规）
            fetcher_em = make_fetcher_if_exists("stock_zh_index_spot_em", symbol="沪深重要指数")
            if fetcher_em:
                snapshot_sources.append(("东方财富", fetcher_em))
            # 新浪接口（部分旧版本或备用）
            fetcher_sina = make_fetcher_if_exists("stock_zh_index_spot")
            if fetcher_sina:
                snapshot_sources.append(("新浪", fetcher_sina))
            # 备用：无symbol版本
            fetcher_em_no_param = make_fetcher_if_exists("stock_zh_index_spot_em")
            if fetcher_em_no_param and fetcher_em_no_param is not fetcher_em:
                snapshot_sources.append(("东方财富(默认参数)", fetcher_em_no_param))

            if not snapshot_sources:
                raise RuntimeError("当前 akshare 版本未提供指数快照接口（stock_zh_index_spot_em / stock_zh_index_spot）。")

            for idx, (source_name, fetcher) in enumerate(snapshot_sources):
                if idx > 0:
                    print(f"{snapshot_sources[idx-1][0]}接口未成功，尝试使用备用的{source_name}接口...")

                snapshot_df_raw = fetch_with_retry(
                    fetcher,
                    f"{source_name}指数快照",
                    max_attempts=SNAPSHOT_MAX_ATTEMPTS,
                    delay=SNAPSHOT_RETRY_DELAY_SECONDS,
                )

                if snapshot_df_raw is not None and not snapshot_df_raw.empty:
                    snapshot_source_used = source_name
                    break
                last_snapshot_error = f"{source_name}接口在重试后仍失败"

            codes_for_filter = [code[2:] for code in INDEX_CODES]
            if snapshot_df_raw is not None and not snapshot_df_raw.empty and '代码' in snapshot_df_raw.columns:
                snapshot_working_df = snapshot_df_raw.copy()
                snapshot_working_df['_代码无前缀'] = snapshot_working_df['代码'].astype(str).str[-6:]
                snapshot_df = snapshot_working_df[snapshot_working_df['_代码无前缀'].isin(codes_for_filter)].copy()
                snapshot_df.drop(columns=['_代码无前缀'], inplace=True, errors='ignore')
            else:
                snapshot_df = pd.DataFrame()

            if not snapshot_df.empty:
                core_columns = ['代码', '名称', '最新价', '涨跌额', '涨跌幅', '成交量', '成交额', '振幅', '最高', '最低', '今开', '昨收', '量比']
                existing_columns = [col for col in core_columns if col in snapshot_df.columns]
                snapshot_df = snapshot_df[existing_columns]
                source_tip = f"，数据来源：{snapshot_source_used}" if snapshot_source_used else ""
                print(f"成功获取 {len(snapshot_df)} 个指数的盘面快照{source_tip}。")

                # 保存快照文件
                snapshot_path = os.path.join(timestamp_folder, "snapshot_report_all.txt")
                with open(snapshot_path, 'w', encoding='utf-8') as f:
                    f.write(f"--- 指数盘面实时快照 ---\n")
                    f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                    f.write(snapshot_df.to_string(index=False))
                print(f"[快照报告] 已保存为: {snapshot_path}")
            else:
                if last_snapshot_error:
                    print(f"未能获取到任何指定指数的盘面快照，最后错误: {last_snapshot_error}")
                else:
                    print("未能获取到任何指定指数的盘面快照。")
        except Exception as e:
            print(f"获取盘面快照时发生错误: {e}")

        # --- B & C. 循环获取每只指数的分钟和日线数据 (独立保存) ---
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")

        for code in INDEX_CODES:
            print(f"\n--- 正在处理指数: {code} ---")
            
            code_for_ak = code[2:]

            # --- 获取分钟K线 (根据开关) ---
            if GET_MINUTE_DATA:
                minute_df = fetch_with_retry(
                    lambda: ak.index_zh_a_hist_min_em(symbol=code_for_ak, period=MINUTE_PERIOD),
                    f"{code} 分钟K线",
                )
                
                if not minute_df.empty:
                    # --- V5.0 核心优化：筛选从今天开盘到当前时间的数据 ---
                    try:
                        # 1. 将'时间'列转换为datetime对象，以便于比较
                        minute_df['时间'] = pd.to_datetime(minute_df['时间'])
                        
                        # 2. 定义今天的开盘时间和当前时间
                        now = datetime.now()
                        market_open_time = now.replace(hour=9, minute=30, second=0, microsecond=0)
                        
                        # 3. 执行筛选
                        #    筛选条件：时间戳必须大于等于今天的开盘时间，并小于等于当前时间
                        filtered_df = minute_df[(minute_df['时间'] >= market_open_time) & (minute_df['时间'] <= now)].copy()
                        
                        if not filtered_df.empty:
                            print(f"已筛选出从 {market_open_time.strftime('%Y-%m-%d %H:%M:%S')} 到当前时间的 {len(filtered_df)} 条分钟数据。")
                            minute_path = os.path.join(timestamp_folder, f"minute_data_today_{code}.csv")
                            filtered_df.to_csv(minute_path, index=False, encoding='utf-8-sig')
                            print(f"[分钟数据] 已保存为: {minute_path}")
                        else:
                            # 如果筛选后为空，说明当前时间可能在开盘前
                            if now < market_open_time:
                                print(f"当前时间 {now.strftime('%H:%M:%S')} 早于开盘时间 09:30，不生成分钟数据文件。")
                            else:
                                print(f"在 {market_open_time.strftime('%H:%M:%S')} 到 {now.strftime('%H:%M:%S')} 之间未找到数据，可能为非交易日或刚开盘。")

                    except Exception as e:
                        print(f"处理和筛选分钟数据时出错: {e}")

                else:
                    print(f"最终未能获取到 {code} 的分钟K线数据。")

            # --- 获取日线K线 (根据开关) ---
            if GET_DAILY_DATA:
                daily_fetchers = []
                fetcher_hist = make_fetcher_if_exists(
                    "index_zh_a_hist", symbol=code_for_ak, period="daily", start_date=start_date, end_date=end_date
                )
                if fetcher_hist:
                    daily_fetchers.append(("东财 index_zh_a_hist", fetcher_hist))

                fetcher_daily_em = make_fetcher_if_exists(
                    "stock_zh_index_daily_em", symbol=code_for_ak, start_date=start_date, end_date=end_date
                )
                if fetcher_daily_em:
                    daily_fetchers.append(("东财 stock_zh_index_daily_em", fetcher_daily_em))

                fetcher_daily = make_fetcher_if_exists(
                    "stock_zh_index_daily", symbol=code_for_ak
                )
                if fetcher_daily:
                    daily_fetchers.append(("新浪 stock_zh_index_daily", fetcher_daily))

                daily_df = pd.DataFrame()
                last_daily_error = None
                for idx, (source_name, fetcher) in enumerate(daily_fetchers):
                    daily_df = fetch_with_retry(
                        fetcher,
                        f"{code} 日线数据（{source_name}）",
                    )
                    if not daily_df.empty:
                        print(f"{code} 日线数据来自 {source_name}。")
                        break
                    last_daily_error = f"{source_name} 重试后仍失败"

                if not daily_df.empty:
                    daily_df['代码'] = code_for_ak
                    print(f"成功获取 {code} 的日线数据。")
                    daily_path = os.path.join(timestamp_folder, f"daily_data_{code}.csv")
                    daily_df.to_csv(daily_path, index=False, encoding='utf-8-sig')
                    print(f"[日线数据] 已保存为: {daily_path}")
                else:
                    if last_daily_error:
                        print(f"获取到 {code} 的日线数据为空，最后错误: {last_daily_error}")
                    else:
                        print(f"获取到 {code} 的日线数据为空。")

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
    
    get_and_save_index_data()
