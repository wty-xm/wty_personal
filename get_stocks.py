import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import re

# --- 1. 配置区 (Configuration Area) ---
# 请在这里输入你需要获取数据的股票代码列表
STOCK_CODES = [
    'sh603019',   # 中科曙光
    'sz000032',   # 深桑达A
    'sh603279',   # 景津装备
    'sh600395',   # 盘江股份
    'sh601918',   # 新集能源
    'sz002415',   # 海康威视

    # 关注股票
    'sh601138',       # 工业富联
    'sz300308',      # 中际旭创
    'sz002230',      # 科大讯飞
    'sz002463',      # 沪电股份
    'sh600938',     # 中海油
    'sh600519',     # 贵州茅台
    'sz000977',     # 浪潮信息
    'sz300274',     # 阳光电源
    'sh600777',     # 新潮能源

    # 已清仓股票
    # 'sh603398',     # st沐邦 
    # 'sz002807',     # 江阴银行
    # 'sh601689',     # 拓普集团
]

# --- 数据获取开关 ---
# True 表示获取, False 表示跳过
GET_MINUTE_DATA = True   # 是否获取分钟K线 (已改为默认开启以演示新功能)
GET_DAILY_DATA = True   # 是否获取日线K线

# GET_MINUTE_DATA = False   # 是否获取分钟K线 (已改为默认开启以演示新功能)
# GET_DAILY_DATA = False   # 是否获取日线K线

# 分钟K线周期: 可选 '1', '5', '15', '30', '60'
MINUTE_PERIOD = '1'

# --- 快照获取策略 ---
SNAPSHOT_MAX_ATTEMPTS = 3        # 每个接口的最大重试次数
SNAPSHOT_RETRY_DELAY_SECONDS = 3 # 接口调用失败后的等待时间(秒)

# --- 实用工具函数 ---
def sanitize_filename_component(value: str) -> str:
    """将值转换为适合文件名的安全片段。"""
    if not value:
        return ''
    value = str(value).strip()
    value = re.sub(r"[\\/:*?\"<>|]", "_", value)
    value = re.sub(r"\s+", "_", value)
    return value.strip('_')


def fetch_stock_name_from_info(code_without_prefix: str) -> str:
    """优先从个股信息接口获取股票简称，失败则返回空字符串。"""
    try:
        info_df = ak.stock_individual_info_em(symbol=code_without_prefix)
        if info_df is not None and not info_df.empty:
            candidates = info_df[info_df['item'].isin(['证券简称', '股票简称', '公司简称', '公司名称', '股票名称'])]
            if not candidates.empty:
                return str(candidates['value'].iloc[0]).strip()
    except Exception:
        pass
    return ''


def get_stock_name(full_code: str, cache: dict, snapshot_lookup: dict) -> str:
    """获取股票名称并缓存，同时优先利用已有快照信息。"""
    if full_code in cache:
        return cache[full_code]

    code_without_prefix = full_code[2:]

    if snapshot_lookup and code_without_prefix in snapshot_lookup:
        name = str(snapshot_lookup[code_without_prefix]).strip()
        if name:
            cache[full_code] = name
            return name

    name = fetch_stock_name_from_info(code_without_prefix)
    if not name:
        name = code_without_prefix

    cache[full_code] = name
    return name


# --- 2. 主功能函数 ---
def get_and_save_stock_data():
    """
    根据脚本中预设的股票代码列表和开关，获取快照、分钟K线和日线K线数据，并分类保存。
    V6.0 核心更新: 分钟K线获取逻辑优化为“今天从开盘到当前时间”。
    """
    print("TY助手 V6.0：正在连接数据接口，获取多维度股票情报...")

    # --- 1. 检查预设的股票代码 ---
    if not STOCK_CODES:
        print("未在脚本中设置任何有效的股票代码，脚本退出。")
        return

    print(f"\n准备处理 {len(STOCK_CODES)} 个代码: {', '.join(STOCK_CODES)}")

    # --- 2. 创建报告文件夹 ---
    timestamp_folder = datetime.now().strftime("stock_report_%Y%m%d_%H%M%S")
    os.makedirs(timestamp_folder, exist_ok=True)
    print(f"所有报告将保存在文件夹: {timestamp_folder}/")

    code_name_cache = {}
    snapshot_lookup = {}

    try:
        # --- A. 获取所有代码的实时快照 (合并) ---
        print("\n--- 正在获取盘面快照 (所有代码) ---")
        try:
            snapshot_df_raw = pd.DataFrame()
            snapshot_source_used = None
            last_snapshot_error = None

            snapshot_sources = [
                ("东方财富", ak.stock_zh_a_spot_em),
                ("新浪", ak.stock_zh_a_spot),
            ]

            for idx, (source_name, fetcher) in enumerate(snapshot_sources):
                if idx > 0:
                    print(f"{snapshot_sources[idx-1][0]}接口未成功，尝试使用备用的{source_name}接口...")

                success = False
                for attempt in range(1, SNAPSHOT_MAX_ATTEMPTS + 1):
                    try:
                        candidate_df = fetcher()
                        if candidate_df is not None and not candidate_df.empty:
                            snapshot_df_raw = candidate_df
                            snapshot_source_used = source_name
                            success = True
                            print(f"{source_name}接口获取盘面快照成功（第 {attempt} 次尝试）。")
                            break
                        else:
                            last_snapshot_error = ValueError("接口返回空数据")
                            print(f"{source_name}接口第 {attempt} 次尝试返回空数据。")
                    except Exception as e:
                        last_snapshot_error = e
                        print(f"{source_name}接口第 {attempt} 次尝试失败: {e}")

                    if attempt < SNAPSHOT_MAX_ATTEMPTS:
                        time.sleep(SNAPSHOT_RETRY_DELAY_SECONDS)

                if success:
                    break

            if snapshot_df_raw is None or snapshot_df_raw.empty:
                if last_snapshot_error:
                    print(f"获取盘面快照失败，最后的错误信息: {last_snapshot_error}")
                else:
                    print("获取盘面快照失败：未能从可用接口获取数据。")
                snapshot_df = pd.DataFrame()
            else:
                codes_without_prefix = {code[2:] for code in STOCK_CODES}
                codes_full_lower = {code.lower() for code in STOCK_CODES}

                if '代码' not in snapshot_df_raw.columns:
                    print("快照数据缺少'代码'列，无法筛选指定股票。")
                    snapshot_df = pd.DataFrame()
                else:
                    snapshot_working_df = snapshot_df_raw.copy()
                    snapshot_working_df['_代码lower'] = snapshot_working_df['代码'].astype(str).str.lower()
                    snapshot_working_df['_代码无前缀'] = snapshot_working_df['代码'].astype(str).str[-6:]

                    filter_mask = snapshot_working_df['_代码无前缀'].isin(codes_without_prefix) | snapshot_working_df['_代码lower'].isin(codes_full_lower)
                    snapshot_df = snapshot_working_df[filter_mask].copy()
                    snapshot_df.drop(columns=['_代码lower', '_代码无前缀'], inplace=True, errors='ignore')

                    if '名称' in snapshot_df_raw.columns:
                        lookup_df = snapshot_df_raw.dropna(subset=['名称']).copy()
                        lookup_df['_代码无前缀'] = lookup_df['代码'].astype(str).str[-6:]
                        snapshot_lookup = lookup_df.set_index('_代码无前缀')['名称'].astype(str).to_dict()
                        for code in STOCK_CODES:
                            code_without_prefix = code[2:]
                            name_candidate = snapshot_lookup.get(code_without_prefix)
                            if name_candidate:
                                code_name_cache[code] = name_candidate.strip()

                    if not snapshot_df.empty:
                        # V5.0 更新: 增加更多快照字段
                        core_columns = [
                            '代码', '名称', '最新价', '涨跌额', '涨跌幅', '成交量', '成交额',
                            '振幅', '最高', '最低', '今开', '昨收', '量比', '换手率',
                            '市盈率-动态', '市净率', '总市值', '流通市值', '涨速',
                            '5分钟涨跌', '60日涨跌幅', '年初至今涨跌幅'
                        ]
                        # 筛选出实际存在的列，避免因接口变动导致列名不存在而报错
                        existing_columns = [col for col in core_columns if col in snapshot_df.columns]
                        snapshot_df = snapshot_df[existing_columns]
                        print(f"成功获取 {len(snapshot_df)} 只股票的盘面快照，数据来源：{snapshot_source_used}。")

                        # 保存快照文件
                        snapshot_path = os.path.join(timestamp_folder, "snapshot_report_all.txt")
                        with open(snapshot_path, 'w', encoding='utf-8') as f:
                            f.write(f"--- 股票盘面实时快照 ---\n")
                            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                            f.write(snapshot_df.to_string(index=False))
                        print(f"[快照报告] 已保存为: {snapshot_path}")
                    else:
                        print("未能获取到任何指定股票的盘面快照。")
        except Exception as e:
            print(f"获取盘面快照时发生错误: {e}")

        # --- B & C. 循环获取每只股票的分钟和日线数据 (独立保存) ---
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")

        for code in STOCK_CODES:
            stock_name = get_stock_name(code, code_name_cache, snapshot_lookup)
            name_token = sanitize_filename_component(stock_name)
            name_suffix = f"_{name_token}" if name_token else ""

            print(f"\n--- 正在处理股票: {code} ({stock_name}) ---")

            code_for_ak = code[2:]

            # --- 获取分钟K线 (根据开关) ---
            if GET_MINUTE_DATA:
                minute_df = pd.DataFrame()
                for i in range(3): # 最多重试3次
                    try:
                        # akshare默认获取最近一个交易日的完整分钟数据
                        minute_df = ak.stock_zh_a_hist_min_em(symbol=code_for_ak, period=MINUTE_PERIOD)
                        if not minute_df.empty:
                            print(f"成功获取 {code} 的分钟K线原始数据。")
                            break
                    except Exception as e:
                        print(f"第 {i+1} 次尝试获取 {code} 分钟K线失败: {e}")
                    if i < 2: time.sleep(2)
                
                if not minute_df.empty:
                    # --- V6.0 核心优化：筛选从今天开盘到当前时间的数据 ---
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
                            minute_path = os.path.join(timestamp_folder, f"minute_data_today_{code}{name_suffix}.csv")
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
                        # 如果筛选失败，可以选择保存原始数据作为备用
                        # minute_path = os.path.join(timestamp_folder, f"minute_data_raw_{code}.csv")
                        # minute_df.to_csv(minute_path, index=False, encoding='utf-8-sig')
                        # print(f"[原始分钟数据] 筛选失败，已将原始数据保存为: {minute_path}")

                else:
                    print(f"最终未能获取到 {code} 的分钟K线数据。")

            # --- 获取日线K线 (根据开关) ---
            if GET_DAILY_DATA:
                try:
                    daily_df = ak.stock_zh_a_hist(symbol=code_for_ak, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
                    if not daily_df.empty:
                        daily_df['代码'] = code_for_ak
                        daily_df['名称'] = stock_name
                        print(f"成功获取 {code} 的日线数据。")
                        daily_path = os.path.join(timestamp_folder, f"daily_data_{code}{name_suffix}.csv")
                        daily_df.to_csv(daily_path, index=False, encoding='utf-8-sig')
                        print(f"[日线数据] 已保存为: {daily_path}")
                    else:
                        print(f"获取到 {code} 的日线数据为空。")
                except Exception as e:
                    print(f"获取 {code} 日线数据失败: {e}")

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
    
    get_and_save_stock_data()
