import akshare as ak
import pandas as pd
import datetime
import time
import random

# 读取原始Excel文件
original_df = pd.read_excel('close2.xlsx', header=None)

# 处理股票代码（移除后缀）
codes = original_df.iloc[2].apply(lambda x: x.split('.')[0]).tolist()

# 计算日期范围
end_date = datetime.datetime.now().strftime('%Y%m%d')
start_date = (datetime.datetime.now() - datetime.timedelta(days=4066)).strftime('%Y%m%d')

# 收集各公司的流通市值数据
circulating_market_caps = []
failed_cnt = 0
for idx, code in enumerate(codes):
    try:
        print(f"Processing: {code}, {idx+1}/{len(codes)}")
        # 获取历史行情数据
        stock_data = ak.stock_zh_a_hist(
            symbol=code, period='daily', 
            start_date=start_date, end_date=end_date, adjust=""
        )
        if not stock_data.empty:
            # 计算流通市值（成交额 / 换手率%）
            stock_data['流通市值'] = stock_data['成交额'] / (stock_data['换手率'] / 100)
            # 提取日期和流通市值
            series = stock_data.set_index('日期')['流通市值']
            circulating_market_caps.append(series)
        else:
            circulating_market_caps.append(pd.Series(name=original_df.iloc[1, idx]))
            print(f"无数据: {code}")
        # time.sleep(random.uniform(0.05, 0.15))    # 随机等待0.5到2.5秒
    except Exception as e:
        print(f"Error retrieving {code}: {e}")
        failed_cnt += 1
        print(f"Failed count: {failed_cnt}")
        circulating_market_caps.append(pd.Series(name=original_df.iloc[1, idx]))

# 合并数据并处理索引
merged_df = pd.concat(circulating_market_caps, axis=1)
merged_df.columns = original_df.iloc[1].tolist()  # 设置列名为公司名称
merged_df = merged_df.sort_index(ascending=False)  # 按日期倒序
merged_df.reset_index(inplace=True)
merged_df.rename(columns={'index': '日期'}, inplace=True)

# 写入新Excel文件
with pd.ExcelWriter('updated_companies2.xlsx') as writer:
    # 写入原始的三行信息
    original_df.iloc[:3].to_excel(writer, index=False, header=False, sheet_name='Sheet1')
    # 从第四行开始写入流通市值数据
    merged_df.to_excel(writer, startrow=3, index=False, header=False, sheet_name='Sheet1')

print(f"Failed count: {failed_cnt}")