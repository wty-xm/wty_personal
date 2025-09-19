# import akshare as ak
# import pandas as pd
# import datetime
# import time
# import random

# # 获取A股所有上市公司基本信息
# def get_stock_list():
#     stock_list = ak.stock_info_a_code_name()  # 获取A股所有上市公司代码和名称
#     return stock_list

# print("获取A股所有上市公司基本信息...")
# stock_list = get_stock_list()

# # 处理股票代码（移除后缀）
# codes = stock_list['code']
# names = stock_list['name']

# # 计算日期范围
# end_date = datetime.datetime.now().strftime('%Y%m%d')
# start_date = (datetime.datetime.now() - datetime.timedelta(days=4068)).strftime('%Y%m%d')

# # 收集各公司的流通市值数据
# circulating_market_caps = []
# failed_cnt = 0
# for idx, code in enumerate(codes):
#     success = False  # 用来控制是否成功获取数据
#     while not success:
#         try:
#             print(f"Processing: {code}, {idx+1}/{len(codes)}")
#             # 获取历史行情数据
#             stock_data = ak.stock_zh_a_hist(
#                 symbol=code, period='daily', 
#                 start_date=start_date, end_date=end_date, adjust=""
#             )
#             if not stock_data.empty:
#                 # 计算流通市值（成交额 / 换手率%）
#                 stock_data['流通市值'] = stock_data['成交额'] / (stock_data['换手率'] / 100)
#                 # 提取日期和流通市值
#                 series = stock_data.set_index('日期')['流通市值']
#                 circulating_market_caps.append(series)
#             else:
#                 circulating_market_caps.append(pd.Series(name=names[idx]))
#                 print(f"无数据: {code}")
#             success = True  # 如果成功获取数据，设置成功标志
#         except Exception as e:
#             print(f"Error retrieving {code}: {e}")
#             failed_cnt += 1
#             print(f"Failed count: {failed_cnt}")
#             time.sleep(random.uniform(1, 3))  # 如果出错，休眠1到3秒后重试
#             # 继续重试，直到成功获取数据

# # 合并数据并处理索引
# merged_df = pd.concat(circulating_market_caps, axis=1)
# merged_df.columns = names  # 设置列名为公司名称
# merged_df = merged_df.sort_index(ascending=False)  # 按日期倒序
# merged_df.reset_index(inplace=True)
# merged_df.rename(columns={'index': '日期'}, inplace=True)

# new_codes = codes.to_list()
# new_codes.insert(0, '日期')
# new_names = names.to_list()
# new_names.insert(0, '日期')

# codes_df = pd.DataFrame([new_codes], columns=new_names)
# # print(codes_df)
# merged_df = pd.concat([codes_df, merged_df], ignore_index=True)
# # print(merged_df)

# # 写入新Excel文件
# with pd.ExcelWriter('流通市值_companies_2014-2025.xlsx') as writer:
#     # 从第1行开始写入流通市值数据
#     merged_df.to_excel(writer, startrow=0, index=False, header=True, sheet_name='Sheet1')

# print(f"Failed count: {failed_cnt}")


import pandas as pd

# 读取原始Excel文件，跳过表头的行
df = pd.read_excel('流通市值_companies_2014-2025.xlsx', sheet_name='Sheet1', skiprows=1)

# 检查并查看数据结构，确保 '日期' 列存在并处理
print(df.head())

# 确保 '日期' 列是字符串类型，并去除无效数据（如果包含 '日期' 字符串）
df = df[~df['日期'].isin(['日期'])]

# 将日期列转换为 datetime 类型
df['日期'] = pd.to_datetime(df['日期'], errors='coerce')  # 使用 errors='coerce' 来处理无法解析的日期

# 检查日期转换后的结果
print(df.head())

# 将日期列设置为索引，方便按周计算
df.set_index('日期', inplace=True)

# 计算每周的平均流通市值
weekly_avg = df.resample('W').mean()  # 使用 resample('W') 来按周进行重采样，并计算每周的平均值

# 重新设置索引
weekly_avg.reset_index(inplace=True)

# 保存结果到新Excel文件
with pd.ExcelWriter('流通市值_companies_2014-2025_weekly.xlsx') as writer:
    weekly_avg.to_excel(writer, index=False, sheet_name='Weekly_Avg')

print("Weekly average market caps have been saved to 'weekly_avg_companies.xlsx'")

