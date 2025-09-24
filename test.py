import akshare as ak

stock_szse_summary_df = ak.stock_margin_detail_sse(date="20250921")
print(stock_szse_summary_df)