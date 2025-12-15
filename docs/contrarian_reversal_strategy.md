# 跨资产反趋势策略脚本说明（`contrarian_reversal_strategy.py`）

## 用途与思路
- 基于“连续单边走势 + 反向持有固定周期”的跨资产反趋势回测脚本，支持周/月/季度等多频率数据。
- 默认只做多，支持通过参数开启做空；仓位随累计涨跌幅动态调整，并受单品种与组合总仓位上限约束。
- 输入为 Excel 多 sheet 价格数据，输出回测指标、权益曲线、交易日志以及资产/频段贡献。

## 快速运行
- 使用默认 Excel 与配置：`python contrarian_reversal_strategy.py`
- 指定数据文件：`python contrarian_reversal_strategy.py --excel 极限配置策略-数据.xlsx`
- 允许做空：`python contrarian_reversal_strategy.py --allow-short`
- 通过 JSON 覆盖配置：`python contrarian_reversal_strategy.py --config-json custom.json`

## 配置结构（`DEFAULT_CONFIG`）
- `excel_path`：Excel 数据路径；每个 sheet 代表一种频率。
- `frequencies`：按 sheet 配置 `freq_label`（重采样标签）、`up_streak`/`down_streak`（触发连续涨跌期数）、`holding_periods`（持有期数）、`min_amplitude`（累计涨跌幅阈值，绝对值过滤弱信号）。
- `trading.long_only`：默认 `True` 仅做多；设为 `False` 连涨做空、连跌做多。
- `asset_class_map`：标的到资产类别映射，未命中归类 `DEFAULT`。
- `position`：仓位参数（每类包含 `sensitivity`、`min_pos`、`max_pos`），并受 `portfolio.per_symbol_cap` 限制。
- `portfolio`：`gross_cap` 组合总绝对仓位上限；`round_trip_cost_bps` 双边手续费（bps）。
- `output`：各 CSV 输出文件名。
- 配置优先级：`DEFAULT_CONFIG` ← `--config-json` 文件 ← CLI 选项（`--excel`、`--allow-short`）。

## 数据与预处理
- `load_prices` 按 sheet 读取：从第 2 行起（`header=1`）定位列，检查 `日期` 列，转换时间索引、排序、前向填充缺失、清理全空列。
- 价格序列使用 `pct_change` 计算收益率；`calc_streaks` 逐行累计连续涨/跌期数，遇 0/缺失重置。

## 信号与交易生成（`generate_trades`）
1. 遍历每个标的，跳过样本太短的数据。
2. 根据 streak 判定信号：默认连跌达阈值→做多；若允许做空且连涨达阈值→做空。信号日后第 1 期进场，持有 `holding_periods` 个周期。
3. 过滤：缺失数据、持有期不足、累计涨跌幅绝对值低于 `min_amplitude`、计算出的仓位为 0 均跳过。
4. 仓位：`position_from_amplitude` 按资产类别用 `sensitivity * |amplitude|` 计算，受 `min_pos`/`max_pos` 及 `per_symbol_cap` 截断。
5. 交易记录：保存标的、资产类别、频率标签、信号/进出场时间、方向、连续期数、累计涨跌幅、原始权重、持有期收益等。

## 回测流程（`run_backtest`）
1. 汇总各频段的交易，按 `entry_time`、`symbol` 排序。
2. 组合约束：同一进场日按原始权重合计计算总绝对仓位，若超出 `gross_cap` 等比例缩放，记录缩放后权重与杠杆。
3. 成交成本：用 `round_trip_cost_bps` 换算为收益率扣减；方向乘持有期收益得到签名收益，乘缩放权重减去成本即 PnL。
4. 权益曲线：按 `exit_time` 聚合 PnL，做乘积累乘得到组合权益。
5. 指标：累计收益、年化收益（按起止天数）、年化波动与近似夏普（基于退出期数估算年化频次）、最大回撤、胜率、交易笔数。
6. 归因：按资产类别/标的与频率标签汇总 PnL。
7. 结果输出：`summary.csv`、`equity.csv`、`trades.csv`、`by_asset.csv`、`by_freq.csv`。

## 主要入口
- CLI 解析于 `parse_args`，`main` 组装配置后调用 `run_backtest`，写出所有 CSV，并在未指定 `--quiet` 时打印指标与主要归因。

## 常见扩展点
- 调整各频段触发阈值、持有期与振幅门槛以匹配不同市场节奏。
- 修改 `asset_class_map` 与 `position` 参数，针对品类的波动特征自定义仓位弹性与上下限。
- 扩展输出：可增加分年度/滚动窗口的风险指标，或生成图表供报告使用。

## 本次运行记录（2025-12-15 09:56:58）
- 运行命令：`python3 contrarian_reversal_strategy.py`
- 数据文件：`极限配置策略-数据.xlsx`（使用默认配置）
- 回测区间：1997-09-30 至 2025-10-17，笔数 151（全部做多）
- 关键参数快照：
  - 频段：周（连涨/跌≥7，持有3，振幅阈值5%）、月（≥8，持有3，振幅8%）、季（≥5，持有3，振幅10%）
  - 仓位：随累计振幅线性放大，类别上限 COMMO 35%、BOND 50%、EQUITY 40%、默认 30%，单标 60%，组合总仓 1.0；交易成本 5 bps
  - 交易方向：`long_only=True`
- 输出文件：`contrarian_summary.csv`、`contrarian_equity_curve.csv`、`contrarian_trades.csv`、`contrarian_by_asset.csv`、`contrarian_by_freq.csv`

### 仓位计算与示例
- 公式：`raw_weight = sensitivity * |累计涨跌幅|`，依次截在 `min_pos` 下限、`max_pos` 上限，并受单标 `per_symbol_cap=60%` 限制；同日如多标的并发，再按组合上限 `gross_cap=1.0` 等比例缩放得到 `scaled_weight`。
- 按类别参数：COMMO(3.0, 5%~35%)，BOND(6.0, 5%~50%)，EQUITY(4.0, 5%~40%)，DEFAULT(4.0, 0~30%)。
- 触发前提：该频段累计振幅需超过阈值（周≥5%，月≥8%，季≥10%）且连续期数达标；默认仅连跌做多（`long_only`）。
- 示例1（EQUITY，连跌累计 -10%，信号来自季频/周频均可）：原始 4×10%=40%，落在 5%~40% 内且低于单标 60%，得到 40%；若同日多标的总仓 >100%，则整体按比例缩放。
- 示例2（COMMO，连跌累计 -3%）：若振幅低于频段阈值（周/月/季），则不进场；若某频段阈值设为 2%，原始 3×3%=9%，截在 5%~35% 内且低于单标 60%，得到 9%，再看同日是否被缩放。

### 结果摘要
- 累计收益 9.88（权益末值 10.88），年化收益 9.05%，近似夏普 1.12，年化波动 0.33，最大回撤 -27.5%，胜率 73.5%（151 笔）。
- 频段贡献：季频 +1.96，周频 +0.70，月频 +0.09（季频占主导）。
- 资产贡献（前五）：恒生综合行业指数-资讯科技业 +0.325，恒生指数 +0.310，恒生地产分类指数 +0.258，中信风格指数:成长 +0.228，恒生医疗保健指数 +0.175。
- 资产贡献（尾部）：CBOT 大豆期货 -0.006，美国10年国债 +0.005，煤炭 +0.006，LME 铜 +0.010，伦敦银 +0.013（影响轻微）。

### 结果解读与可能原因
- 季度频率贡献最高：更长的连续单边段触发了较大的仓位，同时持有期 3 季度让反转收益充分兑现，驱动整体盈利。
- 港股相关指数贡献集中：HK 市场波动较大，容易出现连续单边后反转；配置灵敏度（EQUITY 4.0x 振幅）叠加 40% 上限带来较高权重。
- 月频近乎持平：触发条件（8 连涨/跌，8% 振幅）较苛刻，信号稀疏且样本不足，难以提供稳定超额。
- 回撤 27.5%：集中在少数高权重标的的反转期；长期样本下累计收益高，但年化收益仅 9%，说明盈利主要来自若干阶段性行情而非持续性。
- 全部做多：`long_only=True` 避免了连涨做空的回撤，但也错过了可能的下跌对冲；如需平滑波动，可尝试允许做空并调低上限。
