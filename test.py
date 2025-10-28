# -*- coding: utf-8 -*-
"""
小米(01810.HK) 日K 指标与策略回测（TY版）
------------------------------------------------
功能：
1) 读取日K数据（含：日期,开盘,收盘,最高,最低,成交量,成交额,振幅,涨跌幅,涨跌额,换手率,代码）
2) 计算：MA20/MA60、MACD(12,26,9)、RSI(14)、布林带(20,2)、20日均量
3) 生成两类信号：
   A) 区间低吸高抛（低吸：53.2~54.2；止损：51.4；止盈：一档 56.5~57.0，二档 58.8~59.5）
   B) 突破跟随（突破：>60 且 成交量≥20日均量1.5x；防守：58.8；止盈：62.5 或 65）
4) 风险控制：
   - 单笔风险不超过账户净值的 risk_per_trade(默认1%)
   - 头寸按“止损距离”换算仓位；上限 max_position_pct（默认0.33）
   - 手续费 fee_rate（默认万分之10=0.001=千一/单边，可按需要调整或设为0）
5) 输出：
   - 交易逐笔记录 CSV：out/trades.csv
   - 指标与信号明细 CSV：out/indicators_signals.csv
   - 回测汇总打印 + 图表：out/equity_curve.png, out/panels.png

使用：
1) 将你提供的数据保存为 UTF-8 编码文件：xiaomi_01810_daily.csv（第一行是中文表头）
2) 运行本脚本：python backtest_xiaomi_1810.py
3) 所有输出在 out/ 目录查看

备注：
- 如需改阈值（53/54/60等），修改 CONFIG 中的参数即可。
- 回测为日线收盘价执行，未考虑盘中触发的更精细执行；可扩展。
"""

import os
import math
from dataclasses import dataclass
from typing import List, Dict, Tuple
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# -------------------
# 配置区（按需修改）
# -------------------
DATA_FILE = "hk_stock_report_20250926_171001/hk_daily_data_01810.csv"

@dataclass
class Config:
    start_capital: float = 1_000_000.0  # 初始资金（HKD）
    fee_rate: float = 0.001             # 手续费（单边），例如0.001=千一
    slippage: float = 0.0               # 滑点（按价格百分比；0表示忽略）
    risk_per_trade: float = 0.01        # 单笔风险占净值的上限（1%）
    max_position_pct: float = 0.33      # 单笔最大仓位比例上限（33%）
    # 指标参数
    ma_short: int = 20
    ma_long: int = 60
    bb_window: int = 20
    bb_std: float = 2.0
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    rsi_period: int = 14
    vol_ma_window: int = 20
    # 策略A：区间低吸
    A_buy_low: float = 53.2
    A_buy_high: float = 54.2
    A_stop: float = 51.4
    A_tp1_low: float = 56.5
    A_tp1_high: float = 57.0
    A_tp2_low: float = 58.8
    A_tp2_high: float = 59.5
    # 策略B：突破
    B_break_level: float = 60.0
    B_vol_mult: float = 1.5    # 成交量≥20日均量的倍数
    B_stop: float = 58.8
    B_tp1: float = 62.5
    B_tp2: float = 65.0
    # 其他：是否允许同一时间两策略并持（此处为不允许重叠）
    allow_overlap: bool = False

CONFIG = Config()

# -------------------
# 工具函数
# -------------------
def ensure_outdir(path="out"):
    if not os.path.exists(path):
        os.makedirs(path)

def read_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # 统一列名（去空格）
    df.columns = [c.strip() for c in df.columns]
    # 日期处理与排序
    df["日期"] = pd.to_datetime(df["日期"])
    df = df.sort_values("日期").reset_index(drop=True)
    # 转为数值
    numeric_cols = ["开盘","收盘","最高","最低","成交量","成交额","振幅","涨跌幅","涨跌额","换手率"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def calc_indicators(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    out = df.copy()
    # 均线
    out["MA20"] = out["收盘"].rolling(cfg.ma_short).mean()
    out["MA60"] = out["收盘"].rolling(cfg.ma_long).mean()
    # 布林带
    out["BB_MID"] = out["收盘"].rolling(cfg.bb_window).mean()
    out["BB_STD"] = out["收盘"].rolling(cfg.bb_window).std(ddof=0)
    out["BB_UP"] = out["BB_MID"] + cfg.bb_std * out["BB_STD"]
    out["BB_LOW"] = out["BB_MID"] - cfg.bb_std * out["BB_STD"]
    # MACD
    ema_fast = out["收盘"].ewm(span=cfg.macd_fast, adjust=False).mean()
    ema_slow = out["收盘"].ewm(span=cfg.macd_slow, adjust=False).mean()
    out["MACD_DIFF"] = ema_fast - ema_slow
    out["MACD_DEA"] = out["MACD_DIFF"].ewm(span=cfg.macd_signal, adjust=False).mean()
    out["MACD_BAR"] = (out["MACD_DIFF"] - out["MACD_DEA"]) * 2
    # RSI(14)
    delta = out["收盘"].diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.ewm(alpha=1/cfg.rsi_period, adjust=False).mean()
    roll_down = down.ewm(alpha=1/cfg.rsi_period, adjust=False).mean()
    rs = roll_up / (roll_down + 1e-12)
    out["RSI14"] = 100 - (100 / (1 + rs))
    # 20日均量
    out["VOL_MA20"] = out["成交量"].rolling(cfg.vol_ma_window).mean()
    return out

def within(x, low, high) -> bool:
    return (x >= low) and (x <= high)

# -------------------
# 信号生成
# -------------------
def generate_signals(df: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    d = df.copy()
    d["sig_A_buy"] = False
    d["sig_A_tp1"] = False
    d["sig_A_tp2"] = False
    d["sig_A_stop"] = False

    d["sig_B_buy"] = False
    d["sig_B_tp1"] = False
    d["sig_B_tp2"] = False
    d["sig_B_stop"] = False

    for i in range(1, len(d)):
        c = d.loc[i, "收盘"]
        p = d.loc[i-1, "收盘"]

        # A：区间低吸（收盘进入买点区间）
        if within(c, cfg.A_buy_low, cfg.A_buy_high):
            # 可附加过滤：RSI < 55 且 收盘 > BB_LOW
            if (d.loc[i, "RSI14"] < 55) and (c > d.loc[i, "BB_LOW"]):
                d.loc[i, "sig_A_buy"] = True

        # A 止盈/止损（用收盘价判定；实盘可换盘中）
        if within(c, cfg.A_tp1_low, cfg.A_tp1_high):
            d.loc[i, "sig_A_tp1"] = True
        if within(c, cfg.A_tp2_low, cfg.A_tp2_high):
            d.loc[i, "sig_A_tp2"] = True
        if c < cfg.A_stop:
            d.loc[i, "sig_A_stop"] = True

        # B：突破（收盘上破60且放量）
        if (c > cfg.B_break_level) and (p <= cfg.B_break_level):
            vol_ok = d.loc[i, "成交量"] >= cfg.B_vol_mult * (d.loc[i, "VOL_MA20"] or 0)
            if vol_ok:
                d.loc[i, "sig_B_buy"] = True

        # B 止盈/止损
        if c >= cfg.B_tp2:
            d.loc[i, "sig_B_tp2"] = True
        elif c >= cfg.B_tp1:
            d.loc[i, "sig_B_tp1"] = True
        if c < cfg.B_stop:
            d.loc[i, "sig_B_stop"] = True

    return d

# -------------------
# 简易回测引擎（单标的、日线、每次只持一笔）
# -------------------
@dataclass
class Trade:
    open_date: pd.Timestamp
    close_date: pd.Timestamp
    side: str  # 'long'
    open_price: float
    close_price: float
    shares: int
    pnL: float
    ret: float
    reason: str  # 'tp1'/'tp2'/'stop'/'exit'

def position_size(capital: float, entry: float, stop: float, cfg: Config) -> Tuple[int, float]:
    """
    按“单笔风险不超净值 risk_per_trade”来确定仓位：
    风险金额 = capital * risk_per_trade
    止损距离 = max(entry - stop, entry * slippage, 0.01)
    头寸价值 = min( 风险金额 / 止损距离 , capital * max_position_pct )
    shares = floor(头寸价值 / entry)
    """
    risk_amt = capital * cfg.risk_per_trade
    stop_dist = max(entry - stop, entry * cfg.slippage, 0.01)
    max_notional = capital * cfg.max_position_pct
    target_notional = min(risk_amt / stop_dist * entry, max_notional)
    shares = int(math.floor(target_notional / entry))
    return max(shares, 0), stop_dist

def backtest(df_sig: pd.DataFrame, cfg: Config) -> Tuple[pd.DataFrame, List[Trade], Dict]:
    d = df_sig.copy().reset_index(drop=True)
    capital = cfg.start_capital
    equity_curve = []
    pos_shares = 0
    pos_entry = 0.0
    pos_stop = 0.0
    in_pos = False
    open_idx = None
    trades: List[Trade] = []

    for i in range(len(d)):
        date = d.loc[i, "日期"]
        close = float(d.loc[i, "收盘"])

        # 1) 开仓逻辑（若空仓）
        if not in_pos:
            will_buy_A = bool(d.loc[i, "sig_A_buy"])
            will_buy_B = bool(d.loc[i, "sig_B_buy"])
            # 如不允许重叠，按先到先得；可改为优先级（B > A）
            if will_buy_A or will_buy_B:
                # 选择策略（优先B突破）
                use_B = will_buy_B if not (will_buy_A and will_buy_B) else True
                if use_B:
                    entry = close * (1 + cfg.slippage)
                    stop = cfg.B_stop
                    reason = "B_breakout"
                else:
                    entry = close * (1 + cfg.slippage)
                    stop = cfg.A_stop
                    reason = "A_range_buy"

                shares, _ = position_size(capital, entry, stop, cfg)
                if shares > 0:
                    fee = entry * shares * cfg.fee_rate
                    cost = entry * shares + fee
                    if cost <= capital:
                        # 建仓
                        capital -= cost
                        pos_shares = shares
                        pos_entry = entry
                        pos_stop = stop
                        in_pos = True
                        open_idx = i
                        d.loc[i, "open_flag"] = reason
                    else:
                        d.loc[i, "open_flag"] = "insufficient_capital"

        # 2) 持仓期间的止盈/止损（用收盘触发；可改为盘中高低判断）
        exit_reason = None
        if in_pos:
            # 止损（收盘跌破防守位）
            if close < pos_stop:
                exit_reason = "stop"
            else:
                # A 的分档止盈
                if df_sig.loc[i, "sig_A_tp2"]:
                    exit_reason = "A_tp2"
                elif df_sig.loc[i, "sig_A_tp1"]:
                    exit_reason = "A_tp1"
                # B 的分档止盈
                elif df_sig.loc[i, "sig_B_tp2"]:
                    exit_reason = "B_tp2"
                elif df_sig.loc[i, "sig_B_tp1"]:
                    exit_reason = "B_tp1"

        # 3) 执行平仓
        if in_pos and exit_reason:
            exit_price = close * (1 - cfg.slippage)
            proceeds = exit_price * pos_shares
            fee = proceeds * cfg.fee_rate
            proceeds -= fee
            pnl = proceeds - pos_entry * pos_shares - pos_entry * pos_shares * cfg.fee_rate  # 已在开仓扣过一次费，再扣平仓费
            ret = pnl / (pos_entry * pos_shares)
            trades.append(Trade(
                open_date=d.loc[open_idx, "日期"],
                close_date=date,
                side="long",
                open_price=pos_entry,
                close_price=exit_price,
                shares=pos_shares,
                pnL=pnl,
                ret=ret,
                reason=exit_reason
            ))
            capital += proceeds
            pos_shares = 0
            pos_entry = 0.0
            pos_stop = 0.0
            in_pos = False
            open_idx = None
            d.loc[i, "close_flag"] = exit_reason

        # 4) 记录权益（持仓按收盘价估值）
        mtm = 0.0
        if in_pos:
            mtm = close * pos_shares - pos_entry * pos_shares * cfg.fee_rate  # 市值-（开仓已扣费）
        equity_curve.append(capital + mtm)

    # 若回测结束仍有持仓，这里按最后一个收盘价平仓
    if in_pos and open_idx is not None:
        close = float(d.loc[len(d)-1, "收盘"])
        date = d.loc[len(d)-1, "日期"]
        exit_price = close * (1 - cfg.slippage)
        proceeds = exit_price * pos_shares
        fee = proceeds * cfg.fee_rate
        proceeds -= fee
        pnl = proceeds - pos_entry * pos_shares - pos_entry * pos_shares * cfg.fee_rate
        ret = pnl / (pos_entry * pos_shares)
        trades.append(Trade(
            open_date=d.loc[open_idx, "日期"],
            close_date=date,
            side="long",
            open_price=pos_entry,
            close_price=exit_price,
            shares=pos_shares,
            pnL=pnl,
            ret=ret,
            reason="eod_close"
        ))
        capital += proceeds
        pos_shares = 0

    ec = pd.Series(equity_curve, index=d["日期"])
    stats = compute_stats(ec, trades, start_capital=cfg.start_capital)
    return d, trades, stats

def compute_stats(ec: pd.Series, trades: List[Trade], start_capital: float) -> Dict:
    ret_total = ec.iloc[-1] / start_capital - 1.0
    # 日度收益序列
    daily_ret = ec.pct_change().dropna()
    # 年化收益（按252交易日）
    if len(daily_ret) > 0:
        ann_ret = (1 + daily_ret.mean())**252 - 1
        ann_vol = daily_ret.std(ddof=0) * np.sqrt(252)
        sharpe = 0 if ann_vol == 0 else ann_ret / ann_vol
    else:
        ann_ret = 0.0
        ann_vol = 0.0
        sharpe = 0.0

    # 最大回撤
    roll_max = ec.cummax()
    drawdown = (ec - roll_max) / roll_max
    mdd = drawdown.min()

    # 交易统计
    wins = [t for t in trades if t.pnL > 0]
    losses = [t for t in trades if t.pnL <= 0]
    win_rate = len(wins) / len(trades) if trades else 0.0
    avg_win = np.mean([t.ret for t in wins]) if wins else 0.0
    avg_loss = np.mean([t.ret for t in losses]) if losses else 0.0
    payoff = (abs(avg_win) / abs(avg_loss)) if (avg_loss != 0) else np.nan

    return {
        "start_capital": start_capital,
        "end_capital": float(ec.iloc[-1]),
        "total_return": ret_total,
        "annual_return": ann_ret,
        "annual_vol": ann_vol,
        "sharpe": sharpe,
        "max_drawdown": float(mdd),
        "num_trades": len(trades),
        "win_rate": win_rate,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "payoff_ratio": payoff
    }

# -------------------
# 可视化
# -------------------
def plot_panels(df: pd.DataFrame, ec: pd.Series):
    ensure_outdir("out")
    fig = plt.figure(figsize=(14, 9))

    ax1 = plt.subplot(3,1,1)
    ax1.plot(df["日期"], df["收盘"], label="Close")
    ax1.plot(df["日期"], df["MA20"], label="MA20")
    ax1.plot(df["日期"], df["MA60"], label="MA60")
    ax1.plot(df["日期"], df["BB_UP"], label="BB_UP", linestyle="--")
    ax1.plot(df["日期"], df["BB_MID"], label="BB_MID", linestyle="--")
    ax1.plot(df["日期"], df["BB_LOW"], label="BB_LOW", linestyle="--")
    # 关键位参考线
    for lvl, name in [(CONFIG.A_buy_low,"A_buy_low"), (CONFIG.A_buy_high,"A_buy_high"),
                      (CONFIG.A_stop,"A_stop"), (CONFIG.B_break_level,"B_break"),
                      (CONFIG.B_stop,"B_stop")]:
        ax1.axhline(lvl, color="gray", linestyle=":", linewidth=0.8)
        ax1.text(df["日期"].iloc[3], lvl, name, fontsize=8, va="bottom", ha="left")

    # 买卖信号
    buys = df.index[df["sig_A_buy"] | df["sig_B_buy"]]
    ax1.scatter(df.loc[buys, "日期"], df.loc[buys, "收盘"], marker="^", s=60, label="Buy", zorder=5)
    exits = df.index[df["sig_A_tp1"] | df["sig_A_tp2"] | df["sig_B_tp1"] | df["sig_B_tp2"] | df["sig_A_stop"] | df["sig_B_stop"]]
    ax1.scatter(df.loc[exits, "日期"], df.loc[exits, "收盘"], marker="v", s=50, label="ExitSig", zorder=5)

    ax1.set_title("Price with MAs / Bollinger & Signals")
    ax1.legend(loc="best")
    ax1.grid(alpha=0.2)

    ax2 = plt.subplot(3,1,2, sharex=ax1)
    ax2.plot(df["日期"], df["MACD_DIFF"], label="DIFF")
    ax2.plot(df["日期"], df["MACD_DEA"], label="DEA")
    ax2.bar(df["日期"], df["MACD_BAR"], label="BAR")
    ax2.set_title("MACD(12,26,9)")
    ax2.legend(loc="best")
    ax2.grid(alpha=0.2)

    ax3 = plt.subplot(3,1,3, sharex=ax1)
    ax3.plot(df["日期"], df["RSI14"], label="RSI14")
    ax3.axhline(30, linestyle="--", linewidth=0.8)
    ax3.axhline(70, linestyle="--", linewidth=0.8)
    ax3.set_title("RSI(14)")
    ax3.legend(loc="best")
    ax3.grid(alpha=0.2)

    plt.tight_layout()
    plt.savefig("out/panels.png", dpi=150)
    plt.close()

    # 权益曲线
    plt.figure(figsize=(12,4))
    plt.plot(ec.index, ec.values, label="Equity")
    plt.title("Equity Curve")
    plt.legend()
    plt.grid(alpha=0.2)
    plt.tight_layout()
    plt.savefig("out/equity_curve.png", dpi=150)
    plt.close()

# -------------------
# 主流程
# -------------------
def main():
    ensure_outdir("out")
    df0 = read_data(DATA_FILE)
    df1 = calc_indicators(df0, CONFIG)
    df2 = generate_signals(df1, CONFIG)
    df2.to_csv("out/indicators_signals.csv", index=False, encoding="utf-8-sig")

    df_bt, trades, stats = backtest(df2, CONFIG)

    # 打印回测结果
    print("\n=== 回测结果（TY版）===")
    for k,v in stats.items():
        if isinstance(v, float):
            if "return" in k or "rate" in k or "drawdown" in k or "vol" in k or "sharpe" in k:
                print(f"{k:>15s}: {v:.4f}")
            else:
                print(f"{k:>15s}: {v:.2f}")
        else:
            print(f"{k:>15s}: {v}")

    # 交易明细保存
    rows = []
    for t in trades:
        rows.append({
            "open_date": t.open_date,
            "close_date": t.close_date,
            "side": t.side,
            "open_price": t.open_price,
            "close_price": t.close_price,
            "shares": t.shares,
            "pnl": t.pnL,
            "ret": t.ret,
            "reason": t.reason
        })
    trades_df = pd.DataFrame(rows)
    trades_df.to_csv("out/trades.csv", index=False, encoding="utf-8-sig")

    # 画图
    # 用回测中记录的权益曲线重建
    # 这里简单再跑一遍 equity 序列（也可直接在 backtest 中返回）
    # 为简洁，这里复用：用资金曲线近似 -> 从 trades 重建不如直接回传；我们稍作近似：读取 indicators_signals.csv 重新估值会很冗长
    # 为一致性，修改 backtest 返回 equity series：
    # （简化：在 backtest 中已创建 ec，这里直接重算一次不如改 backtest；为整洁，我们直接在 backtest 内部已经生成 ec，重新计算略麻烦）
    # 因此在 backtest 中已经计算 equity_curve，但未返回。我们简单重算一次：复制一份逻辑会太长。
    # 解决：这里快速用“净值=起始资金*(1+total_return)的直线”仅做占位图；更严谨：把 backtest 中的 equity_curve 加一个返回。
    # ——为保持完整性，我们稍微改 backtest：返回 stats 时附带 'equity_curve'。
    # （为避免修改太多，上移 backtest；但这里我们重新调用以拿到 equity curve）
    # 为一次性给出完整可运行代码，我们直接轻微改 backtest：让其返回 d, trades, stats，同时把 equity曲线序列挂到 stats["equity_series"]
    # ——请向上看，回测函数尚未返回 equity；下面先修正：我们在 backtest 末尾添加 stats["equity_series"]=ec 后再返回

if __name__ == "__main__":
    # --- 小修 backtest：给 stats 附带 equity series，便于画图 ---
    # 重新定义 backtest，复用上方函数体但添加 equity返回（避免打断主结构）
    def backtest(df_sig: pd.DataFrame, cfg: Config) -> Tuple[pd.DataFrame, List[Trade], Dict]:
        d = df_sig.copy().reset_index(drop=True)
        capital = cfg.start_capital
        equity_curve = []
        pos_shares = 0
        pos_entry = 0.0
        pos_stop = 0.0
        in_pos = False
        open_idx = None
        trades: List[Trade] = []

        for i in range(len(d)):
            date = d.loc[i, "日期"]
            close = float(d.loc[i, "收盘"])

            # 开仓
            if not in_pos:
                will_buy_A = bool(d.loc[i, "sig_A_buy"])
                will_buy_B = bool(d.loc[i, "sig_B_buy"])
                if will_buy_A or will_buy_B:
                    use_B = will_buy_B if not (will_buy_A and will_buy_B) else True
                    if use_B:
                        entry = close * (1 + cfg.slippage); stop = cfg.B_stop; reason = "B_breakout"
                    else:
                        entry = close * (1 + cfg.slippage); stop = cfg.A_stop; reason = "A_range_buy"
                    shares, _ = position_size(capital, entry, stop, cfg)
                    if shares > 0:
                        fee = entry * shares * cfg.fee_rate
                        cost = entry * shares + fee
                        if cost <= capital:
                            capital -= cost
                            pos_shares = shares
                            pos_entry = entry
                            pos_stop = stop
                            in_pos = True
                            open_idx = i
                            d.loc[i, "open_flag"] = reason

            # 止盈/止损
            exit_reason = None
            if in_pos:
                if close < pos_stop:
                    exit_reason = "stop"
                else:
                    if df_sig.loc[i, "sig_A_tp2"]: exit_reason = "A_tp2"
                    elif df_sig.loc[i, "sig_A_tp1"]: exit_reason = "A_tp1"
                    elif df_sig.loc[i, "sig_B_tp2"]: exit_reason = "B_tp2"
                    elif df_sig.loc[i, "sig_B_tp1"]: exit_reason = "B_tp1"

            if in_pos and exit_reason:
                exit_price = close * (1 - cfg.slippage)
                proceeds = exit_price * pos_shares
                fee = proceeds * cfg.fee_rate
                proceeds -= fee
                pnl = proceeds - pos_entry * pos_shares - pos_entry * pos_shares * cfg.fee_rate
                ret = pnl / (pos_entry * pos_shares)
                trades.append(Trade(
                    open_date=d.loc[open_idx, "日期"],
                    close_date=date,
                    side="long",
                    open_price=pos_entry,
                    close_price=exit_price,
                    shares=pos_shares,
                    pnL=pnl,
                    ret=ret,
                    reason=exit_reason
                ))
                capital += proceeds
                pos_shares = 0; pos_entry = 0.0; pos_stop = 0.0; in_pos = False; open_idx = None
                d.loc[i, "close_flag"] = exit_reason

            # 记录权益
            mtm = 0.0
            if in_pos:
                mtm = close * pos_shares - pos_entry * pos_shares * cfg.fee_rate
            equity_curve.append(capital + mtm)

        # 收尾强平
        if in_pos and open_idx is not None:
            close = float(d.loc[len(d)-1, "收盘"])
            date = d.loc[len(d)-1, "日期"]
            exit_price = close * (1 - cfg.slippage)
            proceeds = exit_price * pos_shares
            fee = proceeds * cfg.fee_rate
            proceeds -= fee
            pnl = proceeds - pos_entry * pos_shares - pos_entry * pos_shares * cfg.fee_rate
            ret = pnl / (pos_entry * pos_shares)
            trades.append(Trade(
                open_date=d.loc[open_idx, "日期"],
                close_date=date,
                side="long",
                open_price=pos_entry,
                close_price=exit_price,
                shares=pos_shares,
                pnL=pnl,
                ret=ret,
                reason="eod_close"
            ))
            capital += proceeds

        ec = pd.Series(equity_curve, index=d["日期"])
        stats = compute_stats(ec, trades, start_capital=cfg.start_capital)
        stats["equity_series"] = ec

        # 输出可视化与CSV
        ensure_outdir("out")
        # 保存带信号的数据
        d.to_csv("out/indicators_signals.csv", index=False, encoding="utf-8-sig")
        # 保存交易
        rows = [{
            "open_date": t.open_date, "close_date": t.close_date, "side": t.side,
            "open_price": t.open_price, "close_price": t.close_price, "shares": t.shares,
            "pnl": t.pnL, "ret": t.ret, "reason": t.reason
        } for t in trades]
        pd.DataFrame(rows).to_csv("out/trades.csv", index=False, encoding="utf-8-sig")
        # 画图
        plot_panels(d, ec)
        return d, trades, stats

    # 运行
    ensure_outdir("out")
    df0 = read_data(DATA_FILE)
    df1 = calc_indicators(df0, CONFIG)
    df2 = generate_signals(df1, CONFIG)
    df_bt, trades, stats = backtest(df2, CONFIG)

    print("\n=== 回测结果（TY版）===")
    for k,v in stats.items():
        if k == "equity_series":
            continue
        if isinstance(v, float):
            if "return" in k or "rate" in k or "drawdown" in k or "vol" in k or "sharpe" in k:
                print(f"{k:>15s}: {v:.4f}")
            else:
                print(f"{k:>15s}: {v:.2f}")
        else:
            print(f"{k:>15s}: {v}")

    print("\n文件已生成：out/indicators_signals.csv, out/trades.csv, out/panels.png, out/equity_curve.png")
