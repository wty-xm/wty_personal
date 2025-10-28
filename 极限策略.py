# -*- coding: utf-8 -*-
"""
反趋势跨资产回测框架（基于“连续单边→反向持有一个周期”）
Author: TY
Requirements: pandas, numpy, matplotlib (可选), openpyxl (读取xlsx)
Python >= 3.9
"""

import os
import math
import json
import numpy as np
import pandas as pd
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

# =========================
# 配置区（根据你的Excel作最小改动）
# =========================

CONFIG = {

    # === 数据源路径 ===
    "DATA_PATH": "极限配置策略-数据.xlsx",

    # === 数据映射（请按你的Excel实际情况修改）===
    # 支持多个sheet；每个sheet配置：日期列名、价格列清单、所属资产类别（用于仓位参数）
    # 例：一个sheet放国内股票指数；另一个sheet放大宗商品；也可以一个sheet放全部价格列
    "DATA_MAPPING": [
        {
            "sheet": "prices",         # Sheet名称（请改成你文件中的sheet）
            "date_col": "date",        # 日期列名（区分大小写，或用实际列名）
            "price_cols": [
                # —— 股票类（A/H/US，按需取代表指数或宽基ETF/行业指数）——
                {"col": "CSI300", "asset_class": "EQUITY", "label": "A股_沪深300"},
                {"col": "SSEC",   "asset_class": "EQUITY", "label": "A股_上证综指"},
                {"col": "HSI",    "asset_class": "EQUITY", "label": "港股_恒指"},
                {"col": "SPX",    "asset_class": "EQUITY", "label": "美股_S&P500"},
                # —— 债券类（可用中美国债指数、信用债指数、可转债指数或ETF净值）——
                {"col": "CGB10",  "asset_class": "BOND",   "label": "中国10Y国债指数"},
                {"col": "UST10",  "asset_class": "BOND",   "label": "美国10Y国债指数"},
                {"col": "CN_CREDIT", "asset_class": "BOND", "label": "中国信用债指数"},
                {"col": "CN_CB",     "asset_class": "BOND", "label": "中国可转债指数"},
                # —— 大宗商品（主连或连续指数/现货代表价）——
                {"col": "GOLD",   "asset_class": "COMMO",  "label": "黄金"},
                {"col": "SILVER", "asset_class": "COMMO",  "label": "白银"},
                {"col": "COPPER", "asset_class": "COMMO",  "label": "铜"},
                {"col": "IRON",   "asset_class": "COMMO",  "label": "铁矿石"},
                {"col": "WTI",    "asset_class": "COMMO",  "label": "原油WTI"},
                {"col": "COAL",   "asset_class": "COMMO",  "label": "动力煤"},
                {"col": "NG",     "asset_class": "COMMO",  "label": "天然气"},
                # —— 如需行业/市值分层，请另行添加列并在 SIGNAL_RULES 中单独设置 —— 
            ],
        },
        # 若有其它 sheet，可在此继续添加字典
    ],

    # === 信号规则：连续期阈值（你的要求） ===
    "SIGNAL_RULES": {
        "W": {"up_streak": 9, "down_streak": 9, "resample": "W-FRI"},
        "M": {"up_streak": 8, "down_streak": 8, "resample": "M"},
        "Q": {"up_streak": 5, "down_streak": 5, "resample": "Q"},
    },

    # === 仓位参数（按资产类别微调灵敏度与上限、最小仓位）===
    # 仓位计算： weight = clip( SENSITIVITY * |累计涨跌幅|, min_pos, max_pos )
    # 核心思想：商品趋势更持久 → 需要更大幅度才加满仓位；债券更“短” → 小幅也给到可观仓位；股票居中
    "POSITION_PARAMS": {
        "COMMO": {"SENSITIVITY": 3.0, "max_pos": 0.40, "min_pos": 0.05},
        "BOND":  {"SENSITIVITY": 6.0, "max_pos": 0.50, "min_pos": 0.05},
        "EQUITY":{"SENSITIVITY": 4.0, "max_pos": 0.45, "min_pos": 0.05},
    },

    # === 组合层面风险控制 ===
    "PORTFOLIO": {
        # 同一周期内若多资产同时触发信号，允许的总绝对仓位上限（>1 则允许杠杆）
        "gross_leverage_cap": 1.00,
        # 单标的仓位硬上限（与 POSITION_PARAMS 的 max_pos 取较小者）
        "per_symbol_cap": 0.60,
        # 交易成本（双边合计bps；例如 5 表示买+卖合计0.05%）
        "round_trip_cost_bps": 5.0,
    },

    # === 输出控制 ===
    "OUTPUT": {
        "report_csv": "backtest_report_summary.csv",
        "trades_csv": "backtest_trades_log.csv",
        "equity_csv": "backtest_equity_curve.csv",
        "by_asset_csv": "by_asset_contrib.csv",
        "by_freq_csv": "by_freq_contrib.csv",
        "plot_equity": True,   # 若不需要图，设为 False
        "plot_path": "equity_curve.png",
    },
}


# =========================
# 工具函数
# =========================

def _safe_pct_change(s: pd.Series) -> pd.Series:
    return s.pct_change().replace([np.inf, -np.inf], np.nan)

def _last_valid(df: pd.DataFrame) -> pd.DataFrame:
    return df.ffill().bfill()

def _annualize_factor(freq: str) -> float:
    if freq.startswith("W"):
        return 52.0
    if freq.startswith("M"):
        return 12.0
    if freq.startswith("Q"):
        return 4.0
    return 252.0  # 默认日频


@dataclass
class SymbolMeta:
    symbol: str
    label: str
    asset_class: str


# =========================
# 数据加载与适配
# =========================

class DataAdapter:
    def __init__(self, config: dict):
        self.config = config
        self.path = config["DATA_PATH"]
        self.mapping = config["DATA_MAPPING"]

    def load_daily_close(self) -> Tuple[pd.DataFrame, Dict[str, SymbolMeta]]:
        """读取各Sheet并拼成统一的日频收盘价表，列名统一为symbol；返回 (prices_df, meta_dict)"""
        assert os.path.exists(self.path), f"Excel文件不存在: {self.path}"
        xl = pd.ExcelFile(self.path)
        frames = []
        meta: Dict[str, SymbolMeta] = {}

        for block in self.mapping:
            sheet = block["sheet"]
            date_col = block["date_col"]
            price_cols = block["price_cols"]
            assert sheet in xl.sheet_names, f"缺少Sheet: {sheet}"

            df = pd.read_excel(self.path, sheet_name=sheet)
            assert date_col in df.columns, f"{sheet} 缺少日期列: {date_col}"
            df[date_col] = pd.to_datetime(df[date_col])
            df = df.set_index(date_col).sort_index()

            for p in price_cols:
                col = p["col"]
                assert col in df.columns, f"{sheet} 缺少价格列: {col}"
                sym = col  # 用原列名作为symbol
                frames.append(df[[col]].rename(columns={col: sym}))
                meta[sym] = SymbolMeta(symbol=sym, label=p.get("label", sym), asset_class=p["asset_class"])

        prices = pd.concat(frames, axis=1)
        prices = prices.sort_index()
        prices = _last_valid(prices)
        return prices, meta


# =========================
# 信号与仓位生成
# =========================

def compute_period_close(prices: pd.DataFrame, resample_rule: str) -> pd.DataFrame:
    """按规则重采样为期末收盘（周五/月底/季末）"""
    return prices.resample(resample_rule).last()

def calc_streaks(returns: pd.Series) -> Tuple[pd.Series, pd.Series]:
    """计算连续上涨/下跌期数（基于每期收益>0/<0）"""
    up = (returns > 0).astype(int)
    down = (returns < 0).astype(int)

    up_streak = up.copy()
    down_streak = down.copy()

    for i in range(1, len(up)):
        up_streak.iat[i] = up_streak.iat[i-1] + 1 if up.iat[i] == 1 else 0
        down_streak.iat[i] = down_streak.iat[i-1] + 1 if down.iat[i] == 1 else 0

    return up_streak, down_streak

def position_size_from_amplitude(ampl: float, asset_class: str, config: dict) -> float:
    """由累计涨跌幅绝对值计算目标仓位，按资产类别灵敏度/上下限裁剪"""
    params = config["POSITION_PARAMS"].get(asset_class, {"SENSITIVITY": 4.0, "max_pos": 0.4, "min_pos": 0.0})
    w = params["SENSITIVITY"] * abs(ampl)
    w = max(w, params.get("min_pos", 0.0))
    w = min(w, params["max_pos"])
    # 叠加全局单标的上限
    w = min(w, config["PORTFOLIO"]["per_symbol_cap"])
    return float(w)

@dataclass
class Trade:
    start: pd.Timestamp
    end: pd.Timestamp
    symbol: str
    asset_class: str
    freq_key: str
    direction: int    # +1 做多, -1 做空
    weight: float
    entry_px: float
    exit_px: float
    ret: float        # 该周期标的收益
    pnl: float        # 含仓位与成本后的收益
    gross_leverage_at_entry: float


# =========================
# 回测主逻辑（按频段独立生成交易，再合并）
# =========================

class ContrarianEngine:
    def __init__(self, prices_daily: pd.DataFrame, meta: Dict[str, SymbolMeta], config: dict):
        self.prices_daily = prices_daily
        self.meta = meta
        self.config = config

    def _gen_trades_for_freq(self, freq_key: str) -> List[Trade]:
        """针对一个频段（W/M/Q）生成所有交易"""
        rule = self.config["SIGNAL_RULES"][freq_key]["resample"]
        up_need = self.config["SIGNAL_RULES"][freq_key]["up_streak"]
        dn_need = self.config["SIGNAL_RULES"][freq_key]["down_streak"]

        closes = compute_period_close(self.prices_daily, rule)
        rets = closes.pct_change()

        trades: List[Trade] = []

        # 逐symbol处理（信号形成于 t ，在 t+1 持有一个周期）
        for sym in closes.columns:
            series = closes[sym].dropna()
            if len(series) < max(up_need, dn_need) + 3:
                continue

            r = series.pct_change()
            up_streak, dn_streak = calc_streaks(r)

            # 累计幅度：从 streak 起点到当前t（用几何累计）
            # 我们在t形成信号时，计算最近streak长度对应的累计收益幅度
            for t in range(len(series)):
                ts = series.index[t]
                # 只在 t 时点检查是否完成一段连续
                u = up_streak.iloc[t] if t < len(up_streak) else 0
                d = dn_streak.iloc[t] if t < len(dn_streak) else 0

                direction = 0
                streak_len = 0
                if u >= up_need:
                    direction = -1  # 连涨→做空
                    streak_len = u
                elif d >= dn_need:
                    direction = +1  # 连跌→做多
                    streak_len = d
                else:
                    continue

                # 计算累计幅度（从 streak_len-1 根之前开始累到 t）
                start_idx = max(0, t - streak_len + 1)
                sub = series.iloc[start_idx:t+1]
                ampl = sub.iloc[-1] / sub.iloc[0] - 1.0

                asset_class = self.meta[sym].asset_class
                w = position_size_from_amplitude(ampl, asset_class, self.config)

                # 进出场时点：在 t 结束后，于 t+1 开始持有一个完整周期，到 t+1 的期末退出
                if t + 1 >= len(series):
                    continue  # 没有下一期，不能开仓

                entry_time = series.index[t+1]
                # 持有一个周期：从 entry_time 到后一根期末（即 t+2 的时点退出）
                if t + 2 >= len(series):
                    continue  # 不足一个完整持有期

                exit_time = series.index[t+2]

                entry_px = series.iloc[t+1]
                exit_px = series.iloc[t+2]
                period_ret = exit_px / entry_px - 1.0

                # 成本与组合总杠杆在组合层面统一处理；此处先记录原始
                tr = Trade(
                    start=entry_time, end=exit_time, symbol=sym,
                    asset_class=asset_class, freq_key=freq_key,
                    direction=direction, weight=w,
                    entry_px=entry_px, exit_px=exit_px,
                    ret=period_ret, pnl=np.nan,
                    gross_leverage_at_entry=np.nan
                )
                trades.append(tr)

        # 时间排序
        trades.sort(key=lambda x: (x.start, x.symbol))
        return trades

    def backtest(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, List[Trade]]:
        """回测：合并W/M/Q三条线的交易，按同一时间的总绝对仓位做等比缩放，计入成本"""
        all_trades: List[Trade] = []
        for k in self.config["SIGNAL_RULES"].keys():
            all_trades.extend(self._gen_trades_for_freq(k))

        if not all_trades:
            raise ValueError("没有生成任何交易。请检查数据映射、列名与阈值设置。")

        # 组合层面在每个“持有期起点”聚合杠杆并做缩放
        gross_cap = self.config["PORTFOLIO"]["gross_leverage_cap"]
        cost_bps = self.config["PORTFOLIO"]["round_trip_cost_bps"]

        # 建立索引：entry_time -> 同期所有交易
        by_start: Dict[pd.Timestamp, List[int]] = {}
        for i, tr in enumerate(all_trades):
            by_start.setdefault(tr.start, []).append(i)

        for ts, idxs in by_start.items():
            gross = sum(abs(all_trades[i].weight) for i in idxs)
            scale = 1.0 if gross <= gross_cap else (gross_cap / gross if gross > 0 else 1.0)
            for i in idxs:
                tr = all_trades[i]
                eff_w = tr.weight * scale
                # 单笔P&L（含方向）
                gross_ret = eff_w * (tr.direction * tr.ret)
                # 成本：一进一出合计
                net_ret = gross_ret - eff_w * (cost_bps / 10000.0)
                all_trades[i].pnl = net_ret
                all_trades[i].gross_leverage_at_entry = gross * scale

        # 生成权益曲线（以各频段“期末点”对齐，期间不做插值）
        # 组合收益按交易“退出时点”聚合
        pnl_df = pd.DataFrame(
            [(tr.end, tr.pnl, tr.freq_key, tr.symbol, tr.asset_class) for tr in all_trades],
            columns=["time", "pnl", "freq", "symbol", "asset_class"]
        ).set_index("time").sort_index()

        equity = (1.0 + pnl_df["pnl"].groupby(level=0).sum()).cumprod().rename("equity")
        equity.index.name = "time"

        # 汇总指标
        total_ret = equity.iloc[-1] - 1.0
        ret_series = pnl_df.groupby(level=0)["pnl"].sum()
        ann = _annualize_factor("W")  # 这里把“事件序列”近似为周频统计；若要更严谨可换成交割节奏推导
        vol = ret_series.std(ddof=0) * math.sqrt(ann) if len(ret_series) > 1 else np.nan
        sharpe = (ret_series.mean() * ann) / vol if (vol and vol != 0 and not np.isnan(vol)) else np.nan
        dd = (equity / equity.cummax() - 1.0).min()

        summary = pd.DataFrame({
            "metric": ["Total Return", "Sharpe (approx)", "Max Drawdown"],
            "value": [total_ret, sharpe, dd]
        })

        # 分解：按资产与频段贡献
        by_asset = pnl_df.groupby(["asset_class", "symbol"])["pnl"].sum().sort_values(ascending=False).to_frame("pnl")
        by_freq = pnl_df.groupby("freq")["pnl"].sum().sort_values(ascending=False).to_frame("pnl")

        return summary, equity.to_frame(), by_asset, all_trades


# =========================
# 主入口
# =========================

def main():
    # 1) 加载日频收盘价
    adapter = DataAdapter(CONFIG)
    prices_daily, meta = adapter.load_daily_close()

    # 2) 回测
    engine = ContrarianEngine(prices_daily, meta, CONFIG)
    summary, equity, by_asset, trades = engine.backtest()

    # 3) 结果落盘
    out_cfg = CONFIG["OUTPUT"]
    summary.to_csv(out_cfg["report_csv"], index=False)
    equity.to_csv(out_cfg["equity_csv"])

    by_asset.reset_index().to_csv(out_cfg["by_asset_csv"], index=False)

    # 频段贡献
    # 从 trades 重建 by_freq
    df_tr = pd.DataFrame([asdict(t) for t in trades])
    by_freq = df_tr.groupby("freq_key")["pnl"].sum().sort_values(ascending=False).to_frame("pnl")
    by_freq.to_csv(out_cfg["by_freq_csv"])

    # 交易日志
    df_tr.to_csv(out_cfg["trades_csv"], index=False)

    # 4) 可选：绘图
    if out_cfg.get("plot_equity", False):
        try:
            import matplotlib.pyplot as plt
            plt.figure(figsize=(10, 5))
            equity["equity"].plot()
            plt.title("Equity Curve (Contrarian Multi-Asset)")
            plt.xlabel("Time")
            plt.ylabel("Net Value")
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(out_cfg["plot_path"], dpi=160)
            plt.close()
        except Exception as e:
            print("绘图失败（可忽略）：", e)

    # 5) 控制台打印主要指标
    print("=== 回测摘要 ===")
    print(summary)
    print("\n=== 末值净值 ===", float(equity.iloc[-1]))
    print("\n=== 前十资产贡献 ===")
    print(by_asset.head(10))
    print("\n=== 频段贡献 ===")
    print(by_freq)

if __name__ == "__main__":
    main()
