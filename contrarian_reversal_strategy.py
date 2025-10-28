"""
跨资产反趋势交易脚本（基于“连续单边走势+反向持有一个周期”策略）
--------------------------------------------------------------
用法：
    python contrarian_reversal_strategy.py
    python contrarian_reversal_strategy.py --excel 极限配置策略-数据.xlsx

默认参数来自策略描述，可在 DEFAULT_CONFIG 中按需调整，
也可以通过 --config-json <path> 传入一个 JSON 文件覆盖配置。

输出：
    - 回测指标汇总（CSV）
    - 权益曲线（CSV）
    - 交易日志（CSV）
    - 资产与频段贡献（CSV）
"""

import argparse
import json
import math
import os
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


# ----------------------------------------------------------------------
# 参数配置（可按需调整或通过 JSON 文件覆盖）
# ----------------------------------------------------------------------

DEFAULT_CONFIG = {
    "excel_path": "极限配置策略-数据.xlsx",
    # 频率配置：键为 Excel 中的 sheet 名
    "frequencies": {
        "周": {
            "freq_label": "W",
            "up_streak": 7,
            "down_streak": 7,
            "holding_periods": 2,
            "min_amplitude": 0.05,  # 仅在累计涨跌幅绝对值超过阈值时触发
        },
        "月": {
            "freq_label": "M",
            "up_streak": 8,
            "down_streak": 8,
            "holding_periods": 1,
            "min_amplitude": 0.08,
        },
        "季度": {
            "freq_label": "Q",
            "up_streak": 5,
            "down_streak": 5,
            "holding_periods": 1,
            "min_amplitude": 0.10,
        },
    },
    "trading": {
        "long_only": True,  # 默认仅做多；设置为 False 可同时做空
    },
    # 资产类别映射（未列出者归为 DEFAULT）
    "asset_class_map": {
        "中债-新综合财富(10年以上)指数": "BOND",
        "美国国债10年期": "BOND",
        "伦敦金现": "COMMO",
        "伦敦银现": "COMMO",
        "ICE布油": "COMMO",
        "煤炭": "COMMO",
        "现货结算价:LME铜": "COMMO",
        "期货收盘价(连续):CBOT小麦": "COMMO",
        "期货收盘价(连续):CBOT大豆": "COMMO",
        "标普500": "EQUITY",
        "中证全指": "EQUITY",
        "万得科技大类指数": "EQUITY",
        "中证红利": "EQUITY",
        "中信风格指数:金融": "EQUITY",
        "中信风格指数:周期": "EQUITY",
        "中信风格指数:消费": "EQUITY",
        "中信风格指数:成长": "EQUITY",
        "中信风格指数:稳定": "EQUITY",
        "恒生指数": "EQUITY",
        "恒生地产分类指数": "EQUITY",
        "恒生综合行业指数-资讯科技业": "EQUITY",
        "恒生医疗保健指数": "EQUITY",
        "申万小盘指数": "EQUITY",
        "申万大盘指数": "EQUITY",
        "申万中盘指数": "EQUITY",
    },
    # 仓位参数：动态仓位 = sensitivity * |累计涨跌幅|，并按上下限截断
    "position": {
        "COMMO": {"sensitivity": 3.0, "min_pos": 0.05, "max_pos": 0.35},
        "BOND": {"sensitivity": 6.0, "min_pos": 0.05, "max_pos": 0.50},
        "EQUITY": {"sensitivity": 4.0, "min_pos": 0.05, "max_pos": 0.40},
        "DEFAULT": {"sensitivity": 4.0, "min_pos": 0.0, "max_pos": 0.30},
    },
    "portfolio": {
        "gross_cap": 1.0,          # 多标的并发时的总绝对仓位上限（>1 允许杠杆）
        "per_symbol_cap": 0.60,    # 单标的仓位上限
        "round_trip_cost_bps": 5.0  # 双边交易成本（bps）
    },
    "output": {
        "summary_csv": "contrarian_summary.csv",
        "equity_csv": "contrarian_equity_curve.csv",
        "trades_csv": "contrarian_trades.csv",
        "by_asset_csv": "contrarian_by_asset.csv",
        "by_freq_csv": "contrarian_by_freq.csv",
    },
}


# ----------------------------------------------------------------------
# 数据结构
# ----------------------------------------------------------------------

@dataclass
class Trade:
    symbol: str
    asset_class: str
    freq_label: str
    signal_time: pd.Timestamp
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    direction: int                 # +1 = 做多, -1 = 做空
    streak_len: int
    amplitude: float               # 连续段累计涨跌幅
    raw_weight: float
    trade_return: float            # 标的收益（不含方向）
    scaled_weight: float = 0.0
    gross_leverage: float = 0.0
    pnl: float = 0.0


# ----------------------------------------------------------------------
# 工具函数
# ----------------------------------------------------------------------

def load_config_from_json(path: Optional[str]) -> Dict:
    if not path:
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    return data


def deep_update(base: Dict, overrides: Dict) -> Dict:
    """递归更新嵌套字典"""
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            base[k] = deep_update(base[k], v)
        else:
            base[k] = v
    return base


def load_prices(sheet: str, excel_path: str) -> pd.DataFrame:
    """读取指定 sheet，返回按日期索引的价格表"""
    df = pd.read_excel(excel_path, sheet_name=sheet, header=1)
    if "日期" not in df.columns:
        raise KeyError(f"{sheet} 缺少 '日期' 列，请检查表头（可能需要调整 header 行）。")
    df["日期"] = pd.to_datetime(df["日期"])
    df = df.dropna(how="all")
    df = df.set_index("日期").sort_index()
    # 前向填充缺失值，避免中断 streak 计算
    df = df.ffill()
    # 去除全为空的列
    df = df.dropna(axis=1, how="all")
    return df


def calc_streaks(returns: pd.Series) -> pd.DataFrame:
    """计算连续上涨/下跌期数"""
    up_streak = pd.Series(0, index=returns.index, dtype=int)
    down_streak = pd.Series(0, index=returns.index, dtype=int)
    up_run = 0
    down_run = 0

    for idx, val in returns.items():
        if not np.isfinite(val) or val == 0:
            up_run = 0
            down_run = 0
        elif val > 0:
            up_run += 1
            down_run = 0
        else:  # val < 0
            down_run += 1
            up_run = 0
        up_streak.at[idx] = up_run
        down_streak.at[idx] = down_run

    return pd.DataFrame({"up": up_streak, "down": down_streak})


def position_from_amplitude(amplitude: float, asset_class: str, config: Dict) -> float:
    """根据累计涨跌幅绝对值计算目标仓位"""
    params = config["position"].get(asset_class, config["position"]["DEFAULT"])
    abs_amp = abs(amplitude)
    if abs_amp <= 0:
        return 0.0
    weight = params["sensitivity"] * abs_amp
    weight = max(weight, params.get("min_pos", 0.0))
    weight = min(weight, params["max_pos"])
    weight = min(weight, config["portfolio"]["per_symbol_cap"])
    return float(weight)


def estimate_periods_per_year(index: pd.Index) -> Optional[float]:
    """估算每年期数（用于波动率年化）"""
    if len(index) < 3:
        return None
    deltas = index.to_series().diff().dt.days.dropna()
    deltas = deltas[deltas > 0]
    if deltas.empty:
        return None
    median_days = deltas.median()
    if median_days <= 0:
        return None
    return 365.25 / median_days


# ----------------------------------------------------------------------
# 信号与交易生成
# ----------------------------------------------------------------------

def generate_trades(prices: pd.DataFrame, freq_cfg: Dict, asset_map: Dict, config: Dict) -> List[Trade]:
    trades: List[Trade] = []
    holding_periods = freq_cfg.get("holding_periods", 1)
    freq_label = freq_cfg["freq_label"]
    up_need = freq_cfg["up_streak"]
    down_need = freq_cfg["down_streak"]
    long_only = config.get("trading", {}).get("long_only", False)
    min_amplitude = float(freq_cfg.get("min_amplitude", 0.0))

    for col in prices.columns:
        series = prices[col].dropna()
        if len(series) < up_need + down_need + 5:
            continue
        returns = series.pct_change()
        streaks = calc_streaks(returns)
        asset_class = asset_map.get(col, "DEFAULT")

        max_signal_idx = len(series) - holding_periods - 1
        for pos in range(1, max_signal_idx + 1):
            up_run = int(streaks.iloc[pos]["up"])
            down_run = int(streaks.iloc[pos]["down"])
            direction = 0
            streak_len = 0

            if long_only:
                if down_run >= down_need:
                    direction = 1   # 仅在连跌情况下做多
                    streak_len = down_run
            else:
                if up_run >= up_need:
                    direction = -1  # 连涨 → 做空
                    streak_len = up_run
                elif down_run >= down_need:
                    direction = 1   # 连跌 → 做多
                    streak_len = down_run

            if direction == 0 or streak_len <= 0:
                continue

            start_idx = pos - streak_len + 1
            if start_idx < 0:
                continue

            entry_idx = pos + 1
            exit_idx = entry_idx + holding_periods - 1
            if exit_idx >= len(series):
                continue

            # 持有期收益：累计 holding_periods 个收益率
            ret_slice = returns.iloc[entry_idx: exit_idx + 1].dropna()
            if len(ret_slice) < holding_periods:
                continue
            gross_return = float(np.prod(1.0 + ret_slice.values) - 1.0)

            amplitude = float(series.iloc[pos] / series.iloc[start_idx] - 1.0)
            if abs(amplitude) < min_amplitude:
                continue

            raw_weight = position_from_amplitude(amplitude, asset_class, config)
            if raw_weight <= 0:
                continue

            trade = Trade(
                symbol=col,
                asset_class=asset_class,
                freq_label=freq_label,
                signal_time=series.index[pos],
                entry_time=series.index[entry_idx],
                exit_time=series.index[exit_idx],
                direction=direction,
                streak_len=streak_len,
                amplitude=amplitude,
                raw_weight=raw_weight,
                trade_return=gross_return,
            )
            trades.append(trade)

    return trades


# ----------------------------------------------------------------------
# 回测框架
# ----------------------------------------------------------------------

def run_backtest(config: Dict) -> Dict[str, pd.DataFrame]:
    excel_path = config["excel_path"]
    assert os.path.exists(excel_path), f"未找到数据文件：{excel_path}"

    asset_map = config.get("asset_class_map", {})
    trades: List[Trade] = []

    for sheet, freq_cfg in config["frequencies"].items():
        prices = load_prices(sheet, excel_path)
        trades.extend(generate_trades(prices, freq_cfg, asset_map, config))

    if not trades:
        raise RuntimeError("未生成任何交易，请检查阈值或数据。")

    trades_df = pd.DataFrame(asdict(t) for t in trades)
    trades_df.sort_values(["entry_time", "symbol"], inplace=True)
    trades_df.reset_index(drop=True, inplace=True)

    gross_cap = config["portfolio"]["gross_cap"]
    cost_frac = config["portfolio"]["round_trip_cost_bps"] / 10000.0

    # 组合层总仓位约束
    trades_df["scaled_weight"] = trades_df["raw_weight"]
    trades_df["gross_leverage"] = 0.0

    for entry_time, idx in trades_df.groupby("entry_time").groups.items():
        group = trades_df.loc[idx]
        gross = group["raw_weight"].abs().sum()
        scale = 1.0 if gross <= gross_cap else (gross_cap / gross if gross > 0 else 1.0)
        trades_df.loc[idx, "scaled_weight"] = group["raw_weight"] * scale
        trades_df.loc[idx, "gross_leverage"] = gross * scale

    trades_df["signed_return"] = trades_df["trade_return"] * trades_df["direction"]
    trades_df["pnl"] = trades_df["scaled_weight"] * trades_df["signed_return"] - trades_df["scaled_weight"] * cost_frac

    # 权益曲线按 exit_time 聚合
    pnl_series = trades_df.groupby("exit_time")["pnl"].sum().sort_index()
    equity = (1.0 + pnl_series).cumprod()
    equity.name = "equity"

    # 回测指标
    summary_records = []
    total_return = float(equity.iloc[-1] - 1.0)
    summary_records.append({"metric": "Total Return", "value": total_return})

    if len(equity.index) > 1:
        total_days = (equity.index[-1] - equity.index[0]).days
        years = total_days / 365.25 if total_days > 0 else np.nan
    else:
        years = np.nan

    if years and years > 0:
        ann_return = float(equity.iloc[-1] ** (1.0 / years) - 1.0)
        summary_records.append({"metric": "Annualized Return", "value": ann_return})
    else:
        summary_records.append({"metric": "Annualized Return", "value": np.nan})

    periods_per_year = estimate_periods_per_year(pnl_series.index)
    if periods_per_year and periods_per_year > 0:
        mean_ret = pnl_series.mean()
        vol = pnl_series.std(ddof=0) * math.sqrt(periods_per_year)
        sharpe = (mean_ret * periods_per_year) / vol if vol > 0 else np.nan
        summary_records.append({"metric": "Annualized Volatility", "value": vol})
        summary_records.append({"metric": "Sharpe (approx)", "value": sharpe})
    else:
        summary_records.append({"metric": "Annualized Volatility", "value": np.nan})
        summary_records.append({"metric": "Sharpe (approx)", "value": np.nan})

    drawdown = (equity / equity.cummax() - 1.0).min()
    summary_records.append({"metric": "Max Drawdown", "value": float(drawdown)})

    win_rate = (trades_df["pnl"] > 0).mean()
    summary_records.append({"metric": "Win Rate", "value": float(win_rate)})
    summary_records.append({"metric": "Trades", "value": int(len(trades_df))})

    summary_df = pd.DataFrame(summary_records)

    by_asset = trades_df.groupby(["asset_class", "symbol"])["pnl"].sum().sort_values(ascending=False).to_frame("pnl")
    by_freq = trades_df.groupby("freq_label")["pnl"].sum().sort_values(ascending=False).to_frame("pnl")

    return {
        "summary": summary_df,
        "equity": equity.to_frame(),
        "trades": trades_df,
        "by_asset": by_asset,
        "by_freq": by_freq,
    }


# ----------------------------------------------------------------------
# 主入口
# ----------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="跨资产反趋势策略回测")
    parser.add_argument("--excel", default=None, help="Excel 数据路径，默认使用配置中的 excel_path")
    parser.add_argument("--config-json", default=None, help="JSON 配置文件，覆盖默认配置")
    parser.add_argument("--quiet", action="store_true", help="抑制控制台打印")
    parser.add_argument("--allow-short", action="store_true", help="允许做空（默认仅做多）")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    config = deep_update(DEFAULT_CONFIG.copy(), load_config_from_json(args.config_json))
    if args.excel:
        config["excel_path"] = args.excel
    if args.allow_short:
        config.setdefault("trading", {})["long_only"] = False

    results = run_backtest(config)
    out_cfg = config["output"]

    results["summary"].to_csv(out_cfg["summary_csv"], index=False)
    results["equity"].to_csv(out_cfg["equity_csv"])
    results["trades"].to_csv(out_cfg["trades_csv"], index=False)
    results["by_asset"].reset_index().to_csv(out_cfg["by_asset_csv"], index=False)
    results["by_freq"].reset_index().to_csv(out_cfg["by_freq_csv"], index=False)

    if not args.quiet:
        print("=== 回测指标 ===")
        print(results["summary"])
        print("\n=== 权益曲线末值 ===", float(results["equity"].iloc[-1]))
        print("\n=== 主要资产贡献 ===")
        print(results["by_asset"].head(10))
        print("\n=== 频段贡献 ===")
        print(results["by_freq"])


if __name__ == "__main__":
    main()
