"""
分析 contrarian_reversal_strategy.py 回测结果的辅助脚本。

读取 contrarian_* 系列 CSV，生成更丰富的统计指标，并将输出保存到
指定目录（默认 contrarian_analysis_results）。

用法示例：
    python contrarian_analysis.py
    python contrarian_analysis.py --input-dir . --output-dir my_report
"""

import argparse
import json
import math
import os
from datetime import datetime, timezone
from typing import Dict, Tuple

import numpy as np
import pandas as pd


SUMMARY_FILE = "contrarian_summary.csv"
TRADES_FILE = "contrarian_trades.csv"
EQUITY_FILE = "contrarian_equity_curve.csv"
BY_ASSET_FILE = "contrarian_by_asset.csv"
BY_FREQ_FILE = "contrarian_by_freq.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Contrarian 回测结果分析")
    parser.add_argument("--input-dir", default=".", help="输入 CSV 所在目录")
    parser.add_argument("--output-dir", default="contrarian_analysis_results", help="分析结果输出目录")
    parser.add_argument("--export-json", action="store_true", help="额外导出 summary.json")
    return parser.parse_args()


def ensure_directory(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def load_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"缺少输入文件: {path}")
    return pd.read_csv(path)


def compute_trade_metrics(trades: pd.DataFrame) -> Dict[str, float]:
    wins = trades["pnl"] > 0
    losses = trades["pnl"] < 0
    flat = trades["pnl"] == 0
    total = len(trades)
    win_count = int(wins.sum())
    loss_count = int(losses.sum())
    flat_count = int(flat.sum())

    win_sum = trades.loc[wins, "pnl"].sum()
    loss_sum = trades.loc[losses, "pnl"].sum()
    profit_factor = win_sum / abs(loss_sum) if loss_sum < 0 else math.inf

    avg_win = trades.loc[wins, "pnl"].mean() if win_count else 0.0
    avg_loss = trades.loc[losses, "pnl"].mean() if loss_count else 0.0
    median_pnl = trades["pnl"].median()
    mean_pnl = trades["pnl"].mean()
    std_pnl = trades["pnl"].std(ddof=0)
    skew = trades["pnl"].skew()
    kurt = trades["pnl"].kurtosis()

    signed = trades["signed_return"]
    return_ratio = signed.mean()
    return_std = signed.std(ddof=0)

    holding_days = trades["holding_days"]
    avg_hold_days = holding_days.mean()
    max_hold_days = holding_days.max()
    min_hold_days = holding_days.min()

    metrics = {
        "trades_total": total,
        "wins": win_count,
        "losses": loss_count,
        "flats": flat_count,
        "win_rate": win_count / total if total else np.nan,
        "loss_rate": loss_count / total if total else np.nan,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "median_pnl": median_pnl,
        "mean_pnl": mean_pnl,
        "std_pnl": std_pnl,
        "skew_pnl": skew,
        "kurtosis_pnl": kurt,
        "profit_factor": profit_factor,
        "mean_signed_return": return_ratio,
        "std_signed_return": return_std,
        "avg_holding_days": avg_hold_days,
        "min_holding_days": min_hold_days,
        "max_holding_days": max_hold_days,
    }
    return metrics


def compute_streak(series: pd.Series, positive: bool = True) -> Tuple[int, int]:
    target = series > 0 if positive else series < 0
    best = current = 0
    for flag in target:
        if flag:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best, target.sum()


def add_holding_days(trades: pd.DataFrame) -> None:
    trades["entry_time"] = pd.to_datetime(trades["entry_time"])
    trades["exit_time"] = pd.to_datetime(trades["exit_time"])
    trades["holding_days"] = (trades["exit_time"] - trades["entry_time"]).dt.days


def pnl_histogram(trades: pd.DataFrame, bins: int = 30) -> pd.DataFrame:
    hist, edges = np.histogram(trades["pnl"], bins=bins)
    left = edges[:-1]
    right = edges[1:]
    centers = (left + right) / 2.0
    return pd.DataFrame({"bin_left": left, "bin_right": right, "bin_center": centers, "count": hist})


def drawdown_series(equity: pd.Series) -> pd.DataFrame:
    peak = equity.cummax()
    drawdown = equity / peak - 1.0
    return pd.DataFrame({"equity": equity, "drawdown": drawdown})


def compile_summary(summary_df: pd.DataFrame, trade_metrics: Dict[str, float], drawdowns: pd.DataFrame) -> pd.DataFrame:
    latest_dd = float(drawdowns["drawdown"].iloc[-1])
    max_dd = float(drawdowns["drawdown"].min())
    dd_below_5 = (drawdowns["drawdown"] < -0.05).sum()

    records = summary_df.copy()
    extra = pd.DataFrame([
        {"metric": "Mean Trade PnL", "value": trade_metrics["mean_pnl"]},
        {"metric": "Median Trade PnL", "value": trade_metrics["median_pnl"]},
        {"metric": "Average Holding Days", "value": trade_metrics["avg_holding_days"]},
        {"metric": "Max Holding Days", "value": trade_metrics["max_holding_days"]},
        {"metric": "Min Holding Days", "value": trade_metrics["min_holding_days"]},
        {"metric": "Max Drawdown (recalc)", "value": max_dd},
        {"metric": "Current Drawdown", "value": latest_dd},
        {"metric": "Days Below -5pct DD", "value": dd_below_5},
        {"metric": "Profit Factor", "value": trade_metrics["profit_factor"]},
        {"metric": "Pnl StdDev", "value": trade_metrics["std_pnl"]},
        {"metric": "Pnl Skewness", "value": trade_metrics["skew_pnl"]},
        {"metric": "Pnl Kurtosis", "value": trade_metrics["kurtosis_pnl"]},
    ])
    return pd.concat([records, extra], ignore_index=True)


def write_report(path: str, summary: pd.DataFrame, trade_metrics: Dict[str, float], streaks: Dict[str, int],
                 top_assets: pd.DataFrame, yearly: pd.DataFrame) -> None:
    lines = []
    summary_map = {row["metric"]: row["value"] for _, row in summary.iterrows()}
    lines.append("CONTRARIAN STRATEGY ANALYSIS REPORT")
    lines.append("=" * 40)
    lines.append(f"Generated at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("")
    lines.append("Performance Overview")
    lines.append("--------------------")
    lines.append(f"Total Return: {summary_map.get('Total Return', float('nan')):.4f}")
    lines.append(f"Annualized Return: {summary_map.get('Annualized Return', float('nan')):.4f}")
    lines.append(f"Annualized Volatility: {summary_map.get('Annualized Volatility', float('nan')):.4f}")
    lines.append(f"Sharpe (approx): {summary_map.get('Sharpe (approx)', float('nan')):.4f}")
    lines.append(f"Max Drawdown (recalc): {summary_map.get('Max Drawdown (recalc)', float('nan')):.4f}")
    lines.append("")

    lines.append("Trade Diagnostics")
    lines.append("-----------------")
    lines.append(f"Trades: {trade_metrics['trades_total']}")
    lines.append(f"Win Rate: {trade_metrics['win_rate']:.2%}")
    lines.append(f"Average Win: {trade_metrics['avg_win']:.4f}")
    lines.append(f"Average Loss: {trade_metrics['avg_loss']:.4f}")
    lines.append(f"Profit Factor: {trade_metrics['profit_factor']:.4f}")
    lines.append(f"Max Consecutive Wins: {streaks['max_wins']}")
    lines.append(f"Max Consecutive Losses: {streaks['max_losses']}")
    lines.append(f"Average Holding Days: {trade_metrics['avg_holding_days']:.2f}")
    lines.append("")

    lines.append("Top Asset Contributors (PnL)")
    lines.append("-----------------------------")
    for _, row in top_assets.head(10).iterrows():
        lines.append(f"{row['symbol']}: {row['pnl']:.4f}")
    lines.append("")

    lines.append("PnL by Calendar Year")
    lines.append("--------------------")
    for _, row in yearly.iterrows():
        lines.append(f"{int(row['year'])}: {row['pnl']:.4f}")
    lines.append("")

    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))


def main() -> None:
    args = parse_args()
    input_dir = os.path.abspath(args.input_dir)
    output_dir = os.path.abspath(args.output_dir)
    ensure_directory(output_dir)

    summary_path = os.path.join(input_dir, SUMMARY_FILE)
    trades_path = os.path.join(input_dir, TRADES_FILE)
    equity_path = os.path.join(input_dir, EQUITY_FILE)
    by_asset_path = os.path.join(input_dir, BY_ASSET_FILE)
    by_freq_path = os.path.join(input_dir, BY_FREQ_FILE)

    summary_df = load_csv(summary_path)
    trades_df = load_csv(trades_path)
    equity_df = load_csv(equity_path)
    by_asset_df = load_csv(by_asset_path)
    by_freq_df = load_csv(by_freq_path)

    add_holding_days(trades_df)

    trade_metrics = compute_trade_metrics(trades_df)
    win_streak, _ = compute_streak(trades_df["pnl"], positive=True)
    loss_streak, _ = compute_streak(trades_df["pnl"], positive=False)
    streaks = {"max_wins": win_streak, "max_losses": loss_streak}

    hist_df = pnl_histogram(trades_df)
    if "time" in equity_df.columns:
        idx_col = "time"
    elif "exit_time" in equity_df.columns:
        idx_col = "exit_time"
    else:
        idx_col = equity_df.columns[0]
    value_col = "equity" if "equity" in equity_df.columns else equity_df.columns[-1]
    equity_series = equity_df.set_index(idx_col)[value_col]
    dd_df = drawdown_series(equity_series)
    combined_summary = compile_summary(summary_df, trade_metrics, dd_df)

    trades_df.to_csv(os.path.join(output_dir, "trades_with_holding_days.csv"), index=False)
    hist_df.to_csv(os.path.join(output_dir, "pnl_histogram.csv"), index=False)
    dd_df.to_csv(os.path.join(output_dir, "equity_drawdown.csv"))
    combined_summary.to_csv(os.path.join(output_dir, "analysis_summary.csv"), index=False)

    yearly_pnl = trades_df.copy()
    yearly_pnl["year"] = yearly_pnl["exit_time"].dt.year
    yearly_pnl = yearly_pnl.groupby("year")["pnl"].sum().reset_index()
    yearly_pnl.to_csv(os.path.join(output_dir, "pnl_by_year.csv"), index=False)

    class_summary = trades_df.groupby("asset_class")["pnl"].agg(["sum", "count", "mean"]).reset_index()
    class_summary.to_csv(os.path.join(output_dir, "pnl_by_asset_class.csv"), index=False)
    by_freq_df.to_csv(os.path.join(output_dir, "pnl_by_frequency.csv"), index=False)
    by_asset_df.to_csv(os.path.join(output_dir, "pnl_by_asset.csv"), index=False)

    report_path = os.path.join(output_dir, "analysis_report.txt")
    write_report(report_path, combined_summary, trade_metrics, streaks, by_asset_df, yearly_pnl)

    if args.export_json:
        summary_map = {row["metric"]: row["value"] for _, row in combined_summary.iterrows()}
        payload = {
            "summary": summary_map,
            "trade_metrics": trade_metrics,
            "streaks": streaks,
        }
        json_path = os.path.join(output_dir, "analysis_summary.json")
        with open(json_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
