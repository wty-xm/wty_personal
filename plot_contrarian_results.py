import os
from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib import font_manager

# 中文字体：明确指定已安装的 Noto Sans CJK 路径，强制注册后使用
FONT_PATH = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"
FONT_PROPERTIES = None
if Path(FONT_PATH).exists():
    font_manager.fontManager.addfont(FONT_PATH)
    FONT_PROPERTIES = font_manager.FontProperties(fname=FONT_PATH)
    font_name = FONT_PROPERTIES.get_name()
    mpl.rcParams["font.family"] = font_name
    mpl.rcParams["font.sans-serif"] = [font_name]
else:
    mpl.rcParams["font.family"] = "sans-serif"
    mpl.rcParams["font.sans-serif"] = [
        "Noto Sans CJK SC",
        "Noto Sans CJK JP",
        "WenQuanYi Zen Hei",
        "DejaVu Sans",
    ]
mpl.rcParams["axes.unicode_minus"] = False  # 避免负号变方块

OUTPUT_DIR = Path("contrarian_reversal_strategy_result")
OUTPUT_DIR.mkdir(exist_ok=True)
sns.set_theme(style="whitegrid")

def apply_cn_font(ax: plt.Axes) -> None:
    """确保坐标轴/标题/图例使用中文字体"""
    if FONT_PROPERTIES is None:
        return
    ax.set_title(ax.get_title(), fontproperties=FONT_PROPERTIES)
    ax.set_xlabel(ax.get_xlabel(), fontproperties=FONT_PROPERTIES)
    ax.set_ylabel(ax.get_ylabel(), fontproperties=FONT_PROPERTIES)
    for label in ax.get_xticklabels():
        label.set_fontproperties(FONT_PROPERTIES)
    for label in ax.get_yticklabels():
        label.set_fontproperties(FONT_PROPERTIES)
    legend = ax.get_legend()
    if legend:
        for text in legend.get_texts():
            text.set_fontproperties(FONT_PROPERTIES)


def plot_equity_curve(path: str) -> None:
    df = pd.read_csv(path, parse_dates=True, index_col=0)
    df.columns = ["equity"]
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(df.index, df["equity"], color="#1f77b4", linewidth=1.4)
    ax.set_title("Equity Curve")
    ax.set_xlabel("Date")
    ax.set_ylabel("Equity")
    apply_cn_font(ax)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "equity_curve.png", dpi=200)
    plt.close(fig)


def plot_freq_contrib(path: str) -> None:
    df = pd.read_csv(path)
    fig, ax = plt.subplots(figsize=(6, 4))
    sns.barplot(data=df, x="freq_label", y="pnl", hue="freq_label", palette="Blues_d", legend=False, ax=ax)
    ax.set_title("PNL by Frequency")
    ax.set_xlabel("Frequency")
    ax.set_ylabel("PNL")
    apply_cn_font(ax)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "pnl_by_frequency.png", dpi=200)
    plt.close(fig)


def plot_asset_contrib(path: str, top_n: int = 10) -> None:
    df = pd.read_csv(path)
    df_sorted = df.sort_values("pnl", ascending=False)
    top = df_sorted.head(top_n)
    bottom = df_sorted.tail(top_n)

    fig, ax = plt.subplots(figsize=(8, 6))
    sns.barplot(data=top, y="symbol", x="pnl", hue="asset_class", dodge=False, palette="crest", ax=ax)
    ax.set_title(f"Top {top_n} Assets by PNL")
    ax.set_xlabel("PNL")
    ax.set_ylabel("Symbol")
    ax.legend(title="Asset Class")
    apply_cn_font(ax)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "pnl_top_assets.png", dpi=200)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8, 6))
    sns.barplot(data=bottom, y="symbol", x="pnl", hue="asset_class", dodge=False, palette="flare", ax=ax)
    ax.set_title(f"Bottom {top_n} Assets by PNL")
    ax.set_xlabel("PNL")
    ax.set_ylabel("Symbol")
    ax.legend(title="Asset Class")
    apply_cn_font(ax)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "pnl_bottom_assets.png", dpi=200)
    plt.close(fig)


def plot_trade_pnl(trades_path: str) -> None:
    trades = pd.read_csv(trades_path, parse_dates=["entry_time", "exit_time"])
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.histplot(trades["pnl"], bins=30, kde=True, color="#2ca02c", ax=ax)
    ax.set_title("Trade PNL Distribution")
    ax.set_xlabel("PNL")
    ax.set_ylabel("Count")
    apply_cn_font(ax)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "trade_pnl_distribution.png", dpi=200)
    plt.close(fig)


def plot_streaks(trades_path: str) -> None:
    trades = pd.read_csv(trades_path)
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.countplot(x="streak_len", data=trades, color="#ff7f0e", ax=ax)
    ax.set_title("Signal Streak Lengths")
    ax.set_xlabel("Streak Length")
    ax.set_ylabel("Count")
    apply_cn_font(ax)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "streak_lengths.png", dpi=200)
    plt.close(fig)


def plot_gross_leverage(trades_path: str) -> None:
    trades = pd.read_csv(trades_path, parse_dates=["entry_time"])
    daily = trades.groupby(trades["entry_time"])["gross_leverage"].sum().reset_index()
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(daily["entry_time"], daily["gross_leverage"], color="#9467bd", linewidth=1.2)
    ax.set_title("Gross Leverage by Entry Date")
    ax.set_xlabel("Entry Date")
    ax.set_ylabel("Gross Leverage")
    apply_cn_font(ax)
    fig.tight_layout()
    fig.savefig(OUTPUT_DIR / "gross_leverage.png", dpi=200)
    plt.close(fig)


def main() -> None:
    files = {
        "equity": "contrarian_equity_curve.csv",
        "by_freq": "contrarian_by_freq.csv",
        "by_asset": "contrarian_by_asset.csv",
        "trades": "contrarian_trades.csv",
    }
    missing = [f for f in files.values() if not os.path.exists(f)]
    if missing:
        raise FileNotFoundError(f"Missing files: {missing}")

    plot_equity_curve(files["equity"])
    plot_freq_contrib(files["by_freq"])
    plot_asset_contrib(files["by_asset"], top_n=10)
    plot_trade_pnl(files["trades"])
    plot_streaks(files["trades"])
    plot_gross_leverage(files["trades"])
    print(f"Charts saved to {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
