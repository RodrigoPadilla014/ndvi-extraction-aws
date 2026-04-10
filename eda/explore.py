"""
explore.py — Diagnostic analysis for STAC vs Reference correction factor.

Generates:
  plots/scatter.png       — STAC vs Reference scatter per index
  plots/residuals.png     — (STAC - Reference) distribution per index
  plots/year_bias.png     — per-year mean bias per index

Run from project root or eda/:
    python eda/explore.py
"""

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

from utils import INDICES, build_matched, load_reference, load_stac

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
EDA_DIR = Path(__file__).parent
PLOTS_DIR = EDA_DIR / "plots"
PLOTS_DIR.mkdir(exist_ok=True)

SCATTER_SAMPLE = 60_000  # rows to plot in scatter (full dataset is too dense)


# ---------------------------------------------------------------------------
# Plots
# ---------------------------------------------------------------------------

def plot_scatter(df: pd.DataFrame) -> None:
    """STAC vs Reference scatter per index, with identity line and sample."""
    sample = df.sample(min(SCATTER_SAMPLE, len(df)), random_state=42)

    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    fig.suptitle("STAC vs Reference — scatter (sampled)", fontsize=13)

    for ax, idx in zip(axes, INDICES):
        x = sample[f"{idx}_promedio_stac"]
        y = sample[f"{idx}_promedio_ref"]

        ax.scatter(x, y, s=2, alpha=0.3, color="steelblue", rasterized=True)

        # identity line
        lo = min(x.min(), y.min())
        hi = max(x.max(), y.max())
        ax.plot([lo, hi], [lo, hi], "r--", linewidth=1, label="y = x")

        # linear trend — drop NaN pairs together
        valid = pd.DataFrame({"x": x, "y": y}).dropna()
        m, b = np.polyfit(valid["x"], valid["y"], 1)
        ax.plot([lo, hi], [m * lo + b, m * hi + b], "k-", linewidth=1,
                label=f"fit: y={m:.3f}x+{b:.3f}")

        ax.set_xlabel(f"STAC {idx}_promedio")
        ax.set_ylabel(f"Ref {idx}_promedio")
        ax.set_title(idx.upper())
        ax.legend(fontsize=8)
        ax.set_aspect("equal", adjustable="box")

    plt.tight_layout()
    out = PLOTS_DIR / "scatter.png"
    plt.savefig(out, dpi=120)
    plt.close()
    print(f"  saved {out}")


def plot_residuals(df: pd.DataFrame) -> None:
    """Distribution of (STAC - Reference) residuals per index."""
    fig, axes = plt.subplots(1, 3, figsize=(15, 4))
    fig.suptitle("Residuals: STAC − Reference", fontsize=13)

    for ax, idx in zip(axes, INDICES):
        residuals = df[f"{idx}_promedio_stac"] - df[f"{idx}_promedio_ref"]
        mean, std = residuals.mean(), residuals.std()

        ax.hist(residuals.dropna(), bins=100, color="steelblue", edgecolor="none", alpha=0.8)
        ax.axvline(0, color="r", linestyle="--", linewidth=1, label="zero")
        ax.axvline(mean, color="k", linestyle="-", linewidth=1, label=f"mean={mean:.4f}")

        ax.set_title(f"{idx.upper()}  (σ={std:.4f})")
        ax.set_xlabel("STAC − Ref")
        ax.set_ylabel("Count")
        ax.legend(fontsize=8)
        ax.xaxis.set_major_formatter(mticker.FormatStrFormatter("%.3f"))

    plt.tight_layout()
    out = PLOTS_DIR / "residuals.png"
    plt.savefig(out, dpi=120)
    plt.close()
    print(f"  saved {out}")


def plot_year_bias(df: pd.DataFrame) -> None:
    """Per-year mean bias (STAC - Reference) per index — checks temporal stability."""
    fig, axes = plt.subplots(1, 3, figsize=(15, 4))
    fig.suptitle("Per-year mean bias: STAC − Reference", fontsize=13)

    for ax, idx in zip(axes, INDICES):
        residuals = df[f"{idx}_promedio_stac"] - df[f"{idx}_promedio_ref"]
        year_bias = (
            df.assign(residual=residuals)
            .groupby("year")["residual"]
            .agg(["mean", "std"])
            .reset_index()
        )

        ax.bar(year_bias["year"], year_bias["mean"], yerr=year_bias["std"],
               color="steelblue", alpha=0.8, capsize=4, error_kw={"linewidth": 1})
        ax.axhline(0, color="r", linestyle="--", linewidth=1)

        ax.set_title(idx.upper())
        ax.set_xlabel("Year")
        ax.set_ylabel("Mean bias (STAC − Ref)")
        ax.xaxis.set_major_locator(mticker.MultipleLocator(1))

    plt.tight_layout()
    out = PLOTS_DIR / "year_bias.png"
    plt.savefig(out, dpi=120)
    plt.close()
    print(f"  saved {out}")


def print_summary(df: pd.DataFrame) -> None:
    print(f"\n{'='*50}")
    print(f"Matched rows : {len(df):,}")
    print(f"Unique lots  : {df['lote'].nunique():,}")
    print(f"Unique dates : {df['fecha'].nunique():,}")
    print(f"Years        : {sorted(df['year'].unique())}")
    print(f"{'='*50}")

    for idx in INDICES:
        stac_col = f"{idx}_promedio_stac"
        ref_col = f"{idx}_promedio_ref"
        residuals = df[stac_col] - df[ref_col]
        corr = df[[stac_col, ref_col]].corr().iloc[0, 1]
        print(f"\n{idx.upper()}")
        print(f"  STAC   mean={df[stac_col].mean():.4f}  std={df[stac_col].std():.4f}")
        print(f"  Ref    mean={df[ref_col].mean():.4f}  std={df[ref_col].std():.4f}")
        print(f"  Bias   mean={residuals.mean():.4f}  std={residuals.std():.4f}")
        print(f"  Pearson r={corr:.4f}")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Loading reference data...")
    ref = load_reference()
    print(f"  {len(ref):,} rows")

    print("Loading STAC data...")
    stac = load_stac()
    print(f"  {len(stac):,} rows")

    print("Joining on lote + fecha...")
    df = build_matched(ref, stac)
    print(f"  {len(df):,} matched rows")

    print_summary(df)

    print("Generating plots...")
    plot_scatter(df)
    plot_residuals(df)
    plot_year_bias(df)

    print("\nDone. Plots saved to eda/plots/")
