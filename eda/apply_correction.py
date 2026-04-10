"""
apply_correction.py — Apply validation_1 correction factors to matched STAC data.

Correction model (per index): corrected_stac = slope * stac_promedio + intercept
Coefficients source: correction_factors.json -> validation_1 (trained on 2020-2024)

Output: eda/corrected_output.parquet
  Columns: lote, fecha, year
    + per index: {idx}_corrected, {idx}_ref, {idx}_diff

Run from project root or eda/:
    python eda/apply_correction.py
"""

import json
from pathlib import Path

import pandas as pd

from utils import INDICES, build_matched, load_reference, load_stac

EDA_DIR = Path(__file__).parent
FACTORS_PATH = EDA_DIR / "correction_factors.json"
OUTPUT_PATH = EDA_DIR / "corrected_output.csv"


def main() -> None:
    print("Loading correction factors (validation_1)...")
    with open(FACTORS_PATH) as f:
        factors = json.load(f)["validation_1"]
    for idx, coef in factors.items():
        print(f"  {idx.upper():<8} slope={coef['slope']:.4f}  intercept={coef['intercept']:.4f}")

    print("\nLoading data...")
    ref = load_reference()
    stac = load_stac()
    df = build_matched(ref, stac)
    print(f"  {len(df):,} matched rows")

    keep = ["lote", "fecha", "year"]
    for idx in INDICES:
        slope = factors[idx]["slope"]
        intercept = factors[idx]["intercept"]

        stac_col = f"{idx}_promedio_stac"
        ref_col = f"{idx}_promedio_ref"

        df[f"{idx}_corrected"] = slope * df[stac_col] + intercept
        df[f"{idx}_ref"] = df[ref_col]
        df[f"{idx}_diff"] = df[f"{idx}_corrected"] - df[ref_col]

        keep += [f"{idx}_corrected", f"{idx}_ref", f"{idx}_diff"]

    out = df[keep]
    out.to_csv(OUTPUT_PATH, index=False)
    print(f"\nSaved {len(out):,} rows to {OUTPUT_PATH}")

    print("\nDiff summary (corrected_stac - ref):")
    print(f"  {'Index':<10} {'Mean':>10} {'Std':>10} {'MAE':>10}")
    for idx in INDICES:
        diff = out[f"{idx}_diff"].dropna()
        print(f"  {idx.upper():<10} {diff.mean():>10.4f} {diff.std():>10.4f} {diff.abs().mean():>10.4f}")


if __name__ == "__main__":
    main()
