"""
fit_correction.py — Fit per-index linear correction factors: Reference = a * STAC + b

Models:
  NDVI    — OLS (LinearRegression)
  NDWI-11 — OLS (LinearRegression)
  MSI-11  — Huber (robust to outliers)

Validation 1 — Holdout 2025:
  Train on 2020–2024, test on 2025.
  Gives a single out-of-sample score for the most recent year.

  Results:
    NDVI    train R²=0.9486  train MAE=0.0246  test R²=0.9660  test MAE=0.0243
    NDWI-11 train R²=0.9798  train MAE=0.0166  test R²=0.8981  test MAE=0.0229
    MSI-11  train R²=0.9611  train MAE=0.0287  test R²=0.8564  test MAE=0.0400

Validation 2 — Leave-one-year-out CV:
  Each year takes a turn as the test set, trained on the remaining 5.
  Gives a more robust estimate of generalization across all years.

  Results (mean across folds):
    NDVI    mean R²=0.9501  std=0.0496  mean MAE=0.0242
    NDWI-11 mean R²=0.9578  std=0.0331  mean MAE=0.0179
    MSI-11  mean R²=0.8935  std=0.1197  mean MAE=0.0307

Final fit:
  Trained on all 6 years (2020–2025) combined.
  These are the coefficients written to correction_factors.json.

    NDVI    slope=0.8335  intercept=0.0051
    NDWI-11 slope=0.8925  intercept=0.1028
    MSI-11  slope=0.7453  intercept=0.0615

Outputs:
  correction_factors.json — slopes and intercepts ready to plug into the pipeline

Run from project root or eda/:
    python eda/fit_correction.py
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import HuberRegressor, LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score

from utils import INDICES, YEARS, build_matched, load_reference, load_stac

EDA_DIR = Path(__file__).parent
OUTPUT_PATH = EDA_DIR / "correction_factors.json"

# Model to use per index
INDEX_MODELS = {
    "ndvi":   LinearRegression,
    "ndwi11": LinearRegression,
    "msi11":  HuberRegressor,
}


# ---------------------------------------------------------------------------
# Cross-validation
# ---------------------------------------------------------------------------

def cross_validate(df: pd.DataFrame, idx: str) -> dict:
    """Leave-one-year-out CV. Returns per-fold metrics and mean/std summary."""
    stac_col = f"{idx}_promedio_stac"
    ref_col = f"{idx}_promedio_ref"

    clean = df[[stac_col, ref_col, "year"]].dropna()
    ModelClass = INDEX_MODELS[idx]

    folds = []
    for test_year in YEARS:
        train = clean[clean["year"] != test_year]
        test = clean[clean["year"] == test_year]

        model = ModelClass()
        model.fit(train[[stac_col]].values, train[ref_col].values)
        pred = model.predict(test[[stac_col]].values)

        folds.append({
            "year": test_year,
            "n": len(test),
            "r2": float(r2_score(test[ref_col].values, pred)),
            "mae": float(mean_absolute_error(test[ref_col].values, pred)),
        })

    r2s = [f["r2"] for f in folds]
    maes = [f["mae"] for f in folds]

    return {
        "folds": folds,
        "r2_mean": float(np.mean(r2s)),
        "r2_std": float(np.std(r2s)),
        "mae_mean": float(np.mean(maes)),
        "mae_std": float(np.std(maes)),
    }


# ---------------------------------------------------------------------------
# Train on 2020–2024, test on 2025
# ---------------------------------------------------------------------------

def fit_holdout_2025(df: pd.DataFrame, idx: str) -> dict:
    """Train on 2020–2024, test on 2025."""
    stac_col = f"{idx}_promedio_stac"
    ref_col = f"{idx}_promedio_ref"

    clean = df[[stac_col, ref_col, "year"]].dropna()
    train = clean[clean["year"] != 2025]
    test = clean[clean["year"] == 2025]

    ModelClass = INDEX_MODELS[idx]
    model = ModelClass()
    model.fit(train[[stac_col]].values, train[ref_col].values)

    train_pred = model.predict(train[[stac_col]].values)
    test_pred = model.predict(test[[stac_col]].values)

    return {
        "slope": float(model.coef_[0]),
        "intercept": float(model.intercept_),
        "train": {
            "n": len(train),
            "r2": float(r2_score(train[ref_col].values, train_pred)),
            "mae": float(mean_absolute_error(train[ref_col].values, train_pred)),
        },
        "test": {
            "n": len(test),
            "r2": float(r2_score(test[ref_col].values, test_pred)),
            "mae": float(mean_absolute_error(test[ref_col].values, test_pred)),
        },
    }


# ---------------------------------------------------------------------------
# Final fit on all years
# ---------------------------------------------------------------------------

def fit_final(df: pd.DataFrame, idx: str) -> dict:
    stac_col = f"{idx}_promedio_stac"
    ref_col = f"{idx}_promedio_ref"

    clean = df[[stac_col, ref_col]].dropna()
    ModelClass = INDEX_MODELS[idx]

    model = ModelClass()
    model.fit(clean[[stac_col]].values, clean[ref_col].values)

    return {
        "slope": float(model.coef_[0]),
        "intercept": float(model.intercept_),
    }


# ---------------------------------------------------------------------------
# Printing
# ---------------------------------------------------------------------------

def print_cv_results(cv_results: dict) -> None:
    print(f"\n{'='*65}  LEAVE-ONE-YEAR-OUT CV")
    print(f"{'Index':<10} {'Year':>6} {'N':>8}  {'R²':>8} {'MAE':>8}")
    print(f"{'-'*65}")
    for idx, cv in cv_results.items():
        for fold in cv["folds"]:
            print(
                f"{idx.upper():<10} {fold['year']:>6} {fold['n']:>8}  "
                f"{fold['r2']:>8.4f} {fold['mae']:>8.4f}"
            )
        print(
            f"{'':10} {'mean':>6} {'':>8}  "
            f"{cv['r2_mean']:>8.4f} {cv['mae_mean']:>8.4f}"
            f"  (std r2={cv['r2_std']:.4f}, mae={cv['mae_std']:.4f})"
        )
        print()
    print(f"{'='*65}\n")


def print_holdout_results(holdout: dict) -> None:
    print(f"\n{'='*65}  HOLDOUT 2025 (train: 2020–2024)")
    print(f"{'Index':<10} {'Model':<8} {'Slope':>8} {'Intercept':>10}  {'Train R²':>9} {'Train MAE':>10}  {'Test R²':>8} {'Test MAE':>9}")
    print(f"{'-'*65}")
    for idx, r in holdout.items():
        model_name = "Huber" if INDEX_MODELS[idx] == HuberRegressor else "OLS"
        print(
            f"{idx.upper():<10} {model_name:<8} "
            f"{r['slope']:>8.4f} {r['intercept']:>10.4f}  "
            f"{r['train']['r2']:>9.4f} {r['train']['mae']:>10.4f}  "
            f"{r['test']['r2']:>8.4f} {r['test']['mae']:>9.4f}"
        )
    print(f"{'='*65}\n")


def print_final_coefficients(factors: dict) -> None:
    print(f"{'='*40}  FINAL COEFFICIENTS (all years)")
    print(f"{'Index':<10} {'Model':<8} {'Slope':>8} {'Intercept':>10}")
    print(f"{'-'*40}")
    for idx, coef in factors.items():
        model_name = "Huber" if INDEX_MODELS[idx] == HuberRegressor else "OLS"
        print(f"{idx.upper():<10} {model_name:<8} {coef['slope']:>8.4f} {coef['intercept']:>10.4f}")
    print(f"{'='*40}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Loading data...")
    ref = load_reference()
    stac = load_stac()
    df = build_matched(ref, stac)
    print(f"  {len(df):,} matched rows  |  {len(YEARS)} years: {YEARS}")

    print("\nRunning holdout 2025 (train: 2020–2024)...")
    holdout = {idx: fit_holdout_2025(df, idx) for idx in INDICES}
    print_holdout_results(holdout)

    print("Running leave-one-year-out cross-validation...")
    cv_results = {idx: cross_validate(df, idx) for idx in INDICES}
    print_cv_results(cv_results)

    print("Fitting final models on all years...")
    factors = {idx: fit_final(df, idx) for idx in INDICES}
    print_final_coefficients(factors)

    output = {
        "final": {idx: {"slope": factors[idx]["slope"], "intercept": factors[idx]["intercept"]} for idx in INDICES},
        "validation_1": {idx: {"slope": holdout[idx]["slope"], "intercept": holdout[idx]["intercept"]} for idx in INDICES},
    }
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)
    print(f"Correction factors saved to {OUTPUT_PATH}")
