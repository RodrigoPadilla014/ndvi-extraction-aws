"""
utils.py — Shared data loading and cleaning for explore.py and fit_correction.py
"""

from pathlib import Path
import numpy as np
import pandas as pd

EDA_DIR = Path(__file__).parent
DATA_DIR = EDA_DIR / "data"

YEARS = [2020, 2021, 2022, 2023, 2024, 2025]
INDICES = ["ndvi", "ndwi11", "msi11"]

# Valid physical ranges per index — values outside are treated as no-data
INDEX_VALID_RANGE = {
    "ndvi":   (-1.0,  1.0),
    "ndwi11": (-1.0,  1.0),
    "msi11":  ( 0.0, 10.0),
}

INDEX_COLS = [f"{idx}_promedio" for idx in INDICES]


def clean_index_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Replace inf, -inf, and out-of-range values with NaN."""
    for idx, (lo, hi) in INDEX_VALID_RANGE.items():
        col = f"{idx}_promedio"
        df[col] = df[col].replace([np.inf, -np.inf], np.nan)
        df.loc[~df[col].between(lo, hi), col] = np.nan
    return df


def load_reference() -> pd.DataFrame:
    frames = []
    for year in YEARS:
        path = DATA_DIR / "reference" / str(year) / f"{year}_indices_ref.parquet"
        df = pd.read_parquet(path, columns=["lote", "fecha"] + INDEX_COLS)
        df = clean_index_columns(df)
        df["year"] = year
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def load_stac() -> pd.DataFrame:
    frames = []
    for year in YEARS:
        folder = DATA_DIR / "stac" / str(year)
        for path in sorted(folder.glob("*.parquet")):
            df = pd.read_parquet(path, columns=["lote", "fecha", "imagen_id"] + INDEX_COLS)
            df = clean_index_columns(df)
            frames.append(df)
    combined = pd.concat(frames, ignore_index=True)
    combined["year"] = pd.to_datetime(combined["fecha"]).dt.year
    return combined


def build_matched(ref: pd.DataFrame, stac: pd.DataFrame) -> pd.DataFrame:
    merged = ref.merge(
        stac,
        on=["lote", "fecha"],
        suffixes=("_ref", "_stac"),
        how="inner",
    )
    merged["year"] = merged["year_ref"]
    merged.drop(columns=["year_ref", "year_stac"], inplace=True)
    return merged
