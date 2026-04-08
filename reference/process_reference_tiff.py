# -*- coding: utf-8 -*-
"""
process_reference_tiff.py — AWS Batch entry point (reference pipeline)

Processes pre-computed index TIFFs for one year from S3:
    Reference/{YEAR}-raster/{subfolder}/01-NDVI_*.tif   + .tfw
    Reference/{YEAR}-raster/{subfolder}/02-NDWI-11_*.tif + .tfw
    Reference/{YEAR}-raster/{subfolder}/04-MSI-11_*.tif  + .tfw

Matches TIFFs with shapefiles from STAC/{YEAR}/ by date (YYYYMMDD in filename).
Extracts zonal stats per lot, saves one Parquet per year, uploads to S3.
One Batch job per year.

Environment variables (set by Batch at submission time):
    YEAR       — year to process  (e.g. "2020")
    S3_BUCKET  — bucket name      (e.g. ndvi-extraction)
    S3_PREFIX  — output prefix    (default: output/reference)

Formulas:
    NDVI    = (NIR - RED)    / (NIR + RED)
    NDWI-11 = (NIR - SWIR16) / (NIR + SWIR16)
    MSI-11  = SWIR16 / NIR
"""

import os
import re
import sys
import time
import logging
import warnings
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from affine import Affine
from datetime import datetime
from pathlib import Path
import boto3
from botocore.exceptions import BotoCoreError, ClientError
import psutil
from rasterstats import zonal_stats

warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s — %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────
YEAR      = os.environ.get('YEAR')
S3_BUCKET = os.environ.get('S3_BUCKET')
S3_PREFIX = os.environ.get('S3_PREFIX', 'output/reference')

LOTE_FIELD = "COD_CG"

WORK_DIR       = Path('/tmp/ndvi_ref')
SHAPEFILES_DIR = WORK_DIR / 'shapefiles'
TIFFS_DIR      = WORK_DIR / 'tiffs'

COLS = [
    'lote', 'fecha', 'imagen_id',
    'ndvi_promedio',   'ndvi_max',   'ndvi_min',   'ndvi_std',
    'ndwi11_promedio', 'ndwi11_max', 'ndwi11_min', 'ndwi11_std',
    'msi11_promedio',  'msi11_max',  'msi11_min',  'msi11_std',
]


# ── Metrics ───────────────────────────────────────────────────────────────────
def log_metrics(label=""):
    mem  = psutil.virtual_memory()
    cpu  = psutil.cpu_percent(interval=1)
    used = mem.used  / 1024**3
    total= mem.total / 1024**3
    tag  = f"[{label}] " if label else ""
    log.info(f"[METRICS] {tag}RAM: {used:.2f}/{total:.2f} GB ({mem.percent:.1f}%) — CPU: {cpu:.1f}%")


# ── S3 helpers ────────────────────────────────────────────────────────────────
def s3_list(prefix):
    s3        = boto3.client('s3')
    keys      = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            keys.append(obj['Key'])
    return sorted(keys)


def s3_download(s3_key, local_path):
    try:
        boto3.client('s3').download_file(S3_BUCKET, s3_key, str(local_path))
        log.info(f"[S3] Downloaded: {Path(s3_key).name}")
        return True
    except (BotoCoreError, ClientError) as e:
        log.error(f"[S3] Download failed for {s3_key}: {e}")
        return False


def s3_upload(local_path, s3_key):
    try:
        boto3.client('s3').upload_file(str(local_path), S3_BUCKET, s3_key)
        log.info(f"[S3] Uploaded → s3://{S3_BUCKET}/{s3_key}")
    except (BotoCoreError, ClientError) as e:
        log.error(f"[S3] Upload failed for {Path(local_path).name}: {e}")


# ── Date extraction ───────────────────────────────────────────────────────────
def extract_date(filename):
    match = re.search(r'(\d{8})', filename)
    return match.group(1) if match else None


# ── TFW reader ────────────────────────────────────────────────────────────────
def read_tfw(tiff_path):
    """
    Read the .tfw world file and return the correct Affine transform.
    Needed because some TIFFs have a broken internal geotransform.
    Falls back to None if no .tfw exists; rasterio's own transform is used instead.
    """
    tfw_path = Path(tiff_path).with_suffix('.tfw')
    if not tfw_path.exists():
        return None
    vals = [float(l.strip()) for l in tfw_path.read_text().splitlines() if l.strip()]
    if len(vals) < 6:
        return None
    A, D, B, E, C, F = vals
    return Affine(A, B, C, D, E, F)


# ── Zonal stats for one TIFF ──────────────────────────────────────────────────
def zonal_stats_from_tiff(gdf, tiff_path):
    with rasterio.open(str(tiff_path)) as src:
        arr       = src.read(1)
        transform = read_tfw(tiff_path) or src.transform
        gdf_r     = gdf.to_crs(src.crs)
        return zonal_stats(
            gdf_r.geometry.tolist(),
            arr,
            affine=transform,
            stats=['count', 'mean', 'max', 'min', 'std'],
            nodata=np.nan,
        ), gdf_r


# ── Accumulate + finalize ─────────────────────────────────────────────────────
def _accumulate_stats(stats_list, gdf_r):
    lot_data = {}
    for (idx, fila), stat in zip(gdf_r.iterrows(), stats_list):
        cod_cg = fila.get(LOTE_FIELD, str(idx))
        count  = stat.get('count') or 0
        if count == 0 or stat.get('mean') is None:
            if cod_cg not in lot_data:
                lot_data[cod_cg] = {'count': 0, 'sum': 0.0, 'sum_sq': 0.0, 'max': None, 'min': None}
            continue
        mean = stat['mean']
        std  = stat.get('std') or 0.0
        if cod_cg not in lot_data or lot_data[cod_cg]['count'] == 0:
            lot_data[cod_cg] = {
                'count':  count,
                'sum':    mean * count,
                'sum_sq': (std**2 + mean**2) * count,
                'max':    stat['max'],
                'min':    stat['min'],
            }
        else:
            d = lot_data[cod_cg]
            d['count']  += count
            d['sum']    += mean * count
            d['sum_sq'] += (std**2 + mean**2) * count
            d['max']     = max(d['max'], stat['max'])
            d['min']     = min(d['min'], stat['min'])
    return lot_data


def _finalize(lot_data, prefix):
    out = {}
    for cod_cg, d in lot_data.items():
        n = d['count']
        if n == 0:
            out[cod_cg] = {f'{prefix}_promedio': None, f'{prefix}_max': None,
                           f'{prefix}_min': None,      f'{prefix}_std': None}
        else:
            mean = d['sum'] / n
            var  = max(0.0, d['sum_sq'] / n - mean**2)
            out[cod_cg] = {
                f'{prefix}_promedio': round(mean, 4),
                f'{prefix}_max':      round(d['max'], 4),
                f'{prefix}_min':      round(d['min'], 4),
                f'{prefix}_std':      round(var**0.5, 4),
            }
    return out


# ── Download TIF + companion TFW ──────────────────────────────────────────────
def download_tif_pair(s3_key):
    """Download a TIFF and its .tfw world file (if present) to TIFFS_DIR."""
    tif_name  = Path(s3_key).name
    local     = TIFFS_DIR / tif_name
    tfw_s3    = s3_key[:-4] + '.tfw'
    tfw_local = local.with_suffix('.tfw')

    if not s3_download(s3_key, local):
        return None

    # TFW is optional — silently skip if not present
    try:
        boto3.client('s3').download_file(S3_BUCKET, tfw_s3, str(tfw_local))
        log.info(f"[S3] Downloaded: {tfw_local.name}")
    except ClientError:
        pass

    return local


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':

    if not YEAR:
        log.error("YEAR environment variable not set. Exiting.")
        sys.exit(1)
    if not S3_BUCKET:
        log.error("S3_BUCKET environment variable not set. Exiting.")
        sys.exit(1)

    WORK_DIR.mkdir(parents=True, exist_ok=True)
    SHAPEFILES_DIR.mkdir(parents=True, exist_ok=True)
    TIFFS_DIR.mkdir(parents=True, exist_ok=True)

    t_inicio = time.time()
    log.info(f"=== Starting reference pipeline for year {YEAR} ===")
    log_metrics("start")

    # ─ List TIFFs from S3: Reference/{YEAR}-raster/
    ref_prefix = f"Reference/{YEAR}-raster/"
    all_keys   = s3_list(ref_prefix)
    log.info(f"Files found under {ref_prefix}: {len(all_keys)}")

    ndvi_by_date   = {}
    ndwi11_by_date = {}
    msi11_by_date  = {}

    for key in all_keys:
        name = Path(key).name
        if not name.endswith('.tif'):
            continue
        date = extract_date(name)
        if not date:
            continue
        if '01-NDVI_' in name:
            ndvi_by_date[date] = key
        elif '02-NDWI-11_' in name:
            ndwi11_by_date[date] = key
        elif '04-MSI-11_' in name:
            msi11_by_date[date] = key

    log.info(f"TIFFs — NDVI: {len(ndvi_by_date)}, NDWI-11: {len(ndwi11_by_date)}, MSI-11: {len(msi11_by_date)}")

    # ─ List shapefiles from S3: STAC/{YEAR}/
    shp_prefix  = f"STAC/{YEAR}/"
    shp_by_date = {}
    for key in s3_list(shp_prefix):
        if not key.endswith('.shp'):
            continue
        date = extract_date(Path(key).name)
        if date:
            shp_by_date[date] = key

    log.info(f"Shapefiles found: {len(shp_by_date)}")

    # ─ Match dates: need all three TIFFs + a shapefile
    matching = sorted(
        set(ndvi_by_date) & set(ndwi11_by_date) & set(msi11_by_date) & set(shp_by_date)
    )
    log.info(f"Complete matching dates: {len(matching)}")

    skipped = (set(ndvi_by_date) | set(ndwi11_by_date) | set(msi11_by_date)) - set(matching)
    if skipped:
        log.warning(f"Dates skipped (incomplete TIFFs or no shapefile): {sorted(skipped)}")

    if not matching:
        log.warning("No complete date sets found. Exiting.")
        sys.exit(0)

    # ─ Pre-download all unique shapefiles (one SHP may cover multiple dates)
    s3 = boto3.client('s3')
    downloaded_shps = set()
    for date in matching:
        shp_key  = shp_by_date[date]
        shp_stem = Path(shp_key).stem
        if shp_stem in downloaded_shps:
            continue
        shp_dir = str(Path(shp_key).parent) + "/"
        objects = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=shp_dir)
        for obj in objects.get('Contents', []):
            key = obj['Key']
            if Path(key).stem == shp_stem:
                s3_download(key, SHAPEFILES_DIR / Path(key).name)
        downloaded_shps.add(shp_stem)

    # ─ Process each date sequentially
    all_resultados = []

    for date in matching:
        shp_stem  = Path(shp_by_date[date]).stem
        shp_local = SHAPEFILES_DIR / f"{shp_stem}.shp"

        if not shp_local.exists():
            log.error(f"[{date}] Shapefile not found: {shp_local.name}. Skipping.")
            continue

        try:
            gdf = gpd.read_file(str(shp_local))
            gdf = gdf.set_crs(epsg=32615, allow_override=True)
        except Exception as e:
            log.error(f"[{date}] Could not load shapefile: {e}")
            continue

        log.info(f"[{date}] Loaded {len(gdf)} lots")

        # Download TIFFs + TFWs for this date
        ndvi_local   = download_tif_pair(ndvi_by_date[date])
        ndwi11_local = download_tif_pair(ndwi11_by_date[date])
        msi11_local  = download_tif_pair(msi11_by_date[date])

        if not all([ndvi_local, ndwi11_local, msi11_local]):
            log.error(f"[{date}] One or more TIFFs failed to download. Skipping.")
            continue

        # Compute zonal stats
        try:
            log_metrics(f"before-{date}")
            stats_ndvi,   gdf_r = zonal_stats_from_tiff(gdf, ndvi_local)
            stats_ndwi11, _     = zonal_stats_from_tiff(gdf, ndwi11_local)
            stats_msi11,  _     = zonal_stats_from_tiff(gdf, msi11_local)
            log_metrics(f"after-{date}")
        except Exception as e:
            log.error(f"[{date}] Error reading TIFFs: {e}")
            continue

        lot_ndvi   = _finalize(_accumulate_stats(stats_ndvi,   gdf_r), 'ndvi')
        lot_ndwi11 = _finalize(_accumulate_stats(stats_ndwi11, gdf_r), 'ndwi11')
        lot_msi11  = _finalize(_accumulate_stats(stats_msi11,  gdf_r), 'msi11')

        imagen_id = Path(ndvi_by_date[date]).parent.name
        fecha_str = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
        all_lotes = set(lot_ndvi) | set(lot_ndwi11) | set(lot_msi11)

        date_rows = []
        for cod_cg in all_lotes:
            row = {'lote': cod_cg, 'fecha': fecha_str, 'imagen_id': imagen_id}
            row.update(lot_ndvi.get(cod_cg,   {'ndvi_promedio': None,   'ndvi_max': None,
                                                'ndvi_min': None,        'ndvi_std': None}))
            row.update(lot_ndwi11.get(cod_cg, {'ndwi11_promedio': None, 'ndwi11_max': None,
                                                'ndwi11_min': None,      'ndwi11_std': None}))
            row.update(lot_msi11.get(cod_cg,  {'msi11_promedio': None,  'msi11_max': None,
                                                'msi11_min': None,       'msi11_std': None}))
            date_rows.append(row)

        con_ndvi = sum(1 for r in date_rows if r['ndvi_promedio'] is not None)
        log.info(f"[{date}] Done — {len(date_rows)} rows, {con_ndvi} with NDVI")
        all_resultados.extend(date_rows)

        # Clean up TIFFs after each date to keep /tmp usage low
        for f in [ndvi_local, ndwi11_local, msi11_local]:
            f.unlink(missing_ok=True)
            f.with_suffix('.tfw').unlink(missing_ok=True)

    # ─ Save Parquet and upload
    if not all_resultados:
        log.warning("No results generated. Exiting.")
        sys.exit(1)

    parquet_path = WORK_DIR / f"{YEAR}_indices_ref.parquet"
    df = pd.DataFrame(all_resultados, columns=COLS)
    df.to_parquet(str(parquet_path), index=False)

    con_valor = sum(1 for r in all_resultados if r['ndvi_promedio'] is not None)
    s3_key    = f"{S3_PREFIX}/{YEAR}/{YEAR}_indices_ref.parquet"
    s3_upload(parquet_path, s3_key)

    elapsed = time.time() - t_inicio
    log_metrics("done")
    log.info(f"=== Done: {len(all_resultados)} rows across {len(matching)} date(s) — "
             f"{con_valor} with NDVI — {elapsed:.1f}s ===")
