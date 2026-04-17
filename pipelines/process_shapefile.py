# -*- coding: utf-8 -*-
"""
process_shapefile.py — AWS Batch entry point (STAC pipeline)

Processes a single shapefile: queries STAC for Sentinel-2 imagery,
computes NDVI, NDWI-11, MSI-11 per lot, saves Parquet, uploads to S3.
One Batch job per shapefile.

Environment variables (set by Batch at submission time):
    SHP_PATH   — S3 URI to the .shp file  (e.g. s3://ndvi-extraction/STAC/2020/20200115.shp)
    S3_BUCKET  — bucket name              (e.g. ndvi-extraction)
    S3_PREFIX  — output prefix            (default: output/stac)

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
from rasterio.windows import from_bounds as window_from_bounds
from rasterio.enums import Resampling
from shapely.geometry import box
from pystac_client import Client
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
SHP_PATH  = os.environ.get('SHP_PATH')
S3_BUCKET = os.environ.get('S3_BUCKET')
S3_PREFIX = os.environ.get('S3_PREFIX', 'output/stac')

STAC_URL   = "https://earth-search.aws.element84.com/v1"
CLOUD_MAX  = 100
LOTE_FIELD = "COD_CG"
NIR_KEY    = "nir"
RED_KEY    = "red"
SWIR16_KEY = "swir16"

WORK_DIR = Path('/tmp/ndvi')

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
def s3_upload(local_path, s3_key):
    try:
        boto3.client('s3').upload_file(str(local_path), S3_BUCKET, s3_key)
        log.info(f"[S3] Uploaded → s3://{S3_BUCKET}/{s3_key}")
    except (BotoCoreError, ClientError) as e:
        log.error(f"[S3] Upload failed for {Path(local_path).name}: {e}")


# ── Index helpers ─────────────────────────────────────────────────────────────
def _ratio_index(band_a, band_b):
    a = band_a.astype(float)
    b = band_b.astype(float)
    with np.errstate(invalid='ignore', divide='ignore'):
        s = a + b
        return np.where((a > 0) & (b > 0) & (s > 0), (a - b) / s, np.nan)


def _msi(nir, swir):
    n = nir.astype(float)
    s = swir.astype(float)
    with np.errstate(invalid='ignore', divide='ignore'):
        return np.where((n > 0) & (s > 0), s / n, np.nan)


# ── Accumulate + finalize ─────────────────────────────────────────────────────
def _accumulate(lot_data, key, cod_cg, stat, imagen_id):
    count = stat.get('count') or 0
    if count == 0 or stat.get('mean') is None:
        if cod_cg not in lot_data:
            lot_data[cod_cg] = {}
        if key not in lot_data[cod_cg]:
            lot_data[cod_cg][key] = {'count': 0, 'sum': 0.0, 'sum_sq': 0.0, 'max': None, 'min': None}
        if 'imagen_id' not in lot_data[cod_cg]:
            lot_data[cod_cg]['imagen_id'] = imagen_id
        return

    mean = stat['mean']
    std  = stat.get('std') or 0.0

    if cod_cg not in lot_data:
        lot_data[cod_cg] = {'imagen_id': imagen_id}

    if key not in lot_data[cod_cg] or lot_data[cod_cg][key]['count'] == 0:
        lot_data[cod_cg][key] = {
            'count':  count,
            'sum':    mean * count,
            'sum_sq': (std**2 + mean**2) * count,
            'max':    stat['max'],
            'min':    stat['min'],
        }
    else:
        d = lot_data[cod_cg][key]
        d['count']  += count
        d['sum']    += mean * count
        d['sum_sq'] += (std**2 + mean**2) * count
        d['max']     = max(d['max'], stat['max'])
        d['min']     = min(d['min'], stat['min'])


def _finalize(d, prefix):
    entry = d.get(prefix, {})
    n = entry.get('count', 0)
    if n == 0:
        return {f'{prefix}_promedio': None, f'{prefix}_max': None,
                f'{prefix}_min': None,      f'{prefix}_std': None}
    mean = entry['sum'] / n
    var  = max(0.0, entry['sum_sq'] / n - mean**2)
    return {
        f'{prefix}_promedio': round(mean, 4),
        f'{prefix}_max':      round(entry['max'], 4),
        f'{prefix}_min':      round(entry['min'], 4),
        f'{prefix}_std':      round(var**0.5, 4),
    }


# ── STAC search ───────────────────────────────────────────────────────────────
def buscar_tiles(client, bbox, fecha, max_retries=3):
    inicio = fecha.strftime('%Y-%m-%dT00:00:00Z')
    fin    = fecha.strftime('%Y-%m-%dT23:59:59Z')

    items = []
    for intento in range(max_retries):
        try:
            search = client.search(
                collections=['sentinel-2-l2a'],
                bbox=bbox,
                datetime=f"{inicio}/{fin}",
                query={"eo:cloud_cover": {"lt": CLOUD_MAX}},
                limit=50,
            )
            items = list(search.items())
            log.info(f"STAC: {len(items)} item(s) found")
            break
        except Exception as e:
            if intento < max_retries - 1:
                log.warning(f"STAC retry {intento+1}/{max_retries}: {e}")
                time.sleep(5)
            else:
                log.error(f"STAC failed after {max_retries} attempts: {e}")
                return []

    if not items:
        return []

    best = {}
    for item in items:
        tile_id = item.id.split('_')[1]
        nubes   = item.properties.get('eo:cloud_cover', 100)
        if tile_id not in best or nubes < best[tile_id].properties.get('eo:cloud_cover', 100):
            best[tile_id] = item

    for tile_id, item in best.items():
        log.info(f"Tile selected: {tile_id} — cloud cover: {item.properties.get('eo:cloud_cover', '?'):.1f}%")

    return list(best.values())


# ── Index computation ─────────────────────────────────────────────────────────
def calcular_indices(gdf, items):
    processed_indices = set()
    lot_data = {}

    for item in items:
        tile_id = item.id.split('_')[1]
        missing = [k for k in (NIR_KEY, RED_KEY, SWIR16_KEY) if k not in item.assets]
        if missing:
            log.warning(f"Tile {tile_id}: missing assets {missing}. Skipping.")
            continue

        try:
            nir_src    = rasterio.open(item.assets[NIR_KEY].href)
            red_src    = rasterio.open(item.assets[RED_KEY].href)
            swir16_src = rasterio.open(item.assets[SWIR16_KEY].href)
        except Exception as e:
            log.error(f"Could not open tile {tile_id}: {e}")
            continue

        with nir_src, red_src, swir16_src:
            gdf_r         = gdf.to_crs(nir_src.crs)
            extent_tile   = box(*nir_src.bounds)
            lotes_en_tile = gdf_r[gdf_r.intersects(extent_tile)]

            if lotes_en_tile.empty:
                log.info(f"Tile {tile_id}: no lots within extent. Skipping.")
                continue

            lotes_new = lotes_en_tile[~lotes_en_tile.index.isin(processed_indices)]
            processed_indices.update(lotes_new.index)

            if lotes_new.empty:
                log.info(f"Tile {tile_id}: all lots already processed in a prior tile. Skipping.")
                continue

            log.info(f"Tile {tile_id}: {len(lotes_new)} new polygon(s)")
            minx, miny, maxx, maxy = lotes_new.total_bounds

            try:
                t0         = time.time()
                nir_win    = window_from_bounds(minx, miny, maxx, maxy, nir_src.transform)
                red_win    = window_from_bounds(minx, miny, maxx, maxy, red_src.transform)
                swir16_win = window_from_bounds(minx, miny, maxx, maxy, swir16_src.transform)

                nir_arr    = nir_src.read(1, window=nir_win)
                red_arr    = red_src.read(1, window=red_win)
                h, w       = nir_arr.shape
                swir16_arr = swir16_src.read(
                    1, window=swir16_win,
                    out_shape=(h, w),
                    resampling=Resampling.bilinear,
                )
                nir_transform = nir_src.window_transform(nir_win)
                log.info(f"Tile {tile_id}: bands read in {time.time()-t0:.1f}s — shape {nir_arr.shape}")
            except Exception as e:
                log.error(f"Could not read bands for tile {tile_id}: {e}")
                continue

            ndvi_arr   = _ratio_index(nir_arr, red_arr)
            ndwi11_arr = _ratio_index(nir_arr, swir16_arr)
            msi11_arr  = _msi(nir_arr, swir16_arr)
            geoms      = lotes_new.geometry.tolist()

            t0           = time.time()
            stats_ndvi   = zonal_stats(geoms, ndvi_arr,   affine=nir_transform,
                                       stats=['count', 'mean', 'max', 'min', 'std'], nodata=np.nan)
            stats_ndwi11 = zonal_stats(geoms, ndwi11_arr, affine=nir_transform,
                                       stats=['count', 'mean', 'max', 'min', 'std'], nodata=np.nan)
            stats_msi11  = zonal_stats(geoms, msi11_arr,  affine=nir_transform,
                                       stats=['count', 'mean', 'max', 'min', 'std'], nodata=np.nan)
            log.info(f"Tile {tile_id}: zonal stats in {time.time()-t0:.1f}s")

            for (idx, fila), s_ndvi, s_ndwi11, s_msi11 in zip(
                    lotes_new.iterrows(), stats_ndvi, stats_ndwi11, stats_msi11):
                cod_cg = fila.get(LOTE_FIELD, str(idx))
                _accumulate(lot_data, 'ndvi',   cod_cg, s_ndvi,   item.id)
                _accumulate(lot_data, 'ndwi11', cod_cg, s_ndwi11, item.id)
                _accumulate(lot_data, 'msi11',  cod_cg, s_msi11,  item.id)

    resultados = []
    for cod_cg, d in lot_data.items():
        row = {'lote': cod_cg, 'imagen_id': d.get('imagen_id', '')}
        row.update(_finalize(d, 'ndvi'))
        row.update(_finalize(d, 'ndwi11'))
        row.update(_finalize(d, 'msi11'))
        resultados.append(row)

    return resultados


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':

    if not SHP_PATH:
        log.error("SHP_PATH environment variable not set. Exiting.")
        sys.exit(1)
    if not S3_BUCKET:
        log.error("S3_BUCKET environment variable not set. Exiting.")
        sys.exit(1)

    WORK_DIR.mkdir(parents=True, exist_ok=True)
    t_inicio = time.time()

    # Parse S3 URI → s3://ndvi-extraction/STAC/{year}/{stem}.shp
    shp_s3_key = SHP_PATH.replace(f"s3://{S3_BUCKET}/", "")
    shp_stem   = Path(shp_s3_key).stem
    shp_prefix = str(Path(shp_s3_key).parent) + "/"
    year       = Path(shp_s3_key).parts[1]   # STAC / {year} / file.shp

    log.info(f"=== Starting: {shp_stem} (year {year}) ===")
    log_metrics("start")

    # Download all companion files (.shp .dbf .shx .prj .cpg …) from S3
    s3 = boto3.client('s3')
    log.info(f"Downloading shapefile companions from S3: {shp_prefix}{shp_stem}.*")
    objects = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=shp_prefix)
    for obj in objects.get('Contents', []):
        key = obj['Key']
        if Path(key).stem == shp_stem:
            local_path = WORK_DIR / Path(key).name
            s3.download_file(S3_BUCKET, key, str(local_path))
            log.info(f"  Downloaded: {Path(key).name}")

    shp = WORK_DIR / f"{shp_stem}.shp"
    if not shp.exists():
        log.error(f"Shapefile not found after download: {shp}")
        sys.exit(1)

    # Extract date from filename
    match = re.search(r'(\d{8})', shp.stem)
    if not match:
        log.error(f"No date found in filename: {shp.name}")
        sys.exit(1)

    fecha     = datetime.strptime(match.group(1), '%Y%m%d')
    fecha_str = fecha.strftime('%Y-%m-%d')
    log.info(f"Date: {fecha_str}")

    # Load shapefile
    gdf = gpd.read_file(str(shp))
    gdf = gdf.set_crs(epsg=32615, allow_override=True)
    log.info(f"Lots: {len(gdf)}")

    # Query STAC
    bbox   = list(gdf.to_crs(epsg=4326).total_bounds)
    client = Client.open(STAC_URL)
    items  = buscar_tiles(client, bbox, fecha)

    if not items:
        log.warning(f"No imagery found for {fecha_str}. Exiting.")
        sys.exit(0)

    log.info(f"Tiles to process: {[i.id.split('_')[1] for i in items]}")

    # Compute indices
    log_metrics("before-compute")
    resultados = calcular_indices(gdf, items)
    log_metrics("after-compute")

    for r in resultados:
        r['fecha'] = fecha_str

    # Save Parquet locally then upload
    date_tag     = match.group(1)
    parquet_path = WORK_DIR / f"{date_tag}_indices_stac.parquet"
    df = pd.DataFrame(resultados, columns=COLS)
    df.to_parquet(str(parquet_path), index=False)

    con_valor = sum(1 for r in resultados if r['ndvi_promedio'] is not None)
    s3_key    = f"{S3_PREFIX}/{year}/{date_tag}_indices_stac.parquet"
    s3_upload(parquet_path, s3_key)

    elapsed = time.time() - t_inicio
    log_metrics("done")
    log.info(f"=== Done: {con_valor}/{len(gdf)} lots with NDVI — {elapsed:.1f}s ===")
