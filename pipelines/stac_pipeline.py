# -*- coding: utf-8 -*-
"""
process_shapefile_local.py — Local version, parallel (Windows/Linux)

Processes shapefiles from D:\STAC\{YEAR}\ locally:
- Reads shapefiles from local disk
- Queries STAC for matching Sentinel-2 imagery
- Calculates NDVI, NDWI-11, MSI-11 per lot in parallel
- Saves combined CSV to local output/

Formulas:
  NDVI    = (NIR - RED)     / (NIR + RED)
  NDWI-11 = (NIR - SWIR16)  / (NIR + SWIR16)
  MSI-11  = SWIR16 / NIR
"""

import json
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
import rasterio.features
from rasterio.windows import from_bounds as window_from_bounds
from rasterio.enums import Resampling
from rasterstats import zonal_stats
from shapely.geometry import box
from pystac_client import Client
from datetime import datetime
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from sqlalchemy import create_engine, text
import psutil

warnings.filterwarnings('ignore')


# ── Config ────────────────────────────────────────────────────────────────────
import sys; sys.path.insert(0, str(Path(__file__).parent.parent))
from config import STAC_ROOT, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from config import STAC_WORKERS, OUTPUT_ROOT as _OUTPUT_ROOT

YEAR        = os.environ.get('YEAR')
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', STAC_WORKERS))
OUTPUT_ROOT = _OUTPUT_ROOT / 'stac'
DB_TABLE    = "stac_indices"
CHECKPOINT  = _OUTPUT_ROOT / 'checkpoints' / 'stac.json'

STAC_URL   = "https://earth-search.aws.element84.com/v1"
CLOUD_MAX  = 100
LOTE_FIELD = "COD_CG"
NIR_KEY    = "nir"
RED_KEY    = "red"
SWIR16_KEY = "swir16"

COLS = [
    'lote', 'fecha', 'imagen_id',
    'ndvi_promedio',   'ndvi_max',   'ndvi_min',   'ndvi_std',
    'ndwi11_promedio', 'ndwi11_max', 'ndwi11_min', 'ndwi11_std',
    'msi11_promedio',  'msi11_max',  'msi11_min',  'msi11_std',
]


# ── Checkpoint ────────────────────────────────────────────────────────────────
class Checkpoint:
    """Tracks completed date keys per year in a JSON file."""
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data = json.loads(self.path.read_text()) if self.path.exists() else {}

    def completed(self, year):
        return set(self._data.get(str(year), []))

    def mark_done(self, year, key):
        yr = str(year)
        self._data.setdefault(yr, [])
        if key not in self._data[yr]:
            self._data[yr].append(key)
        self.path.write_text(json.dumps(self._data, indent=2))


# ── Per-year DB flush ─────────────────────────────────────────────────────────
def _flush_year_to_db(year, rows, engine, log):
    if not rows:
        log.warning(f"[{year}] No rows to write to PostgreSQL.")
        return
    df = pd.DataFrame(rows, columns=COLS)
    df['fecha'] = pd.to_datetime(df['fecha'])
    with engine.connect() as conn:
        conn.execute(text(f"DELETE FROM {DB_TABLE} WHERE EXTRACT(YEAR FROM fecha) = :y"), {'y': int(year)})
        conn.commit()
    df.to_sql(DB_TABLE, engine, if_exists='append', index=False)
    log.info(f"[{year}] PostgreSQL: {len(rows)} rows written to '{DB_TABLE}'.")


# ── Worker logging setup ──────────────────────────────────────────────────────
def _setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format=f'%(asctime)s [pid-{os.getpid()}] — %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
    return logging.getLogger(__name__)


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
        return {f'{prefix}_promedio': None, f'{prefix}_max': None, f'{prefix}_min': None, f'{prefix}_std': None}
    mean = entry['sum'] / n
    var  = max(0.0, entry['sum_sq'] / n - mean**2)
    return {
        f'{prefix}_promedio': round(mean, 4),
        f'{prefix}_max':      round(entry['max'], 4),
        f'{prefix}_min':      round(entry['min'], 4),
        f'{prefix}_std':      round(var**0.5, 4),
    }


def _buscar_tiles(client, bbox, fecha, max_retries=3):
    log = logging.getLogger(__name__)
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

    return list(best.values())


def _calcular_indices(gdf, items):
    log = logging.getLogger(__name__)
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

            if len(lotes_en_tile) == 0:
                continue

            lotes_new = lotes_en_tile[~lotes_en_tile.index.isin(processed_indices)]
            processed_indices.update(lotes_new.index)

            if lotes_new.empty:
                continue

            log.info(f"Tile {tile_id}: {len(lotes_new)} new polygon(s)")
            minx, miny, maxx, maxy = lotes_new.total_bounds

            try:
                t0         = time.time()
                nir_win    = window_from_bounds(minx, miny, maxx, maxy, nir_src.transform)
                red_win    = window_from_bounds(minx, miny, maxx, maxy, red_src.transform)
                swir16_win = window_from_bounds(minx, miny, maxx, maxy, swir16_src.transform)

                nir_arr = nir_src.read(1, window=nir_win)
                red_arr = red_src.read(1, window=red_win)
                h, w    = nir_arr.shape
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
            stats_ndvi   = zonal_stats(geoms, ndvi_arr,   affine=nir_transform, stats=['count','mean','max','min','std'], nodata=np.nan)
            stats_ndwi11 = zonal_stats(geoms, ndwi11_arr, affine=nir_transform, stats=['count','mean','max','min','std'], nodata=np.nan)
            stats_msi11  = zonal_stats(geoms, msi11_arr,  affine=nir_transform, stats=['count','mean','max','min','std'], nodata=np.nan)
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


# ── Top-level worker (must be top-level for pickling) ─────────────────────────
def process_single_shp(shp_path):
    """Worker: process one shapefile end-to-end. Returns list of result dicts."""
    log = _setup_logging()
    shp_path = Path(shp_path)

    match = re.search(r'(\d{8})', shp_path.stem)
    if not match:
        log.error(f"No date in filename: {shp_path.name}")
        return []

    fecha     = datetime.strptime(match.group(1), '%Y%m%d')
    fecha_str = fecha.strftime('%Y-%m-%d')

    try:
        gdf = gpd.read_file(str(shp_path))
        gdf = gdf.set_crs(epsg=32615, allow_override=True)
    except Exception as e:
        log.error(f"Could not load {shp_path.name}: {e}")
        return []

    log.info(f"[{fecha_str}] Loaded {len(gdf)} lots")

    client = Client.open(STAC_URL)
    bbox   = list(gdf.to_crs(epsg=4326).total_bounds)
    items  = _buscar_tiles(client, bbox, fecha)

    if not items:
        log.warning(f"[{fecha_str}] No images found. Skipping.")
        return []

    log.info(f"[{fecha_str}] Tiles: {[i.id.split('_')[1] for i in items]}")

    resultados = _calcular_indices(gdf, items)
    for r in resultados:
        r['fecha'] = fecha_str

    con_valor = sum(1 for r in resultados if r['ndvi_promedio'] is not None)
    log.info(f"[{fecha_str}] Done — {con_valor}/{len(gdf)} lots with NDVI")

    return resultados


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s — %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    log = logging.getLogger(__name__)

    if not STAC_ROOT.exists():
        log.error(f"STAC_ROOT not found: {STAC_ROOT}")
        sys.exit(1)

    # Discover years
    all_years = sorted(p.name for p in STAC_ROOT.iterdir() if p.is_dir() and p.name.isdigit())
    if YEAR:
        all_years = [y for y in all_years if y == str(YEAR)]
    log.info(f"Years to process: {all_years} — {MAX_WORKERS} workers")

    if not all_years:
        log.error("No years found.")
        sys.exit(1)

    t_inicio    = time.time()
    checkpoint  = Checkpoint(CHECKPOINT)
    engine      = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    # Collect shapefiles, skipping already-completed dates
    all_shp_files   = []
    pending_per_year = {}   # year -> remaining count
    total_per_year   = {}   # year -> total shapefiles (for CSV flush logic)

    for year in all_years:
        stac_dir = STAC_ROOT / year
        shps     = sorted(stac_dir.glob('*.shp'))
        done     = checkpoint.completed(year)
        pending  = []
        for shp in shps:
            m = re.search(r'(\d{8})', shp.stem)
            key = m.group(1) if m else shp.stem
            if key in done:
                log.info(f"  [{year}] Skipping {shp.name} (checkpoint)")
            else:
                pending.append(shp)
        total_per_year[year]   = len(shps)
        pending_per_year[year] = len(pending)
        log.info(f"  {year}: {len(shps)} total, {len(pending)} pending, {len(done)} already done")
        all_shp_files.extend(pending)

    log.info(f"Total shapefiles to process: {len(all_shp_files)}")

    # Run all in parallel
    results_by_year = {}

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_single_shp, shp): shp for shp in all_shp_files}
        for future in as_completed(futures):
            shp  = futures[future]
            year = shp.parent.name
            m    = re.search(r'(\d{8})', shp.stem)
            key  = m.group(1) if m else shp.stem
            try:
                result = future.result()
                results_by_year.setdefault(year, []).extend(result)
                checkpoint.mark_done(year, key)
                log.info(f"Collected {len(result)} rows from {shp.name} ({year})")
            except Exception as e:
                log.error(f"Worker failed for {shp.name}: {e}")

            # Flush year to CSV + DB when all its shapefiles are done
            pending_per_year[year] -= 1
            if pending_per_year[year] == 0:
                rows = results_by_year.get(year, [])
                if rows:
                    output_dir = OUTPUT_ROOT / year
                    output_dir.mkdir(parents=True, exist_ok=True)
                    csv_path = output_dir / f"{year}_indices_stac.csv"
                    df = pd.DataFrame(rows, columns=COLS)
                    df.to_csv(str(csv_path), index=False, encoding='utf-8-sig')
                    con_valor = sum(1 for r in rows if r['ndvi_promedio'] is not None)
                    log.info(f"[{year}] CSV saved: {csv_path} — {len(rows)} rows, {con_valor} with NDVI")
                    _flush_year_to_db(year, rows, engine, log)
                else:
                    log.warning(f"[{year}] No results to write.")

    # Ensure indexes exist
    with engine.connect() as conn:
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{DB_TABLE}_lote  ON {DB_TABLE} (lote)"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{DB_TABLE}_fecha ON {DB_TABLE} (fecha)"))
        conn.commit()

    total_rows = sum(len(v) for v in results_by_year.values())
    elapsed    = time.time() - t_inicio
    mem        = psutil.virtual_memory()
    log.info(f"=== Done: {total_rows} rows across {len(results_by_year)} year(s) — {elapsed:.1f}s ===")
    log.info(f"[METRICS] RAM: {mem.used/1024**3:.2f}/{mem.total/1024**3:.2f} GB ({mem.percent:.1f}%)")
