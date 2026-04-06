# -*- coding: utf-8 -*-
"""
process_reference_tiff_local.py — Local version, parallel (Windows/Linux)

Processes pre-computed index TIFFs from D:\Reference\{YEAR}-raster\:
- 01-NDVI_*.tif
- 02-NDWI-11_*.tif
- 04-MSI-11_*.tif

Matches TIFF dates with shapefiles from D:\STAC\{YEAR}\,
extracts zonal stats per lot in parallel, saves combined CSV to local output/.
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
from concurrent.futures import ProcessPoolExecutor, as_completed
from sqlalchemy import create_engine, text
import psutil
from rasterstats import zonal_stats

warnings.filterwarnings('ignore')


# ── Config ────────────────────────────────────────────────────────────────────
# YEAR: filter to a single year (optional). If not set, processes all years found.
YEAR        = os.environ.get('YEAR')
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', REF_WORKERS))

import sys; sys.path.insert(0, str(Path(__file__).parent.parent))
from config import STAC_ROOT, REF_ROOT, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from config import REF_WORKERS, OUTPUT_ROOT as _OUTPUT_ROOT

OUTPUT_ROOT = _OUTPUT_ROOT / 'reference'
DB_TABLE    = "reference_indices"

LOTE_FIELD = "COD_CG"

COLS = [
    'lote', 'fecha', 'imagen_id',
    'ndvi_promedio',   'ndvi_max',   'ndvi_min',   'ndvi_std',
    'ndwi11_promedio', 'ndwi11_max', 'ndwi11_min', 'ndwi11_std',
    'msi11_promedio',  'msi11_max',  'msi11_min',  'msi11_std',
]


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


# ── Date extraction ───────────────────────────────────────────────────────────
def extract_date(filename):
    match = re.search(r'(\d{8})', filename)
    return match.group(1) if match else None


# ── World file (.tfw) reader ──────────────────────────────────────────────────
def read_tfw(tiff_path):
    """
    Read the .tfw world file and return the correct Affine transform.
    Needed because the TIFFs have a broken internal geotransform (pixel coords).
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
def _zonal_stats_from_tiff(gdf, tiff_path):
    tiff_path = Path(tiff_path)
    with rasterio.open(str(tiff_path)) as src:
        crs       = src.crs
        arr       = src.read(1)
        transform = read_tfw(tiff_path) or src.transform
        gdf_r     = gdf.to_crs(crs)
        return zonal_stats(
            gdf_r.geometry.tolist(),
            arr,
            affine=transform,
            stats=['count', 'mean', 'max', 'min', 'std'],
            nodata=np.nan,
        ), gdf_r


# ── Accumulate + finalize helpers ─────────────────────────────────────────────
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
            out[cod_cg] = {f'{prefix}_promedio': None, f'{prefix}_max': None, f'{prefix}_min': None, f'{prefix}_std': None}
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


# ── Top-level worker (must be top-level for pickling) ─────────────────────────
def process_single_date(args):
    """Worker: process one date (SHP + 3 TIFFs). Returns list of result dicts."""
    date_str, shp_path, ndvi_tif, ndwi11_tif, msi11_tif = args
    log = _setup_logging()

    shp_path   = Path(shp_path)
    ndvi_tif   = Path(ndvi_tif)
    ndwi11_tif = Path(ndwi11_tif)
    msi11_tif  = Path(msi11_tif)

    try:
        gdf = gpd.read_file(str(shp_path))
        gdf = gdf.set_crs(epsg=32615, allow_override=True)
    except Exception as e:
        log.error(f"[{date_str}] Could not load shapefile: {e}")
        return []

    log.info(f"[{date_str}] Loaded {len(gdf)} lots")

    try:
        stats_ndvi,   gdf_r = _zonal_stats_from_tiff(gdf, ndvi_tif)
        stats_ndwi11, _     = _zonal_stats_from_tiff(gdf, ndwi11_tif)
        stats_msi11,  _     = _zonal_stats_from_tiff(gdf, msi11_tif)
    except Exception as e:
        log.error(f"[{date_str}] Error reading TIFFs: {e}")
        return []

    lot_ndvi   = _finalize(_accumulate_stats(stats_ndvi,   gdf_r), 'ndvi')
    lot_ndwi11 = _finalize(_accumulate_stats(stats_ndwi11, gdf_r), 'ndwi11')
    lot_msi11  = _finalize(_accumulate_stats(stats_msi11,  gdf_r), 'msi11')

    imagen_id = ndvi_tif.parent.name
    fecha_str = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
    all_lotes = set(lot_ndvi) | set(lot_ndwi11) | set(lot_msi11)

    resultados = []
    for cod_cg in all_lotes:
        row = {'lote': cod_cg, 'fecha': fecha_str, 'imagen_id': imagen_id}
        row.update(lot_ndvi.get(cod_cg,   {'ndvi_promedio': None,   'ndvi_max': None,   'ndvi_min': None,   'ndvi_std': None}))
        row.update(lot_ndwi11.get(cod_cg, {'ndwi11_promedio': None, 'ndwi11_max': None, 'ndwi11_min': None, 'ndwi11_std': None}))
        row.update(lot_msi11.get(cod_cg,  {'msi11_promedio': None,  'msi11_max': None,  'msi11_min': None,  'msi11_std': None}))
        resultados.append(row)

    con_ndvi = sum(1 for r in resultados if r['ndvi_promedio'] is not None)
    log.info(f"[{date_str}] Done — {len(resultados)} rows, {con_ndvi} with NDVI")

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

    if not REF_ROOT.exists():
        log.error(f"REF_ROOT not found: {REF_ROOT}")
        sys.exit(1)
    if not STAC_ROOT.exists():
        log.error(f"STAC_ROOT not found: {STAC_ROOT}")
        sys.exit(1)

    # Discover years from Reference folder (pattern: {YEAR}-raster)
    all_years = sorted(
        p.name.replace('-raster', '')
        for p in REF_ROOT.iterdir()
        if p.is_dir() and p.name.endswith('-raster')
    )
    if YEAR:
        all_years = [y for y in all_years if y == str(YEAR)]
    log.info(f"Years to process: {all_years} — {MAX_WORKERS} workers")

    if not all_years:
        log.error("No years found.")
        sys.exit(1)

    t_inicio = time.time()

    # Collect all work items across all years
    all_worker_args = []  # list of (date_str, shp, ndvi, ndwi11, msi11)
    year_of_date    = {}  # date_str -> year (for grouping results)

    for year in all_years:
        ref_dir  = REF_ROOT  / f'{year}-raster'
        stac_dir = STAC_ROOT / year

        if not stac_dir.exists():
            log.warning(f"{year}: STAC dir not found, skipping.")
            continue

        ndvi_tiffs   = {extract_date(p.name): p for p in sorted(ref_dir.glob('*/01-NDVI_*.tif'))    if extract_date(p.name)}
        ndwi11_tiffs = {extract_date(p.name): p for p in sorted(ref_dir.glob('*/02-NDWI-11_*.tif')) if extract_date(p.name)}
        msi11_tiffs  = {extract_date(p.name): p for p in sorted(ref_dir.glob('*/04-MSI-11_*.tif'))  if extract_date(p.name)}
        shp_by_date  = {extract_date(p.name): p for p in sorted(stac_dir.glob('*.shp'))             if extract_date(p.name)}

        matching = sorted(set(ndvi_tiffs) & set(ndwi11_tiffs) & set(msi11_tiffs) & set(shp_by_date))
        skipped  = (set(ndvi_tiffs) | set(ndwi11_tiffs) | set(msi11_tiffs)) - set(matching)

        log.info(f"  {year}: {len(matching)} complete dates, {len(skipped)} skipped")
        if skipped:
            log.warning(f"  {year} skipped: {sorted(skipped)}")

        for d in matching:
            all_worker_args.append((d, str(shp_by_date[d]), str(ndvi_tiffs[d]), str(ndwi11_tiffs[d]), str(msi11_tiffs[d])))
            year_of_date[d] = year

    log.info(f"Total dates to process: {len(all_worker_args)}")

    # Run all in parallel
    results_by_year = {}

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_single_date, args): args[0] for args in all_worker_args}
        for future in as_completed(futures):
            date_str = futures[future]
            year     = year_of_date[date_str]
            try:
                result = future.result()
                results_by_year.setdefault(year, []).extend(result)
                log.info(f"Collected {len(result)} rows from {date_str} ({year})")
            except Exception as e:
                log.error(f"Worker failed for {date_str}: {e}")

    # Write one CSV per year
    total_rows = 0
    for year in sorted(results_by_year):
        rows = results_by_year[year]
        if not rows:
            log.warning(f"{year}: no results.")
            continue
        output_dir = OUTPUT_ROOT / year
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / f"{year}_indices_ref.csv"
        df = pd.DataFrame(rows, columns=COLS)
        df.to_csv(str(csv_path), index=False, encoding='utf-8-sig')
        con_valor = sum(1 for r in rows if r['ndvi_promedio'] is not None)
        log.info(f"[{year}] CSV saved: {csv_path} — {len(rows)} rows, {con_valor} with NDVI")
        total_rows += len(rows)

    elapsed = time.time() - t_inicio
    mem     = psutil.virtual_memory()
    log.info(f"=== Done: {total_rows} total rows across {len(results_by_year)} year(s) — {elapsed:.1f}s ===")
    log.info(f"[METRICS] RAM: {mem.used/1024**3:.2f}/{mem.total/1024**3:.2f} GB ({mem.percent:.1f}%)")

    # ── Write all years combined to PostgreSQL ────────────────────────────────
    all_rows = [row for rows in results_by_year.values() for row in rows]
    if all_rows:
        log.info(f"Writing {len(all_rows)} rows to PostgreSQL table '{DB_TABLE}'...")
        engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        df_all = pd.DataFrame(all_rows, columns=COLS)
        df_all['fecha'] = pd.to_datetime(df_all['fecha'])
        df_all.to_sql(DB_TABLE, engine, if_exists='replace', index=False)

        with engine.connect() as conn:
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{DB_TABLE}_lote  ON {DB_TABLE} (lote)"))
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{DB_TABLE}_fecha ON {DB_TABLE} (fecha)"))
            conn.commit()

        log.info(f"PostgreSQL: table '{DB_TABLE}' written and indexed.")
