# NDVI Local Pipeline

Processes Sentinel-2 satellite imagery to extract per-lot vegetation indices (NDVI, NDWI-11, MSI-11) for sugarcane lots. Results are written to PostgreSQL.

## Pipelines

### STAC Pipeline (`pipelines/stac_pipeline.py`)
- Reads shapefiles from `STAC_ROOT/{year}/`
- Queries Sentinel-2 imagery via STAC API (element84)
- Computes NDVI, NDWI-11, MSI-11 from raw NIR/RED/SWIR16 bands
- Writes results to PostgreSQL table `stac_indices`

### Reference Pipeline (`pipelines/reference_pipeline.py`)
- Reads pre-computed index TIFFs from `REF_ROOT/{year}-raster/`
- Matches TIFFs with shapefiles by date
- Extracts zonal statistics per lot
- Writes results to PostgreSQL table `reference_indices`

## Output columns

| Column | Description |
|--------|-------------|
| lote | Lot identifier (COD_CG) |
| fecha | Date (YYYY-MM-DD) |
| imagen_id | Sentinel-2 image ID |
| ndvi_promedio / _max / _min / _std | NDVI statistics |
| ndwi11_promedio / _max / _min / _std | NDWI-11 statistics |
| msi11_promedio / _max / _min / _std | MSI-11 statistics |

## Setup

```bash
git clone <repo>
cd NDVI-local
pip install -r requirements.txt
cp config.example.py config.py   # edit with your paths and DB credentials
```

## Run

```bash
# Process all years
python pipelines/stac_pipeline.py
python pipelines/reference_pipeline.py

# Process a single year
YEAR=2020 python pipelines/stac_pipeline.py
YEAR=2020 python pipelines/reference_pipeline.py

# Adjust number of parallel workers
MAX_WORKERS=4 python pipelines/stac_pipeline.py
```

## Configuration (`config.py`)

| Setting | Description |
|---------|-------------|
| `STAC_ROOT` | Folder with year subfolders containing `.shp` files |
| `REF_ROOT` | Folder with `{year}-raster` subfolders containing TIFFs |
| `OUTPUT_ROOT` | Where per-year CSVs are written |
| `STAC_WORKERS` | Parallel workers for STAC pipeline (default 6) |
| `REF_WORKERS` | Parallel workers for reference pipeline (default 4) |
| `DB_*` | PostgreSQL connection details |
