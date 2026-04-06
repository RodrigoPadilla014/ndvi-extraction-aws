from pathlib import Path

# ── Data paths ────────────────────────────────────────────────────────────────
# Adjust these to match your machine's directory structure
STAC_ROOT   = Path('D:/STAC')           # folder containing year subfolders with .shp files
REF_ROOT    = Path('D:/Reference')      # folder containing {year}-raster subfolders
OUTPUT_ROOT = Path('./output')          # where per-year CSVs will be written

# ── Processing ────────────────────────────────────────────────────────────────
STAC_WORKERS = 6   # parallel workers for STAC pipeline
REF_WORKERS  = 4   # parallel workers for reference pipeline (lower: large TIFFs)

# ── PostgreSQL ────────────────────────────────────────────────────────────────
DB_HOST     = "localhost"
DB_PORT     = 5432
DB_NAME     = "your_database"
DB_USER     = "your_user"
DB_PASSWORD = "your_password"
