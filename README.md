## NDVI Extraction Pipeline — Sentinel-2 Vegetation Indices for Sugarcane Yield Forecasting

This pipeline extracts field-level vegetation indices from Sentinel-2 satellite imagery for ~11,000 sugarcane lots across Guatemala's Pacific coast, producing the satellite feature set that feeds CENGICAÑA's sugarcane yield forecasting models. It runs on AWS Batch and supports two modes: a STAC pipeline that queries and processes raw satellite bands on the fly via the Element84 API, and a reference pipeline that extracts statistics from pre-validated GeoTIFF composites. Because the two sources produce systematically different index values, the project also includes a cross-validated correction step — OLS and Huber regression models (R² 0.89–0.96) trained on 2020–2024 data — that aligns STAC output to the reference standard before results are loaded into PostgreSQL.

## Project Structure

```
pipelines/        — AWS Batch pipeline scripts (STAC + Reference)
eda/
  correction/     — STAC correction factor analysis and application
  viz/            — Interactive HTML visualizations (lot history, growth curves)
db/               — Migrations and DB upload scripts
  migrations/     — Numbered SQL migrations (001–011)
queries/          — SELECT queries and ML dataset definitions
terraform/        — AWS infrastructure (Batch, ECR, IAM, CloudWatch)
batch/            — Job submission scripts
tools/            — One-time utilities
```

## Pipelines

### STAC Pipeline (`pipelines/process_shapefile.py`)
- One AWS Batch job per shapefile
- Downloads shapefiles from `s3://ndvi-extraction/STAC/{year}/`
- Queries Sentinel-2 imagery via STAC API (element84)
- Computes NDVI, NDWI-11, MSI-11 from raw NIR/RED/SWIR16 bands
- Uploads Parquet to `s3://ndvi-extraction/output/stac/{year}/`

### Reference Pipeline (`pipelines/process_reference_tiff.py`)
- One AWS Batch job per year
- Downloads pre-computed index TIFFs from `s3://ndvi-extraction/Reference/{year}-raster/`
- Matches TIFFs with shapefiles by date
- Extracts zonal statistics per lot
- Uploads Parquet to `s3://ndvi-extraction/output/reference/{year}/`

## S3 Structure

```
ndvi-extraction/
├── STAC/
│   └── {year}/
│       └── *.shp  (+ .dbf .shx .prj .cpg)
├── Reference/
│   └── {year}-raster/
│       └── {subfolder}/
│           ├── 01-NDVI_*.tif     + .tfw
│           ├── 02-NDWI-11_*.tif  + .tfw
│           └── 04-MSI-11_*.tif   + .tfw
└── output/
    ├── stac/
    │   └── {year}/
    │       └── {date}_indices_stac.parquet
    └── reference/
        └── {year}/
            └── {year}_indices_ref.parquet
```

## Infrastructure

Managed with Terraform (`terraform/`):
- **ECR** — Docker image repository
- **AWS Batch** — compute environment (EC2 `r6i.large`, up to 96 vCPUs), job queue, job definition
- **IAM** — roles for Batch service, EC2 instances, and job execution
- **CloudWatch** — log group (`/aws/batch/ndvi-extraction`, 7-day retention)

The S3 bucket is not managed by Terraform — it is referenced by name only.

## Setup

```bash
git clone <repo>
cd ndvi-extraction-locally

# Deploy infrastructure
cd terraform
terraform init
terraform apply

# Build and push Docker image
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <ecr-url>
docker build -t ndvi-extraction-job .
docker tag ndvi-extraction-job:latest <ecr-url>:latest
docker push <ecr-url>:latest
```

## Submit Jobs

```bash
# STAC pipeline — one job per shapefile, auto-discovers years from S3
python batch/submit_jobs.py

# Reference pipeline — one job per year, auto-discovers years from S3
python batch/submit_reference_jobs.py

# Submit specific years only
python batch/submit_jobs.py --years 2020 2021
python batch/submit_reference_jobs.py --years 2020 2021

# Test with a limited number of jobs
python batch/submit_jobs.py --jobs 5
```

## Monitor

Logs are available in CloudWatch:
```
AWS Console → CloudWatch → Log groups → /aws/batch/ndvi-extraction
```

---

## EDA — STAC Correction Factors

The STAC pipeline computes indices from raw Sentinel-2 COG bands on the fly. Field cross-validations confirmed these values are systematically off compared to the Reference pipeline, which uses pre-validated TIFFs.

`eda/correction/` contains the analysis and scripts to derive and apply a per-index linear correction: `Reference = slope * STAC + intercept`.

### Correction model

| Index | Model | Slope | Intercept | CV R² |
|-------|-------|-------|-----------|-------|
| NDVI | OLS | 0.8335 | 0.0069 | 0.950 |
| NDWI-11 | OLS | 0.8971 | 0.1013 | 0.958 |
| MSI-11 | Huber | 0.7499 | 0.0578 | 0.894 |

Trained on 2020–2024, validated on held-out 2025. Coefficients stored in `eda/correction/correction_factors.json` under the `validation_1` key.

### Scripts

| Script | Description |
|--------|-------------|
| `eda/correction/utils.py` | Shared data loading and cleaning |
| `eda/correction/explore.py` | Scatter plots, residual distributions, per-year bias |
| `eda/correction/fit_correction.py` | Fits correction models, runs CV, saves coefficients to JSON |
| `eda/correction/apply_correction.py` | Applies correction factors to matched dataset, outputs CSV |
| `db/upload_to_db.py` | Uploads corrected output to PostgreSQL via SSH tunnel |
| `tools/convert_ppk.py` | One-time conversion of PuTTY `.ppk` key to OpenSSH `.pem` |

### Run order

```bash
python eda/correction/explore.py
python eda/correction/fit_correction.py
python eda/correction/apply_correction.py
python db/upload_to_db.py
```

---

## Database — `maestra` Table

PostgreSQL table in `DB_Lake` (148.113.225.209). Created as a LEFT JOIN of `stac_corrected_indices` with `productividad` on `cod_cg_zafra`. ~1.5M rows covering 2020–2025.

### Key columns

| Column | Description |
|--------|-------------|
| `cod_cg` | Lot identifier |
| `fecha` | Observation date |
| `zafra` | Nov-Oct calendar season (e.g. `2020_2021`) |
| `cierre_ciclo` | Next harvest date >= fecha — defines the agronomic cycle |
| `edad_de_cultivo` | Days since previous harvest (resets at each cycle boundary) |
| `gap_in_data` | TRUE if gap between consecutive harvests > 548 days |
| `ciclo_valido` | TRUE if cycle is clean enough for ML training |
| `ndvi_ref`, `ndwi11_ref`, `msi11_ref` | Reference pipeline indices (primary source) |
| `ndvi_corrected`, `ndwi11_corrected`, `msi11_corrected` | Corrected STAC indices |
| `tch` | Yield in toneladas de caña por hectárea (target variable) |

### Migrations

Numbered SQL files in `db/migrations/`. Run with `python db/migrate.py`.

| Migration | Description |
|-----------|-------------|
| 001 | Add `zafra` and `cod_cg_zafra` join keys |
| 002 | Create `maestra` table via LEFT JOIN |
| 003 | Add `edad_de_cultivo` (initial version) |
| 007 | Add `cierre_ciclo` (agronomic cycle boundary) |
| 008 | Recalculate `edad_de_cultivo` using `cierre_ciclo` |
| 009 | Add `gap_in_data` flag |
| 010 | Detect renovation cycles in lots missing from productividad |
| 011 | Add `ciclo_valido` boolean |

> Migrations 004–006 are superseded by 007–010 and not applied (tracked in `_migrations` to prevent accidental execution).

### `cierre_ciclo` vs `zafra`

`zafra` is the Nov-Oct administrative calendar window. `cierre_ciclo` is the actual next harvest date the plant is growing toward — these differ for observations after a harvest but still within the same zafra window. `cierre_ciclo` is the correct grouping for agronomic analysis.

---

## ML Dataset

**628,522 rows** across **34,033 (lot, zafra) samples** (2020–2025), ready for yield prediction.

Filters applied:
- `ciclo_valido = TRUE` — clean cycle (known start + end, no data gap, ≥7 observations, max edad ≥150 days)
- `cierre IS NOT NULL` — has a yield label from productividad

Fetch query: `queries/ml_dataset.sql`

### Valid cycle distribution

| Range (days) | % of valid cycles |
|---|---|
| 300–350 | 35.2% |
| 350–400 | 45.6% |
| Other | 19.2% |

80.8% of valid cycles fall between 300–400 days — consistent annual sugarcane harvest cycle.

### Visualizations

```bash
# Lot history — NDVI over time colored by agronomic cycle
python eda/viz/lot_history.py        → eda/viz/lot_history.html

# Growth curves — NDVI vs edad_de_cultivo per lot
python eda/viz/ndvi_growth_curves.py → eda/viz/ndvi_growth_curves.html

# Cultivo renovado — renovation-detected cycles
python eda/viz/cultivo_renovado.py   → eda/viz/cultivo_renovado.html
```

### Environment

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```
