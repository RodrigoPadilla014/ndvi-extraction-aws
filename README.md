# NDVI AWS Pipeline

Processes Sentinel-2 satellite imagery to extract per-lot vegetation indices (NDVI, NDWI-11, MSI-11) for sugarcane lots. Runs on AWS Batch with inputs and outputs stored in S3.

## Pipelines

### STAC Pipeline (`scripts/process_shapefile.py`)
- One AWS Batch job per shapefile
- Downloads shapefiles from `s3://ndvi-extraction/STAC/{year}/`
- Queries Sentinel-2 imagery via STAC API (element84)
- Computes NDVI, NDWI-11, MSI-11 from raw NIR/RED/SWIR16 bands
- Uploads Parquet to `s3://ndvi-extraction/output/stac/{year}/`

### Reference Pipeline (`reference/process_reference_tiff.py`)
- One AWS Batch job per year
- Downloads pre-computed index TIFFs from `s3://ndvi-extraction/Reference/{year}-raster/`
- Matches TIFFs with shapefiles by date
- Extracts zonal statistics per lot
- Uploads Parquet to `s3://ndvi-extraction/output/reference/{year}/`

## Output columns

| Column | Description |
|--------|-------------|
| lote | Lot identifier (COD_CG) |
| fecha | Date (YYYY-MM-DD) |
| imagen_id | Sentinel-2 image ID |
| ndvi_promedio / _max / _min / _std | NDVI statistics |
| ndwi11_promedio / _max / _min / _std | NDWI-11 statistics |
| msi11_promedio / _max / _min / _std | MSI-11 statistics |

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
cd ndvi-extraction-aws

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
