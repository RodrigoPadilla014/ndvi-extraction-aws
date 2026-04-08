# -*- coding: utf-8 -*-
"""
submit_reference_jobs.py — Submits reference pipeline jobs to AWS Batch

Submits one Batch job per year. Each job downloads TIFFs and shapefiles
from S3, matches them by date, computes NDVI/NDWI-11/MSI-11 zonal stats,
and uploads a Parquet to output/reference/{year}/.

Usage:
    python submit_reference_jobs.py                        # auto-discover years from S3
    python submit_reference_jobs.py --years 2020 2021 2022 # specific years
"""

import argparse
import boto3
import json
from datetime import datetime
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
S3_BUCKET     = "ndvi-extraction"
S3_REF_PREFIX = "Reference/"
JOB_QUEUE     = "ndvi-extraction-queue"
JOB_DEFINITION = "ndvi-extraction-job"
REGION        = "us-east-1"


def list_years(s3):
    """Discover year subfolders under Reference/ (pattern: {year}-raster)."""
    result   = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_REF_PREFIX, Delimiter="/")
    prefixes = result.get('CommonPrefixes', [])
    years    = []
    for p in prefixes:
        part = p['Prefix'].rstrip('/').split('/')[-1]
        if part.endswith('-raster') and part.replace('-raster', '').isdigit():
            years.append(part.replace('-raster', ''))
    return sorted(years)


def main():
    parser = argparse.ArgumentParser(description="Submit reference pipeline jobs to AWS Batch")
    parser.add_argument('--years', nargs='+', type=str, default=None,
                        help='Years to process (default: auto-discover from S3)')
    args = parser.parse_args()

    s3    = boto3.client('s3',    region_name=REGION)
    batch = boto3.client('batch', region_name=REGION)

    years = args.years or list_years(s3)
    if not years:
        print(f"No year subfolders found under s3://{S3_BUCKET}/{S3_REF_PREFIX}")
        return

    print(f"Submitting {len(years)} reference job(s) to queue '{JOB_QUEUE}'")
    print(f"Years: {years}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    submitted = []

    for year in years:
        job_name = f"ndvi-ref-{year}"[:128]

        response = batch.submit_job(
            jobName=job_name,
            jobQueue=JOB_QUEUE,
            jobDefinition=JOB_DEFINITION,
            containerOverrides={
                'environment': [
                    {'name': 'SCRIPT_TYPE', 'value': 'reference'},
                    {'name': 'YEAR',        'value': year},
                    {'name': 'S3_BUCKET',   'value': S3_BUCKET},
                    {'name': 'S3_PREFIX',   'value': 'output/reference'},
                ]
            }
        )

        job_id = response['jobId']
        submitted.append({'job_id': job_id, 'job_name': job_name, 'year': year})
        print(f"  Submitted: {job_name}")
        print(f"  Job ID:    {job_id}")
        print(f"  Year:      {year}\n")

    log_path = Path(__file__).parent / f"job_ids_reference_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_path, 'w') as f:
        json.dump(submitted, f, indent=2)

    print(f"Job IDs saved to: {log_path}")
    print(f"\nMonitor logs:")
    print(f"  AWS Console → CloudWatch → Log groups → /aws/batch/ndvi-extraction")


if __name__ == '__main__':
    main()
