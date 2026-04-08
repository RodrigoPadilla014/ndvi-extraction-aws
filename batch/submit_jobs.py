# -*- coding: utf-8 -*-
"""
submit_jobs.py — Submits STAC pipeline jobs to AWS Batch

Lists .shp files from S3 under STAC/{year}/ and submits one Batch job per
shapefile. Each job processes one date: downloads the shapefile from S3,
queries STAC for Sentinel-2 imagery, computes NDVI/NDWI-11/MSI-11, and
uploads a Parquet to output/stac/{year}/.

Usage:
    python submit_jobs.py                          # all years found in S3
    python submit_jobs.py --years 2020 2021        # specific years
    python submit_jobs.py --years 2020 --jobs 5   # first 5 jobs only (dry-run test)
"""

import argparse
import boto3
import json
from datetime import datetime
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
S3_BUCKET      = "ndvi-extraction"
S3_STAC_PREFIX = "STAC/"
JOB_QUEUE      = "ndvi-extraction-queue"
JOB_DEFINITION = "ndvi-extraction-job"
REGION         = "us-east-1"


def list_years(s3):
    """Discover year subfolders under STAC/ in S3."""
    result   = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_STAC_PREFIX, Delimiter="/")
    prefixes = result.get('CommonPrefixes', [])
    years    = []
    for p in prefixes:
        part = p['Prefix'].rstrip('/').split('/')[-1]
        if part.isdigit():
            years.append(part)
    return sorted(years)


def main():
    parser = argparse.ArgumentParser(description="Submit STAC pipeline jobs to AWS Batch")
    parser.add_argument('--years', nargs='+', type=str, default=None,
                        help='Years to process (default: auto-discover from S3)')
    parser.add_argument('--jobs', type=int, default=None,
                        help='Max number of jobs to submit (default: all)')
    args = parser.parse_args()

    s3    = boto3.client('s3',    region_name=REGION)
    batch = boto3.client('batch', region_name=REGION)

    years = args.years or list_years(s3)
    if not years:
        print(f"No year subfolders found under s3://{S3_BUCKET}/{S3_STAC_PREFIX}")
        return

    print(f"Years to process: {years}")

    # Collect all .shp keys across all years
    shp_keys = []
    for year in years:
        prefix   = f"{S3_STAC_PREFIX}{year}/"
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        year_shps = [
            obj['Key'] for obj in response.get('Contents', [])
            if obj['Key'].endswith('.shp')
        ]
        shp_keys.extend(sorted(year_shps))
        print(f"  {year}: {len(year_shps)} shapefile(s)")

    if not shp_keys:
        print(f"No .shp files found.")
        return

    if args.jobs:
        shp_keys = shp_keys[:args.jobs]

    print(f"\nSubmitting {len(shp_keys)} job(s) to queue '{JOB_QUEUE}'")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    submitted = []

    for shp_key in shp_keys:
        shp_name = Path(shp_key).stem
        job_name = f"ndvi-stac-{shp_name}"[:128]
        shp_path = f"s3://{S3_BUCKET}/{shp_key}"

        response = batch.submit_job(
            jobName=job_name,
            jobQueue=JOB_QUEUE,
            jobDefinition=JOB_DEFINITION,
            containerOverrides={
                'environment': [
                    {'name': 'SCRIPT_TYPE', 'value': 'shapefile'},
                    {'name': 'SHP_PATH',    'value': shp_path},
                    {'name': 'S3_BUCKET',   'value': S3_BUCKET},
                    {'name': 'S3_PREFIX',   'value': 'output/stac'},
                ]
            }
        )

        job_id = response['jobId']
        submitted.append({'job_id': job_id, 'job_name': job_name, 'shp': shp_path})
        print(f"  Submitted: {job_name}")
        print(f"  Job ID:    {job_id}")
        print(f"  SHP:       {shp_path}\n")

    log_path = Path(__file__).parent / f"job_ids_stac_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_path, 'w') as f:
        json.dump(submitted, f, indent=2)

    print(f"Job IDs saved to: {log_path}")
    print(f"\nMonitor logs:")
    print(f"  AWS Console → CloudWatch → Log groups → /aws/batch/ndvi-extraction")


if __name__ == '__main__':
    main()
