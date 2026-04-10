"""
upload_to_db.py — Upload corrected STAC indices to PostgreSQL via SSH tunnel.

Table: stac_corrected_indices
Columns: lote, fecha, ndvi_corrected, ndvi_ref, ndwi11_corrected, ndwi11_ref, msi11_corrected, msi11_ref

Credentials are read from .env in the project root. Copy .env.example to .env and fill in your values.

Run from project root:
    python db/upload_to_db.py
"""

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder

PROJECT_ROOT = Path(__file__).parent.parent
INPUT_PATH = PROJECT_ROOT / "eda" / "corrected_output.csv"

load_dotenv(PROJECT_ROOT / ".env")

SSH_HOST = os.environ["SSH_HOST"]
SSH_PORT = int(os.environ["SSH_PORT"])
SSH_USER = os.environ["SSH_USER"]
SSH_KEY  = PROJECT_ROOT / os.environ["SSH_KEY"]

DB_HOST     = os.environ["DB_HOST"]
DB_PORT     = int(os.environ["DB_PORT"])
DB_NAME     = os.environ["DB_NAME"]
DB_USER     = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

TABLE_NAME = "stac_corrected_indices"

COLUMNS = [
    "lote", "fecha",
    "ndvi_corrected", "ndvi_ref",
    "ndwi11_corrected", "ndwi11_ref",
    "msi11_corrected", "msi11_ref",
]


def main() -> None:
    print(f"Reading {INPUT_PATH}...")
    df = pd.read_csv(INPUT_PATH, usecols=COLUMNS, parse_dates=["fecha"])
    print(f"  {len(df):,} rows")

    print(f"Opening SSH tunnel to {SSH_HOST}...")
    with SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USER,
        ssh_pkey=str(SSH_KEY),
        remote_bind_address=(DB_HOST, DB_PORT),
    ) as tunnel:
        local_port = tunnel.local_bind_port
        print(f"  Tunnel open on local port {local_port}")

        db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{local_port}/{DB_NAME}"
        engine = create_engine(db_url)

        print(f"Uploading to {TABLE_NAME}...")
        df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False, chunksize=10_000)
        print(f"Done. {len(df):,} rows written to {TABLE_NAME}.")


if __name__ == "__main__":
    main()
