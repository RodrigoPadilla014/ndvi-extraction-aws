"""
upload_productividad.py — Parse and upload reporteIngenios XLS (HTML) to PostgreSQL.

Table: productividad
Source: HTML-based .xls export (multi-file workbook, data in sheet001.htm)

Run from project root:
    python db/upload_productividad.py path/to/sheet001.htm
"""

import os
import re
import socket
import subprocess
import sys
import time
import unicodedata
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

PROJECT_ROOT = Path(__file__).parent.parent
TABLE_NAME = "productividad"
CHUNK_SIZE = 10_000

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


def slugify(name: str) -> str:
    """Normalize a column name to a safe SQL identifier."""
    # Normalize unicode (e.g. á → a)
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")
    # Lowercase, replace spaces and special chars with underscore
    name = re.sub(r"[^\w]+", "_", name.lower()).strip("_")
    return name


def parse_sheet(path: str) -> pd.DataFrame:
    print(f"Reading {path}  ({Path(path).stat().st_size / 1024**2:.1f} MB)...")

    with open(path, encoding="utf-8", errors="replace") as f:
        html = f.read()

    # Extract all <tr>...</tr> blocks
    row_pattern = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL | re.IGNORECASE)
    cell_pattern = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.DOTALL | re.IGNORECASE)
    tag_pattern  = re.compile(r"<[^>]+>")

    raw_rows = row_pattern.findall(html)
    print(f"  Found {len(raw_rows):,} raw rows in HTML.")

    def extract_cells(row_html: str) -> list[str]:
        cells = cell_pattern.findall(row_html)
        return [tag_pattern.sub("", c).replace("\xa0", " ").strip() for c in cells]

    # Find the header row: first row with enough non-empty cells
    header_idx = None
    headers = []
    for i, row in enumerate(raw_rows):
        cells = extract_cells(row)
        non_empty = [c for c in cells if c]
        if len(non_empty) >= 10:
            header_idx = i
            headers = cells
            break

    if header_idx is None:
        raise ValueError("Could not find header row in HTML.")

    print(f"  Header row at index {header_idx}: {len(headers)} columns.")

    # Slugify column names
    col_names = [slugify(h) if h else f"col_{i}" for i, h in enumerate(headers)]
    # Deduplicate
    seen: dict[str, int] = {}
    deduped = []
    for name in col_names:
        if name in seen:
            seen[name] += 1
            deduped.append(f"{name}_{seen[name]}")
        else:
            seen[name] = 0
            deduped.append(name)
    col_names = deduped

    # Parse data rows
    data = []
    for row in raw_rows[header_idx + 1:]:
        cells = extract_cells(row)
        if not any(cells):
            continue
        # Pad or trim to match header length
        if len(cells) < len(col_names):
            cells += [""] * (len(col_names) - len(cells))
        else:
            cells = cells[:len(col_names)]
        data.append(cells)

    print(f"  Parsed {len(data):,} data rows.")
    df = pd.DataFrame(data, columns=col_names)

    # Replace empty strings with NaN
    df.replace("", pd.NA, inplace=True)

    # Infer better types (numeric columns)
    df = df.infer_objects()
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except (ValueError, TypeError):
            pass

    print(f"  DataFrame shape: {df.shape}")
    print(f"  Columns: {col_names}")
    return df


def find_free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python db/upload_productividad.py <path/to/sheet001.htm>")
        sys.exit(1)

    sheet_path = sys.argv[1]
    df = parse_sheet(sheet_path)

    local_port = find_free_port()
    print(f"\nOpening SSH tunnel to {SSH_HOST} on local port {local_port}...")
    tunnel = subprocess.Popen([
        "ssh", "-N",
        "-L", f"{local_port}:{DB_HOST}:{DB_PORT}",
        "-i", str(SSH_KEY),
        "-o", "StrictHostKeyChecking=no",
        "-o", "BatchMode=yes",
        "-p", str(SSH_PORT),
        f"{SSH_USER}@{SSH_HOST}",
    ])
    time.sleep(3)

    try:
        db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{local_port}/{DB_NAME}"
        engine = create_engine(db_url)

        print(f"Uploading {len(df):,} rows to table '{TABLE_NAME}'...")
        df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False, chunksize=CHUNK_SIZE)
        print(f"Done. {len(df):,} rows written to '{TABLE_NAME}'.")
    finally:
        tunnel.terminate()


if __name__ == "__main__":
    main()
