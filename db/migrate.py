"""
migrate.py — Apply pending migrations to PostgreSQL via SSH tunnel.

Migrations live in db/migrations/ as numbered SQL files (e.g. 001_..., 002_...).
Applied migrations are tracked in a _migrations table in the DB, so each file
runs exactly once. Safe to run repeatedly — already-applied files are skipped.

Run from project root:
    python db/migrate.py
"""

import os
import socket
import subprocess
import time
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).parent.parent
MIGRATIONS_DIR = Path(__file__).parent / "migrations"

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

# Migrations already applied to the DB before this tracking system was introduced.
BASELINE = [
    "001_add_zafra_columns.sql",
    "002_create_maestra.sql",
]


def find_free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main() -> None:
    local_port = find_free_port()
    print(f"Opening SSH tunnel to {SSH_HOST} on local port {local_port}...")
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

        with engine.begin() as conn:
            # Create tracking table if it doesn't exist
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS _migrations (
                    filename   text        PRIMARY KEY,
                    applied_at timestamptz NOT NULL DEFAULT now()
                )
            """))

            # On first run, record baseline migrations as already applied
            count = conn.execute(text("SELECT COUNT(*) FROM _migrations")).scalar()
            if count == 0:
                for filename in BASELINE:
                    conn.execute(
                        text("INSERT INTO _migrations (filename) VALUES (:f)"),
                        {"f": filename},
                    )
                print(f"Baseline recorded: {BASELINE}")

            # Determine what has already been applied
            applied = {
                row[0] for row in conn.execute(text("SELECT filename FROM _migrations"))
            }

            # Find and apply pending migrations in order
            all_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
            pending = [p for p in all_files if p.name not in applied]

            if not pending:
                print("Nothing to apply — DB is up to date.")
                return

            for path in pending:
                print(f"Applying {path.name}...")
                conn.execute(text(path.read_text()))
                conn.execute(
                    text("INSERT INTO _migrations (filename) VALUES (:f)"),
                    {"f": path.name},
                )
                print(f"  Done.")

        print("\nAll migrations applied successfully.")

    finally:
        tunnel.terminate()


if __name__ == "__main__":
    main()
