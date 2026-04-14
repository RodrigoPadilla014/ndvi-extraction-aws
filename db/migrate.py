"""
migrate.py — Apply migrations.sql to PostgreSQL via SSH tunnel.

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
MIGRATIONS_FILE = Path(__file__).parent / "migrations.sql"

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


def find_free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main() -> None:
    sql = MIGRATIONS_FILE.read_text()

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
            conn.execute(text(sql))
        print("Migrations applied successfully.")
    finally:
        tunnel.terminate()


if __name__ == "__main__":
    main()
