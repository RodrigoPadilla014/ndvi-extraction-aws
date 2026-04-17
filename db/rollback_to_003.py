"""
rollback_to_003.py — Reverts DB state to after migration 003.

Drops changes from 004, 005, 006:
  - Drops gap_in_data column (006)
  - Resets edad_de_cultivo to NULL and re-applies 003 logic (undoes 004, 005)
  - Removes 004, 005, 006 from _migrations tracking table

Migration SQL files are NOT deleted — they can be re-applied later.
"""

import os
import socket
import subprocess
import time
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / ".env")

SSH_HOST = os.environ["SSH_HOST"]
SSH_USER = os.environ["SSH_USER"]
SSH_KEY  = PROJECT_ROOT / os.environ["SSH_KEY"]
DB_HOST  = os.environ["DB_HOST"]
DB_PORT  = int(os.environ["DB_PORT"])
DB_NAME  = os.environ["DB_NAME"]
DB_USER  = os.environ["DB_USER"]
DB_PASS  = os.environ["DB_PASSWORD"]

MIGRATION_003 = (PROJECT_ROOT / "db" / "migrations" / "003_add_edad_de_cultivo.sql").read_text()


def open_tunnel():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    proc = subprocess.Popen([
        "ssh", "-N", "-L", f"{port}:{DB_HOST}:{DB_PORT}",
        "-i", str(SSH_KEY), "-o", "StrictHostKeyChecking=no",
        "-o", "BatchMode=yes", "-p", "22", f"{SSH_USER}@{SSH_HOST}",
    ])
    time.sleep(3)
    return proc, port


def main():
    tunnel, port = open_tunnel()
    try:
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@127.0.0.1:{port}/{DB_NAME}")
        with engine.begin() as conn:

            print("Step 1: Drop gap_in_data column (undo 006)...")
            conn.execute(text("ALTER TABLE maestra DROP COLUMN IF EXISTS gap_in_data"))
            print("  Done.")

            print("Step 2: Reset edad_de_cultivo to NULL (undo 005, 004)...")
            conn.execute(text("UPDATE maestra SET edad_de_cultivo = NULL"))
            print("  Done.")

            print("Step 3: Re-apply 003 computation...")
            conn.execute(text(MIGRATION_003))
            print("  Done.")

            print("Step 4: Remove 004, 005, 006 from _migrations...")
            conn.execute(text("""
                DELETE FROM _migrations
                WHERE filename IN (
                    '004_fix_edad_de_cultivo.sql',
                    '005_fix_edad_de_cultivo_ratoon.sql',
                    '006_add_gap_in_data.sql'
                )
            """))
            print("  Done.")

            # Verify
            applied = [r[0] for r in conn.execute(text("SELECT filename FROM _migrations ORDER BY filename")).fetchall()]
            print(f"\nMigrations now applied: {applied}")

            count = conn.execute(text("SELECT COUNT(*) FROM maestra WHERE edad_de_cultivo IS NOT NULL")).scalar()
            print(f"Rows with edad_de_cultivo: {count:,}")

        print("\nRollback to 003 complete.")
    finally:
        tunnel.terminate()


if __name__ == "__main__":
    main()
