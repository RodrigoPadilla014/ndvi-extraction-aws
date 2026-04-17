"""
repair_after_004_006.py — Restores correct edad_de_cultivo, gap_in_data, ciclo_valido.

Migrations 004, 005, 006 ran unexpectedly and overwrote edad_de_cultivo / gap_in_data
with the old zafra-calendar logic, undoing migrations 008, 009, 010.
cierre_ciclo was NOT touched, so we just re-run the cierre_ciclo-based recalculations.
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
SSH_PORT = int(os.environ["SSH_PORT"])
SSH_USER = os.environ["SSH_USER"]
SSH_KEY  = PROJECT_ROOT / os.environ["SSH_KEY"]
DB_HOST     = os.environ["DB_HOST"]
DB_PORT     = int(os.environ["DB_PORT"])
DB_NAME     = os.environ["DB_NAME"]
DB_USER     = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]


STEPS = [
    ("Reset edad_de_cultivo to NULL", """
        UPDATE maestra SET edad_de_cultivo = NULL;
    """),

    ("Recalc edad for known cierre_ciclo rows (from 008/010 step 3)", """
        WITH distinct_cycles AS (
            SELECT DISTINCT cod_cg, cierre_ciclo
            FROM maestra
            WHERE cierre_ciclo IS NOT NULL
        ),
        with_prev AS (
            SELECT cod_cg, cierre_ciclo,
                   LAG(cierre_ciclo) OVER (PARTITION BY cod_cg ORDER BY cierre_ciclo) AS prev_cierre
            FROM distinct_cycles
        )
        UPDATE maestra m
        SET edad_de_cultivo = m.fecha::date - wp.prev_cierre
        FROM with_prev wp
        WHERE m.cod_cg = wp.cod_cg
          AND m.cierre_ciclo = wp.cierre_ciclo
          AND wp.prev_cierre IS NOT NULL;
    """),

    ("Recalc edad for NULL cierre_ciclo rows (last active cycle)", """
        WITH last_cierre AS (
            SELECT cod_cg, MAX(cierre_ciclo) AS last_cierre_ciclo
            FROM maestra
            WHERE cierre_ciclo IS NOT NULL
            GROUP BY cod_cg
        )
        UPDATE maestra m
        SET edad_de_cultivo = m.fecha::date - lc.last_cierre_ciclo
        FROM last_cierre lc
        WHERE m.cod_cg = lc.cod_cg
          AND m.cierre_ciclo IS NULL;
    """),

    ("Reset gap_in_data to FALSE", """
        UPDATE maestra SET gap_in_data = FALSE;
    """),

    ("Recalc gap_in_data (cierre_ciclo gap > 548 days)", """
        WITH distinct_cycles AS (
            SELECT DISTINCT cod_cg, cierre_ciclo
            FROM maestra
            WHERE cierre_ciclo IS NOT NULL
        ),
        with_prev AS (
            SELECT cod_cg, cierre_ciclo,
                   LAG(cierre_ciclo) OVER (PARTITION BY cod_cg ORDER BY cierre_ciclo) AS prev_cierre
            FROM distinct_cycles
        ),
        flagged_cycles AS (
            SELECT cod_cg, cierre_ciclo
            FROM with_prev
            WHERE prev_cierre IS NOT NULL
              AND (cierre_ciclo - prev_cierre) > 548
        )
        UPDATE maestra m
        SET gap_in_data = TRUE,
            edad_de_cultivo = NULL
        FROM flagged_cycles fc
        WHERE m.cod_cg = fc.cod_cg
          AND m.cierre_ciclo = fc.cierre_ciclo;
    """),

    ("Reset ciclo_valido to FALSE", """
        UPDATE maestra SET ciclo_valido = FALSE;
    """),

    ("Recompute ciclo_valido (from 011)", """
        WITH valid_cycles AS (
            SELECT cod_cg, cierre_ciclo
            FROM maestra
            WHERE cierre_ciclo IS NOT NULL
              AND edad_de_cultivo IS NOT NULL
              AND gap_in_data = FALSE
            GROUP BY cod_cg, cierre_ciclo
            HAVING COUNT(*) >= 7
               AND MAX(edad_de_cultivo) >= 150
        )
        UPDATE maestra m
        SET ciclo_valido = TRUE
        FROM valid_cycles vc
        WHERE m.cod_cg = vc.cod_cg
          AND m.cierre_ciclo = vc.cierre_ciclo;
    """),
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
            for label, sql in STEPS:
                print(f"  {label}...")
                result = conn.execute(text(sql))
                print(f"    {result.rowcount} rows affected.")

        print("\nRepair complete.")

    finally:
        tunnel.terminate()


if __name__ == "__main__":
    main()
