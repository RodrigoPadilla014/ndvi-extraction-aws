-- 009_add_gap_in_data.sql
-- Adds gap_in_data boolean to maestra.
-- Flags cycles where the gap between consecutive cierre_ciclo values exceeds
-- a normal harvest interval (threshold: 548 days / ~18 months).
-- For flagged rows, sets edad_de_cultivo = NULL.
-- 2026-04-16

ALTER TABLE maestra ADD COLUMN IF NOT EXISTS gap_in_data boolean DEFAULT FALSE;

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
