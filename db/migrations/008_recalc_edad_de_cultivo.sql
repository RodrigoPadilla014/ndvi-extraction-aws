-- 008_recalc_edad_de_cultivo.sql
-- Recalculates edad_de_cultivo using cierre_ciclo (migration 007).
-- edad = fecha - previous cierre_ciclo (the last harvest date before this observation).
-- This replaces the zafra-calendar-based logic from migration 003.
-- 2026-04-16

UPDATE maestra SET edad_de_cultivo = NULL;

-- Step 1: build one row per (cod_cg, cierre_ciclo) with its previous cierre_ciclo.
-- Deduplicate BEFORE applying LAG to avoid non-deterministic ordering ties.
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

-- Step 2: observations after the last known cierre (cierre_ciclo IS NULL).
-- Their cycle started at the last cierre for that lot.
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
