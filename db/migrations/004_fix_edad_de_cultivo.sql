-- 004_fix_edad_de_cultivo.sql
-- Fix edad_de_cultivo to only use the immediately previous zafra's cierre.
-- Previous version skipped over zafras with NULL cierre, reaching too far back.
-- Fix: LAG over ALL zafras (including NULL cierre ones) so gaps produce NULL
-- instead of jumping multiple seasons back.
-- 2026-04-15

-- Reset all existing values
UPDATE maestra SET edad_de_cultivo = NULL;

-- Recompute using all zafras in the window, not just those with cierre
WITH all_zafras AS (
    SELECT DISTINCT cod_cg, zafra, cierre
    FROM maestra
),
lagged_cierre AS (
    SELECT
        cod_cg, zafra,
        LAG(cierre) OVER (PARTITION BY cod_cg ORDER BY zafra) AS prev_cierre
    FROM all_zafras
)
UPDATE maestra m
SET edad_de_cultivo = m.fecha::date - TO_DATE(lc.prev_cierre, 'DD/MM/YYYY')
FROM lagged_cierre lc
WHERE m.cod_cg = lc.cod_cg
  AND m.zafra = lc.zafra
  AND lc.prev_cierre IS NOT NULL;
