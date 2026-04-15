-- 003_add_edad_de_cultivo.sql
-- Add edad_de_cultivo (age of crop in days) to maestra.
-- Computed as: fecha - cierre of the previous zafra for the same cod_cg.
-- cierre marks the end of one cycle and the start of the next (ratoon logic).
-- Rows where no previous cierre exists (first cycle per lot) are left NULL.
-- 2026-04-15

ALTER TABLE maestra ADD COLUMN IF NOT EXISTS edad_de_cultivo integer;

WITH distinct_cierre AS (
    SELECT DISTINCT cod_cg, zafra, cierre
    FROM maestra
    WHERE cierre IS NOT NULL
),
lagged_cierre AS (
    SELECT
        cod_cg, zafra,
        LAG(cierre) OVER (PARTITION BY cod_cg ORDER BY zafra) AS prev_cierre
    FROM distinct_cierre
)
UPDATE maestra m
SET edad_de_cultivo = m.fecha::date - TO_DATE(lc.prev_cierre, 'DD/MM/YYYY')
FROM lagged_cierre lc
WHERE m.cod_cg = lc.cod_cg
  AND m.zafra = lc.zafra
  AND lc.prev_cierre IS NOT NULL;
