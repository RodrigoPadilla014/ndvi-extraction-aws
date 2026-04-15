-- 005_fix_edad_de_cultivo_ratoon.sql
-- Fix edad_de_cultivo to handle post-harvest (ratoon) images correctly.
-- Pre-harvest  (fecha <= cierre): edad = fecha - prev_cierre (age since last cut)
-- Post-harvest (fecha >  cierre): edad = fecha - cierre      (age of new ratoon)
-- No cierre data: NULL
-- 2026-04-15

UPDATE maestra SET edad_de_cultivo = NULL;

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
SET edad_de_cultivo = CASE
    WHEN m.cierre IS NULL
        THEN NULL
    WHEN m.fecha::date > TO_DATE(m.cierre, 'DD/MM/YYYY')
        THEN m.fecha::date - TO_DATE(m.cierre, 'DD/MM/YYYY')
    WHEN lc.prev_cierre IS NOT NULL
        THEN m.fecha::date - TO_DATE(lc.prev_cierre, 'DD/MM/YYYY')
    ELSE NULL
END
FROM lagged_cierre lc
WHERE m.cod_cg = lc.cod_cg
  AND m.zafra = lc.zafra;
