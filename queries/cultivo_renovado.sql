-- cultivo_renovado.sql
-- Detects "cultivo renovado" events: cycles where edad_de_cultivo > 300
-- and ndvi_ref drops below 0.2, indicating the land was renovated.
-- The first date that satisfies both conditions is taken as the renovation date
-- (effective new cierre). Uses ndvi_ref as the reliable source.
-- 2026-04-16

WITH renovations AS (
    SELECT
        cod_cg,
        cierre,
        MIN(fecha::date)        AS renovation_date,
        MIN(edad_de_cultivo)    AS edad_at_renovation,
        MIN(ndvi_ref)           AS min_ndvi_ref
    FROM maestra
    WHERE gap_in_data      = FALSE
      AND edad_de_cultivo  > 300
      AND ndvi_ref         < 0.2
      AND ndvi_ref         IS NOT NULL
    GROUP BY cod_cg, cierre
)
SELECT
    cod_cg,
    cierre,
    renovation_date,
    edad_at_renovation,
    ROUND(min_ndvi_ref::numeric, 4) AS min_ndvi_ref
FROM renovations
ORDER BY cod_cg, cierre;
