-- 010_add_renovation_cycles.sql
-- Detects renovation points in NULL-cierre segments (lots missing from productividad)
-- and assigns those dates as cierre_ciclo boundaries.
-- Then recalculates edad_de_cultivo and gap_in_data.
-- 2026-04-16

-- Step 1: compute renovation points for NULL cierre segments
-- A renovation = edad > 200 AND ndvi_ref < 0.2, with a recovery before it (LAG over ALL obs)
CREATE TEMP TABLE renov_points AS
WITH all_null AS (
    SELECT cod_cg, fecha::date AS fecha, ndvi_ref, edad_de_cultivo,
           LAG(ndvi_ref) OVER (PARTITION BY cod_cg ORDER BY fecha) AS prev_ndvi
    FROM maestra
    WHERE cierre_ciclo IS NULL
      AND gap_in_data = FALSE
),
null_candidates AS (
    SELECT cod_cg, fecha, ndvi_ref, edad_de_cultivo,
           SUM(CASE WHEN prev_ndvi IS NULL OR prev_ndvi >= 0.2 THEN 1 ELSE 0 END)
               OVER (PARTITION BY cod_cg ORDER BY fecha) AS event_id
    FROM all_null
    WHERE edad_de_cultivo > 200 AND ndvi_ref < 0.2
)
SELECT DISTINCT ON (cod_cg, event_id)
       cod_cg, fecha AS renov_date
FROM null_candidates
ORDER BY cod_cg, event_id, fecha;

-- Step 2: assign cierre_ciclo = next renovation date for NULL-cierre observations
UPDATE maestra m
SET cierre_ciclo = sub.renov_date
FROM (
    SELECT m2.cod_cg, m2.fecha::date AS fecha,
           MIN(rp.renov_date) AS renov_date
    FROM maestra m2
    JOIN renov_points rp
      ON rp.cod_cg = m2.cod_cg
     AND rp.renov_date >= m2.fecha::date
    WHERE m2.cierre_ciclo IS NULL
    GROUP BY m2.cod_cg, m2.fecha
) sub
WHERE m.cod_cg = sub.cod_cg
  AND m.fecha::date = sub.fecha;

DROP TABLE renov_points;

-- Step 3: recalculate edad_de_cultivo with updated cierre_ciclo
UPDATE maestra SET edad_de_cultivo = NULL;

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

-- Step 4: recalculate gap_in_data with updated cierre_ciclo
UPDATE maestra SET gap_in_data = FALSE;

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
