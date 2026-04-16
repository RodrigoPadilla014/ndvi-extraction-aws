-- 007_add_cierre_ciclo.sql
-- Adds cierre_ciclo (date) to maestra: the next harvest date >= fecha for each lot.
-- This defines the agronomic cycle each observation belongs to, independent of
-- the Nov-Oct zafra calendar used in the original join.
-- 2026-04-16

ALTER TABLE maestra ADD COLUMN IF NOT EXISTS cierre_ciclo date;

WITH cierres AS (
    SELECT DISTINCT cod_cg,
           TO_DATE(cierre, 'DD/MM/YYYY') AS cierre_date
    FROM maestra
    WHERE cierre IS NOT NULL
),
next_cierre AS (
    SELECT m.cod_cg,
           m.fecha::date AS fecha,
           MIN(c.cierre_date) AS cierre_ciclo
    FROM maestra m
    LEFT JOIN cierres c
           ON c.cod_cg = m.cod_cg
          AND c.cierre_date >= m.fecha::date
    GROUP BY m.cod_cg, m.fecha
)
UPDATE maestra m
SET cierre_ciclo = nc.cierre_ciclo
FROM next_cierre nc
WHERE m.cod_cg = nc.cod_cg
  AND m.fecha::date = nc.fecha;
