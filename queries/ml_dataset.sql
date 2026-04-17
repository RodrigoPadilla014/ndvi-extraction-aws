-- ml_dataset.sql
-- Fetches the preliminary ML training dataset.
--
-- Unit: one row per (lot, observation date) within a valid cycle.
-- Filters:
--   ciclo_valido = TRUE  — clean cycle (known start+end, no gap, >=7 obs, max edad >=150)
--   cierre IS NOT NULL   — has a yield label from productividad
--
-- Target: tch (toneladas de caña por hectárea) — yield normalized by area
-- NDVI source: ndvi_ref (Reference pipeline, more reliable than corrected STAC)

SELECT *
FROM maestra
WHERE ciclo_valido = TRUE
  AND cierre IS NOT NULL
;
