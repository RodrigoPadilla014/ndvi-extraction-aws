-- 011_add_ciclo_valido.sql
-- Adds ciclo_valido boolean to maestra.
-- A cycle is valid for analysis/ML if:
--   - cierre_ciclo IS NOT NULL  (known end — from productividad or renovation detection)
--   - edad_de_cultivo IS NOT NULL (known start — prev cierre exists)
--   - gap_in_data = FALSE (no missing zafra in history)
--   - >= 7 observations in the cycle
--   - MAX(edad_de_cultivo) >= 150 days (cycle was observed long enough)
-- 2026-04-16

ALTER TABLE maestra ADD COLUMN IF NOT EXISTS ciclo_valido boolean DEFAULT FALSE;

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
