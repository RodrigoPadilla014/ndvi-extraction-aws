-- 006_add_gap_in_data.sql
-- Add gap_in_data flag to maestra.
-- A lot is flagged when its immediately preceding zafra is missing from productividad.
-- Flagged rows get edad_de_cultivo = NULL since the start of the cycle is unknown.
-- 2026-04-15

ALTER TABLE maestra ADD COLUMN IF NOT EXISTS gap_in_data boolean NOT NULL DEFAULT FALSE;

-- Get all (cod_cg, zafra) combinations present in productividad
-- (identified by cierre IS NOT NULL in maestra, since cierre comes from productividad)
-- For each, derive what the expected previous zafra should be and check if it exists
WITH zafras_in_prod AS (
    SELECT DISTINCT cod_cg, zafra
    FROM maestra
    WHERE cierre IS NOT NULL
),
zafra_with_prev AS (
    SELECT
        cod_cg,
        zafra,
        -- Derive expected previous zafra from current zafra text (e.g. 2021_2022 -> 2020_2021)
        (SPLIT_PART(zafra, '_', 1)::int - 1)::text || '_' || SPLIT_PART(zafra, '_', 1) AS expected_prev_zafra
    FROM zafras_in_prod
),
gaps AS (
    SELECT z.cod_cg, z.zafra
    FROM zafra_with_prev z
    -- Flag if the expected previous zafra does NOT exist for this cod_cg in productividad
    WHERE NOT EXISTS (
        SELECT 1 FROM zafras_in_prod p
        WHERE p.cod_cg = z.cod_cg
          AND p.zafra = z.expected_prev_zafra
    )
    -- Exclude the very first zafra per lot (no previous expected)
    AND EXISTS (
        SELECT 1 FROM zafras_in_prod p
        WHERE p.cod_cg = z.cod_cg
          AND p.zafra < z.zafra
    )
)
UPDATE maestra m
SET gap_in_data = TRUE
FROM gaps g
WHERE m.cod_cg = g.cod_cg
  AND m.zafra = g.zafra;

-- NULL out edad_de_cultivo for flagged rows
UPDATE maestra
SET edad_de_cultivo = NULL
WHERE gap_in_data = TRUE;
