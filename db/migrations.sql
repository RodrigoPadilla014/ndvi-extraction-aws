-- migrations.sql
-- Schema changes applied to DB_Lake. Run via db/migrate.py.
-- Each migration is idempotent where possible.

-- 2026-04-14: Add zafra and cod_cg_zafra columns to stac_corrected_indices
-- zafra is derived from fecha: Nov–Oct defines a season (e.g. Nov 2020–Oct 2021 = 2020_2021)

ALTER TABLE stac_corrected_indices ADD COLUMN IF NOT EXISTS zafra text;

UPDATE stac_corrected_indices
SET zafra = CASE
    WHEN EXTRACT(MONTH FROM fecha) >= 11
        THEN EXTRACT(YEAR FROM fecha)::int::text || '_' || (EXTRACT(YEAR FROM fecha)::int + 1)::text
    ELSE
        (EXTRACT(YEAR FROM fecha)::int - 1)::text || '_' || EXTRACT(YEAR FROM fecha)::int::text
END;

ALTER TABLE stac_corrected_indices ADD COLUMN IF NOT EXISTS cod_cg_zafra text;

UPDATE stac_corrected_indices
SET cod_cg_zafra = cod_cg || '_' || zafra;

-- 2026-04-14: Add lote_zafra column to productividad
-- zafra in productividad is stored as "Zafra 2019-2020"; strip prefix and replace hyphen with underscore

ALTER TABLE productividad ADD COLUMN IF NOT EXISTS lote_zafra text;

UPDATE productividad
SET lote_zafra = lote || '_' || REPLACE(SUBSTRING(zafra FROM 7), '-', '_');

ALTER TABLE productividad RENAME COLUMN lote_zafra TO cod_cg_zafra;
