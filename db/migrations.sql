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

-- 2026-04-14: Create maestra table — LEFT JOIN stac_corrected_indices with productividad
-- Join key: cod_cg_zafra; productividad.zafra aliased as zafra_prod to avoid conflict

DROP TABLE IF EXISTS maestra;

CREATE TABLE maestra AS
SELECT
    s.*,
    p.ingenio, p.zafra AS zafra_prod, p.zae, p.grupo_de_suelo, p.grupo_de_humedad, p.codigo_zae,
    p.lote, p.semana, p.finca, p.familia_de_suelo, p.variedad, p.area,
    p.tc, p.tch, p.rendimiento, p.tah, p.brix, p.pureza, p.jugo, p.ph,
    p.pol, p.fibra, p.humedad, p.edad, p.no_corte, p.nitrogeno, p.potasio,
    p.fosforo, p.cachaza, p.vinaza, p.sulfato, p.urea_nitro_exted,
    p.aplicaciones_foliares, p.riego, p.total_riego_aplicado_mm,
    p.numero_de_riegos, p.dias_ultimo_riego, p.pre_incorporado,
    p.pre_emergente, p.post_emergente, p.pre_post_emergente,
    p.ultimo_control_de_malezas, p.bejuco, p.parchoneo, p.arranque,
    p.tipo_aplicacion_control_malezas, p.precipitacion, p.temp_minima,
    p.radiacion_solar, p.inhibidor_de_floracion, p.premadurante,
    p.madurante, p.tipo_de_aplicacion_madurante, p.dias_madurantes,
    p.horas_quema, p.tipo_quema, p.para_cosecha_en_verde, p.cierre,
    p.mes_de_cosecha, p.cosecha, p.de_infestacion_barrenador,
    p.tipo_de_control_para_barrenador, p.de_infestacion_de_roedores,
    p.tipo_de_control_para_roedores, p.chinche_salivosa_ninfas_tallo,
    p.chinche_salivosa_adultos_tallo, p.tipo_de_control_de_chinche,
    p.latitud, p.longitud, p.zona_longitudinal, p.estrato,
    p.ultima_imagen, p.ndvi, p.ndwi_11, p.ndwi_12, p.msi_11, p.msi_12
FROM stac_corrected_indices s
LEFT JOIN productividad p ON s.cod_cg_zafra = p.cod_cg_zafra;

-- 2026-04-15: Add edad_de_cultivo column to maestra
-- Age of crop in days: fecha - cierre of the previous zafra for the same cod_cg
-- cierre marks end of a cycle and start of the next; first cycle per lot gets NULL

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
