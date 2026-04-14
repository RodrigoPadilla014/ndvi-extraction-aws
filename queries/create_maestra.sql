-- create_maestra.sql
-- Creates the maestra table by left joining stac_corrected_indices with productividad.
-- Join key: cod_cg_zafra (lot ID + zafra season, e.g. "07-0600308_2020_2021")
-- Duplicate columns (zafra, cod_cg_zafra) are taken from the left table only.
-- 2026-04-14

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
