-- 30_p3_pollution : tables Silver Lakehouse air_capteur, air_mesure et vues métier P3 (Q1 à Q4)
-- Responsable : P3.

CREATE DATABASE IF NOT EXISTS urbanhub;

CREATE TABLE IF NOT EXISTS urbanhub.air_capteur (
    sensor_id        Int64,
    location_id      Int64,
    ville_id         LowCardinality(String),
    location_name    String,
    provider         LowCardinality(String) DEFAULT '',
    pollutant        LowCardinality(String),
    pollutant_label  LowCardinality(String),
    unit             LowCardinality(String),
    latitude         Float64,
    longitude        Float64,
    distance_m       Nullable(Float64)
)
ENGINE = ReplacingMergeTree
ORDER BY (ville_id, sensor_id);

CREATE TABLE IF NOT EXISTS urbanhub.air_mesure (
    sensor_id        Int64,
    ville_id         LowCardinality(String),
    location_id      Int64,
    location_name    String,
    pollutant        LowCardinality(String),
    pollutant_label  LowCardinality(String),
    timestamp        DateTime64(3, 'UTC'),
    heure_utc        DateTime64(3, 'UTC'),
    value            Float64,
    unit             LowCardinality(String),
    latitude         Float64,
    longitude        Float64,
    ratio_oms        Float32,
    depasse_oms      UInt8,
    depasse_episode  UInt8,
    quality_flags    String DEFAULT '',
    is_valid         UInt8 DEFAULT 1
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(heure_utc)
ORDER BY (ville_id, pollutant, sensor_id, timestamp);

-- Q1 : Zones et stations les plus polluées (classement ratio OMS 2021)
CREATE OR REPLACE VIEW urbanhub.v_air_zones_polluees AS
SELECT
    ville_id,
    location_id,
    anyLast(location_name) AS location_name,
    pollutant,
    anyLast(pollutant_label) AS pollutant_label,
    round(avg(value), 2) AS concentration_moyenne_ugm3,
    round(quantile(0.90)(value), 2) AS concentration_p90_ugm3,
    round(avg(ratio_oms), 2) AS ratio_oms_moyen,
    round(avg(depasse_oms) * 100.0, 1) AS pct_depassement_oms,
    count() AS nb_mesures
FROM urbanhub.air_mesure FINAL
WHERE is_valid = 1
GROUP BY ville_id, location_id, pollutant;

-- Q2 : Variation journalière (0h–23h heure locale, Semaine vs Week-end)
CREATE OR REPLACE VIEW urbanhub.v_air_variation_journaliere AS
SELECT
    ville_id,
    pollutant,
    anyLast(pollutant_label) AS pollutant_label,
    if(toDayOfWeek(toTimezone(timestamp, 'Europe/Paris')) IN (6, 7), 'Week-end', 'Semaine') AS type_jour,
    toHour(toTimezone(timestamp, 'Europe/Paris')) AS heure_locale,
    round(avg(value), 2) AS concentration_moyenne_ugm3,
    round(avg(ratio_oms), 2) AS ratio_oms_moyen,
    count() AS nb_mesures
FROM urbanhub.air_mesure FINAL
WHERE is_valid = 1
GROUP BY ville_id, pollutant, type_jour, heure_locale;

-- Q3 : Polluants dominants par ville (ratio OMS & concentration moyenne)
CREATE OR REPLACE VIEW urbanhub.v_air_polluants_dominants AS
SELECT
    ville_id,
    pollutant,
    anyLast(pollutant_label) AS pollutant_label,
    round(avg(value), 2) AS concentration_moyenne_ugm3,
    round(avg(ratio_oms), 2) AS ratio_oms_moyen,
    round(avg(depasse_oms) * 100.0, 1) AS pct_depassement_oms,
    count() AS nb_mesures
FROM urbanhub.air_mesure FINAL
WHERE is_valid = 1
GROUP BY ville_id, pollutant;

-- Q4 : Heures d'épisodes de pollution anormale (dépassement seuil ATMO ou ratio OMS >= 1.5)
CREATE OR REPLACE VIEW urbanhub.v_air_episodes_anormaux AS
SELECT
    ville_id,
    pollutant,
    anyLast(pollutant_label) AS pollutant_label,
    toStartOfHour(heure_utc) AS heure_utc,
    round(avg(value), 2) AS concentration_moyenne_ugm3,
    round(max(value), 2) AS concentration_pic_ugm3,
    round(max(ratio_oms), 2) AS ratio_oms_pic,
    if(max(ratio_oms) >= 2.0, 'ALERTE', 'MODERE') AS severite
FROM urbanhub.air_mesure FINAL
WHERE is_valid = 1
GROUP BY ville_id, pollutant, heure_utc
HAVING max(depasse_episode) = 1 OR max(ratio_oms) >= 1.5;

-- Agrégat ville × heure_utc pour la jointure Gold P4
CREATE OR REPLACE VIEW urbanhub.v_air_ville_heure AS
SELECT
    ville_id,
    toStartOfHour(heure_utc) AS heure_utc,
    round(avgIf(value, pollutant = 'pm25'), 2) AS pm25,
    round(avgIf(value, pollutant = 'pm10'), 2) AS pm10,
    round(avgIf(value, pollutant = 'no2'), 2) AS no2,
    round(avgIf(value, pollutant = 'o3'), 2) AS o3,
    round(avgIf(value, pollutant = 'co'), 2) AS co,
    round(max(ratio_oms), 3) AS indice_pollution_oms,
    argMax(pollutant, ratio_oms) AS polluant_dominant
FROM urbanhub.air_mesure FINAL
WHERE is_valid = 1
GROUP BY ville_id, heure_utc;
