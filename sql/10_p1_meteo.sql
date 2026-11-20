-- 10_p1_meteo : tables Silver Lakehouse meteo_station, meteo_obs et vues métier P1
-- Responsable : P1.

CREATE DATABASE IF NOT EXISTS urbanhub;

CREATE TABLE IF NOT EXISTS urbanhub.meteo_station (
    station_id      String,
    nom             String DEFAULT '',
    ville_id        LowCardinality(String) DEFAULT '',
    lat             Nullable(Float64),
    lon             Nullable(Float64),
    nb_heures       UInt32 DEFAULT 0,
    premiere_heure  Nullable(DateTime64(3, 'UTC')),
    derniere_heure  Nullable(DateTime64(3, 'UTC'))
)
ENGINE = ReplacingMergeTree
ORDER BY (station_id);

CREATE TABLE IF NOT EXISTS urbanhub.meteo_obs (
    station_id         String,
    ville_id           LowCardinality(String),
    timestamp          DateTime64(3, 'UTC'),
    heure_utc          DateTime64(3, 'UTC'),
    temperature        Nullable(Float64),
    dew_point          Nullable(Float64),
    relative_humidity  Nullable(Float64),
    pressure           Nullable(Float64),
    wind_speed         Nullable(Float64),
    wind_direction     Nullable(Float64),
    precipitation      Nullable(Float64),
    visibility         Nullable(Float64),
    latitude           Nullable(Float64),
    longitude          Nullable(Float64),
    quality_flags      String DEFAULT '',
    is_valid           UInt8 DEFAULT 1
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(heure_utc)
ORDER BY (ville_id, station_id, heure_utc);

-- Agrégat horaire ville × heure_utc (clé de jointure Gold P4)
CREATE OR REPLACE VIEW urbanhub.v_meteo_ville_heure AS
SELECT
    ville_id,
    toStartOfHour(heure_utc) AS heure_utc,
    round(avg(temperature), 2) AS temperature,
    round(avg(dew_point), 2) AS dew_point,
    round(avg(relative_humidity), 2) AS relative_humidity,
    round(avg(pressure), 2) AS pressure,
    round(avg(wind_speed), 2) AS wind_speed,
    round(avg(precipitation), 2) AS precipitation,
    round(avg(visibility), 2) AS visibility,
    uniqExact(station_id) AS nb_stations
FROM urbanhub.meteo_obs FINAL
WHERE ville_id != ''
GROUP BY ville_id, heure_utc;

-- Q3 : Évolution saisonnière mensuelle de la température par ville
CREATE OR REPLACE VIEW urbanhub.v_meteo_saisonnalite AS
SELECT
    ville_id,
    toYear(toTimezone(heure_utc, 'Europe/Paris')) AS annee,
    toMonth(toTimezone(heure_utc, 'Europe/Paris')) AS mois,
    round(avg(temperature), 2) AS tmoy_c,
    round(min(temperature), 2) AS tmin_c,
    round(max(temperature), 2) AS tmax_c,
    count() AS nb_heures
FROM urbanhub.meteo_obs FINAL
WHERE temperature IS NOT NULL AND ville_id != ''
GROUP BY ville_id, annee, mois;

-- Q4 : Journées aux conditions météorologiques extrêmes (seuils vigilance Météo-France)
CREATE OR REPLACE VIEW urbanhub.v_meteo_jours_extremes AS
SELECT
    ville_id,
    toDate(toTimezone(heure_utc, 'Europe/Paris')) AS jour,
    round(min(temperature), 2) AS tmin,
    round(max(temperature), 2) AS tmax,
    round(avg(temperature), 2) AS tmoy,
    round(max(wind_speed), 2) AS vent_max,
    round(avg(precipitation) * 24.0, 1) AS precip_jour,
    round(min(visibility), 0) AS visibilite_min,
    round(min(pressure), 1) AS pression_min
FROM urbanhub.meteo_obs FINAL
WHERE ville_id != ''
GROUP BY ville_id, jour
HAVING
    tmax >= 35.0
    OR tmin >= 20.0
    OR tmin <= -5.0
    OR vent_max >= 15.0
    OR precip_jour >= 30.0
    OR visibilite_min < 200.0
    OR pression_min < 980.0;
