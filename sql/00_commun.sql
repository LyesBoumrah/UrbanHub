-- 00_commun : base urbanhub, tables ref_ville et pipeline_alertes
-- Exécuté en premier (ordre alphabétique). Responsable : commun.

CREATE DATABASE IF NOT EXISTS urbanhub;

CREATE TABLE IF NOT EXISTS urbanhub.ref_ville (
    ville_id            LowCardinality(String),
    nom                 String,
    core_urbanhub       UInt8 DEFAULT 0,
    latitude            Float64,
    longitude           Float64,
    bbox_lat_min        Float64,
    bbox_lat_max        Float64,
    bbox_lon_min        Float64,
    bbox_lon_max        Float64,
    citybikes_network   LowCardinality(String) DEFAULT '',
    citybikes_name      String DEFAULT '',
    noaa_station_id     String DEFAULT '',
    updated_at_utc      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at_utc)
ORDER BY (ville_id);

CREATE TABLE IF NOT EXISTS urbanhub.pipeline_alertes (
    ts_utc        DateTime64(3, 'UTC'),
    pipeline      LowCardinality(String),
    ville_id      LowCardinality(String) DEFAULT '',
    niveau        LowCardinality(String),
    message       String,
    details_json  String DEFAULT '{}'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts_utc)
ORDER BY (pipeline, ts_utc, ville_id);
CREATE TABLE IF NOT EXISTS ref_ville
(
    ville_id String,
    nom      String,
    lat      Float64,
    lon      Float64
)
ENGINE = MergeTree
ORDER BY ville_id;

CREATE TABLE IF NOT EXISTS pipeline_alertes
(
    ts      DateTime64(3),
    level   String,
    source  String,
    message String
)
ENGINE = MergeTree
ORDER BY ts;
