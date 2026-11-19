-- 10_p1_meteo : tables meteo_station, meteo_obs
-- Responsable : P1.

CREATE TABLE IF NOT EXISTS urbanhub.meteo_station
(
    station_id String,
    nom        String,
    lat        Float64,
    lon        Float64
)
ENGINE = MergeTree
ORDER BY station_id;

CREATE TABLE IF NOT EXISTS urbanhub.meteo_obs
(
    station_id      String,
    heure_utc       DateTime64(3),
    temperature     Nullable(Float64),
    pressure        Nullable(Float64),
    visibility      Nullable(Float64),
    precipitation   Nullable(Float64),
    wind_speed      Nullable(Float64),
    wind_direction  Nullable(Float64),
    latitude        Nullable(Float64),
    longitude       Nullable(Float64),
    quality_flags   String DEFAULT '',
    is_valid        UInt8 DEFAULT 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(heure_utc)
ORDER BY (station_id, heure_utc);
