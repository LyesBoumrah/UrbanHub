-- 20_p2_velos : tables Silver Lakehouse velo_station, velo_releve et vues métier P2 (Q1 à Q5)
-- Responsable : P2.

CREATE DATABASE IF NOT EXISTS urbanhub;

CREATE TABLE IF NOT EXISTS urbanhub.velo_station (
    ville_id        LowCardinality(String),
    network_id      LowCardinality(String),
    station_id      String,
    station_name    String,
    latitude        Float64,
    longitude       Float64,
    capacity        UInt16,
    is_virtual      UInt8 DEFAULT 0,
    updated_at_utc  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at_utc)
ORDER BY (ville_id, station_id);

CREATE TABLE IF NOT EXISTS urbanhub.velo_releve (
    ville_id          LowCardinality(String),
    network_id        LowCardinality(String),
    station_id        String,
    station_name      String,
    timestamp         DateTime64(3, 'UTC'),
    heure_utc         DateTime('UTC'),
    latitude          Float64,
    longitude         Float64,
    bikes_available   UInt16,
    free_slots        UInt16,
    capacity          UInt16,
    taux_occupation   Float32,
    taux_utilisation  Float32,
    est_vide          UInt8,
    est_pleine        UInt8,
    est_critique      UInt8,
    ingested_at_utc   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at_utc)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (ville_id, station_id, timestamp);

-- Q1 : Stations au plus fort taux d'utilisation
CREATE OR REPLACE VIEW urbanhub.v_velo_utilisation_stations AS
SELECT
    ville_id,
    network_id,
    station_id,
    anyLast(station_name) AS station_name,
    anyLast(latitude) AS latitude,
    anyLast(longitude) AS longitude,
    round(avg(capacity), 1) AS capacite_moyenne,
    round(avg(taux_utilisation) * 100.0, 1) AS taux_utilisation_pct,
    round(avg(taux_occupation) * 100.0, 1) AS taux_occupation_pct,
    round(stddevPop(bikes_available), 2) AS volatilite_velos,
    max(bikes_available) - min(bikes_available) AS amplitude_velos,
    count() AS nb_releves
FROM urbanhub.velo_releve FINAL
GROUP BY ville_id, network_id, station_id;

-- Q2 : Zones où l'offre de vélos est insuffisante
CREATE OR REPLACE VIEW urbanhub.v_velo_zones_sous_offre AS
SELECT
    ville_id,
    round(latitude, 2) AS zone_lat,
    round(longitude, 2) AS zone_lon,
    uniqExact(station_id) AS nb_stations,
    round(avg(bikes_available), 2) AS velos_dispo_moyens,
    round(avg(taux_occupation) * 100.0, 1) AS taux_occupation_pct,
    round(countIf(est_vide = 1) / count() * 100.0, 1) AS pct_temps_vide,
    round(countIf(taux_occupation <= 0.15) / count() * 100.0, 1) AS pct_temps_sous_offre,
    count() AS nb_releves
FROM urbanhub.velo_releve FINAL
GROUP BY ville_id, zone_lat, zone_lon;

-- Q3 : Pics d'utilisation journaliers (0h–23h heure locale, Semaine vs Week-end)
CREATE OR REPLACE VIEW urbanhub.v_velo_pics_journaliers AS
SELECT
    ville_id,
    toHour(toTimezone(timestamp, 'Europe/Paris')) AS heure_locale,
    if(toDayOfWeek(toTimezone(timestamp, 'Europe/Paris')) IN (6, 7), 'Week-end', 'Semaine') AS type_jour,
    round(avg(taux_utilisation) * 100.0, 1) AS taux_utilisation_pct,
    round(avg(taux_occupation) * 100.0, 1) AS taux_occupation_pct,
    round(avg(bikes_available), 2) AS velos_dispo_moyens,
    round(countIf(est_vide = 1) / count() * 100.0, 1) AS pct_stations_vides,
    round(countIf(est_pleine = 1) / count() * 100.0, 1) AS pct_stations_pleines,
    count() AS nb_releves
FROM urbanhub.velo_releve FINAL
GROUP BY ville_id, heure_locale, type_jour;

-- Q4 : Déséquilibres géographiques par secteur (Centre vs quadrants N/S/E/O)
CREATE OR REPLACE VIEW urbanhub.v_velo_desequilibre_geo AS
SELECT
    r.ville_id AS ville_id,
    multiIf(
        geoDistance(r.longitude, r.latitude, v.longitude, v.latitude) <= 1500, 'Centre (<1.5 km)',
        r.latitude >= v.latitude AND r.longitude >= v.longitude, 'Nord-Est',
        r.latitude >= v.latitude AND r.longitude < v.longitude, 'Nord-Ouest',
        r.latitude < v.latitude AND r.longitude >= v.longitude, 'Sud-Est',
        'Sud-Ouest'
    ) AS secteur_geo,
    uniqExact(r.station_id) AS nb_stations,
    round(avg(r.taux_occupation) * 100.0, 1) AS taux_occupation_pct,
    round(avg(r.taux_utilisation) * 100.0, 1) AS taux_utilisation_pct,
    round(countIf(r.est_vide = 1) / count() * 100.0, 1) AS pct_temps_vide,
    round(countIf(r.est_pleine = 1) / count() * 100.0, 1) AS pct_temps_pleine,
    round(avg(geoDistance(r.longitude, r.latitude, v.longitude, v.latitude)) / 1000.0, 2) AS distance_centre_km
FROM urbanhub.velo_releve AS r FINAL
LEFT JOIN urbanhub.ref_ville AS v FINAL ON r.ville_id = v.ville_id
GROUP BY r.ville_id, secteur_geo;

-- Q5 : Stations critiques nécessitant un rééquilibrage
CREATE OR REPLACE VIEW urbanhub.v_velo_stations_critiques AS
SELECT
    ville_id,
    network_id,
    station_id,
    station_name,
    latitude,
    longitude,
    dernier_timestamp,
    bikes_available,
    free_slots,
    capacity,
    round(dernier_taux_occupation * 100.0, 1) AS taux_occupation_pct,
    round(pct_temps_critique * 100.0, 1) AS pct_temps_critique,
    multiIf(
        dernier_taux_occupation <= 0.10, 'REAPPROVISIONNER',
        dernier_taux_occupation >= 0.90, 'DESENGORGER',
        'AUCUNE'
    ) AS action_reequilibrage,
    toInt32(round(capacity * 0.5) - bikes_available) AS delta_velos_equilibre
FROM (
    SELECT
        ville_id,
        network_id,
        station_id,
        argMax(station_name, timestamp) AS station_name,
        argMax(latitude, timestamp) AS latitude,
        argMax(longitude, timestamp) AS longitude,
        max(timestamp) AS dernier_timestamp,
        argMax(bikes_available, timestamp) AS bikes_available,
        argMax(free_slots, timestamp) AS free_slots,
        argMax(capacity, timestamp) AS capacity,
        argMax(taux_occupation, timestamp) AS dernier_taux_occupation,
        countIf(est_critique = 1) / count() AS pct_temps_critique
    FROM urbanhub.velo_releve FINAL
    GROUP BY ville_id, network_id, station_id
)
WHERE action_reequilibrage != 'AUCUNE';

-- Agrégat ville × heure_utc pour la jointure Gold P4
CREATE OR REPLACE VIEW urbanhub.v_velo_ville_heure AS
SELECT
    ville_id,
    heure_utc,
    uniqExact(station_id) AS nb_stations_actives,
    round(avg(bikes_available), 2) AS velos_dispo_moyens,
    round(avg(free_slots), 2) AS bornes_libres_moyennes,
    round(avg(taux_utilisation), 4) AS taux_utilisation_moyen,
    round(avg(taux_occupation), 4) AS taux_occupation_moyen,
    round(countIf(est_vide = 1) / count(), 4) AS pct_stations_vides,
    round(countIf(est_pleine = 1) / count(), 4) AS pct_stations_pleines,
    round(countIf(est_critique = 1) / count(), 4) AS pct_stations_critiques,
    count() AS nb_releves
FROM urbanhub.velo_releve FINAL
GROUP BY ville_id, heure_utc;
