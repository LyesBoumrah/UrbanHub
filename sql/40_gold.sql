-- 40_gold : couche GOLD dans ClickHouse (base urbanhub) — tables agrégées prêtes pour le dashboard et le ML.
-- Responsable : commun.
--
-- Toutes les tables sont RECALCULÉES depuis le silver (Parquet dans Silo) par p4_croise/build_gold.py.
-- Chargement idempotent : remplissage d'une table <nom>__new puis EXCHANGE TABLES (bascule atomique).
--
-- MergeTree : moteur standard de ClickHouse, stockage en COLONNES compressées.
-- ORDER BY  : clé de tri physique → les filtres sur ses premières colonnes (ville, date) sont très rapides.
-- PARTITION BY : découpage physique (par année) pour les tables temporelles volumineuses.

CREATE DATABASE IF NOT EXISTS urbanhub;

-- 1. Croisement horaire ville × heure (P4, séries temporelles, ML) — ~ 526 000 lignes
CREATE TABLE IF NOT EXISTS urbanhub.gold_ville_heure
(
    ville_id               LowCardinality(String),
    heure_utc              DateTime('UTC'),
    temperature_c          Nullable(Float64),
    humidite_pct           Nullable(Float64),
    precipitation_mm       Nullable(Float64),
    pression_hpa           Nullable(Float64),
    nebulosite_pct         Nullable(Float64),
    vent_ms                Nullable(Float64),
    vent_direction_deg     Nullable(Float64),
    pm25                   Nullable(Float64),
    pm10                   Nullable(Float64),
    no2                    Nullable(Float64),
    o3                     Nullable(Float64),
    co                     Nullable(Float64),
    nb_stations_pm25       UInt8,
    nb_stations_pm10       UInt8,
    nb_stations_no2        UInt8,
    nb_stations_o3         UInt8,
    nb_stations_co         UInt8,
    velo_stations          Nullable(Float64),
    velos_dispo_total      Nullable(Float64),
    activite_totale        Nullable(Float64),
    activite_par_station   Nullable(Float64),
    remplissage_moyen      Nullable(Float64),
    part_stations_vides    Nullable(Float64),
    part_stations_pleines  Nullable(Float64),
    part_heures_observees  Nullable(Float64),
    a_meteo                Bool,
    a_pollution            Bool,
    a_velos                Bool,
    heure_locale           UInt8,
    jour_semaine           UInt8,
    est_weekend            Bool,
    date_locale            Date,
    mois                   UInt8,
    saison                 LowCardinality(String),
    score_favorabilite     UInt8
)
ENGINE = MergeTree
PARTITION BY toYear(heure_utc)
ORDER BY (ville_id, heure_utc);

-- 2. Synthèse journalière ville × jour local (tableaux de bord, alertes OMS)
CREATE TABLE IF NOT EXISTS urbanhub.gold_ville_jour
(
    ville_id                   LowCardinality(String),
    date_locale                Date,
    saison                     LowCardinality(String),
    est_weekend                Bool,
    t_moy                      Nullable(Float64),
    t_min                      Nullable(Float64),
    t_max                      Nullable(Float64),
    pluie_mm                   Nullable(Float64),
    vent_moy_ms                Nullable(Float64),
    vent_max_ms                Nullable(Float64),
    humidite_pct               Nullable(Float64),
    heures_favorables          UInt8,               -- heures 7 h-21 h avec score de favorabilité = 3
    pm25                       Nullable(Float64),   -- moyenne journalière de la ville
    pm10                       Nullable(Float64),
    no2                        Nullable(Float64),
    o3                         Nullable(Float64),
    co                         Nullable(Float64),
    o3_max_8h                  Nullable(Float64),   -- max des moyennes glissantes 8 h (stations de la ville)
    depassement_oms_pm25       Nullable(UInt8),     -- moyenne de ville > 15 µg/m³
    depassement_oms_pm10       Nullable(UInt8),     -- > 45
    depassement_oms_no2        Nullable(UInt8),     -- > 25
    depassement_oms_o3         Nullable(UInt8),     -- max 8 h > 100
    velo_activite_totale       Nullable(Float64),
    velo_activite_par_station  Nullable(Float64),
    velo_remplissage_moyen     Nullable(Float64),
    velo_part_stations_vides   Nullable(Float64),
    velo_indice_activite       Nullable(Float64)    -- 1 = activité normale pour la ville / saison / type de jour
)
ENGINE = MergeTree
PARTITION BY toYear(date_locale)
ORDER BY (ville_id, date_locale);

-- 3. Indicateurs par station de vélos (P2 : utilisation, sous-offre, criticité)
CREATE TABLE IF NOT EXISTS urbanhub.gold_station_velo
(
    ville_id             LowCardinality(String),
    station_id           String,
    station_nom          String,
    latitude             Float64,
    longitude            Float64,
    distance_centre_km   Float64,
    capacite_declaree    Nullable(Float64),
    statut_station       LowCardinality(String),   -- exploitable / inactive / technique / aberrante
    jours_observes       Float64,
    activite_jour        Float64,
    pct_vide             Float64,
    pct_pleine           Float64,
    remplissage_moy      Float64,
    pct_vide_pointe      Nullable(Float64),
    pct_pleine_pointe    Nullable(Float64),
    remplissage_matin    Nullable(Float64),
    remplissage_soir     Nullable(Float64),
    score_criticite      Nullable(Float64),
    action               LowCardinality(String),   -- réapprovisionner / désengorger / aucune
    rang_activite_ville  UInt32
)
ENGINE = MergeTree
ORDER BY (ville_id, station_id);

-- 4. Zones de ~500 m (P2 Q2 : sous-offre)
CREATE TABLE IF NOT EXISTS urbanhub.gold_zone_velo
(
    ville_id        LowCardinality(String),
    zone_id         String,
    latitude        Float64,
    longitude       Float64,
    stations        UInt32,
    activite_jour   Float64,
    pct_vide        Float64,
    sous_offre      Bool                       -- stations vides ≥ 15 % du temps
)
ENGINE = MergeTree
ORDER BY (ville_id, zone_id);

-- 5. Profil hebdomadaire de l'activité vélo (P2 Q3)
CREATE TABLE IF NOT EXISTS urbanhub.gold_velo_profil
(
    ville_id             LowCardinality(String),
    jour_semaine         UInt8,
    heure_locale         UInt8,
    activite_station_h   Float64,             -- mouvements par station et par heure
    indice               Float64              -- 1 = moyenne de la ville
)
ENGINE = MergeTree
ORDER BY (ville_id, jour_semaine, heure_locale);

-- 6. Indicateurs par station de mesure de la pollution (P3)
CREATE TABLE IF NOT EXISTS urbanhub.gold_station_pollution
(
    ville_id      LowCardinality(String),
    station_id    UInt32,
    station_nom   String,
    latitude      Float64,
    longitude     Float64,
    type_station  LowCardinality(String),       -- trafic / fond urbain (déduit du nom)
    polluant      LowCardinality(String),
    moyenne       Float64,
    p95           Float64,
    heures        UInt32,
    ratio_oms     Nullable(Float64)             -- moyenne / valeur guide annuelle OMS 2021
)
ENGINE = MergeTree
ORDER BY (ville_id, station_id, polluant);

-- 7. Profil journalier de la pollution (P3 Q2)
CREATE TABLE IF NOT EXISTS urbanhub.gold_pollution_profil
(
    ville_id       LowCardinality(String),
    polluant       LowCardinality(String),
    type_jour      LowCardinality(String),      -- semaine / week-end
    heure_locale   UInt8,
    valeur_moy     Float64
)
ENGINE = MergeTree
ORDER BY (ville_id, polluant, type_jour, heure_locale);

-- 8. Indicateurs clés par ville (page d'accueil du dashboard)
CREATE TABLE IF NOT EXISTS urbanhub.gold_kpi_ville
(
    ville_id                    LowCardinality(String),
    nom                         String,
    latitude                    Float64,
    longitude                   Float64,
    t_moy_c                     Float64,
    pluie_mm_an                 Float64,
    pct_heures_favorables       Float64,
    pm25_moy                    Nullable(Float64),
    pm10_moy                    Nullable(Float64),
    no2_moy                     Nullable(Float64),
    o3_moy                      Nullable(Float64),
    ratio_oms_pm25              Nullable(Float64),
    ratio_oms_no2               Nullable(Float64),
    polluant_dominant           LowCardinality(String),
    pct_jours_depassement_pm25  Nullable(Float64),
    pct_jours_depassement_no2   Nullable(Float64),
    pct_jours_depassement_o3    Nullable(Float64),
    stations_pollution          UInt32,
    stations_velo               UInt32,
    stations_velo_inactives     UInt32,
    velo_activite_station_jour  Nullable(Float64),
    pct_zones_sous_offre        Nullable(Float64),
    pct_stations_critiques      Nullable(Float64),
    effet_pluie_velo_pct        Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY ville_id;

-- 9. Couverture des données par source × ville × mois (page Qualité)
CREATE TABLE IF NOT EXISTS urbanhub.gold_couverture
(
    source          LowCardinality(String),   -- météo / pollution / vélos
    ville_id        LowCardinality(String),
    detail          LowCardinality(String),   -- polluant pour la pollution, '' sinon
    mois            Date,                     -- premier jour du mois
    pct_presentes   Float64,                  -- % des heures attendues présentes
    pct_valides     Float64                   -- % des heures attendues présentes ET valides
)
ENGINE = MergeTree
ORDER BY (source, ville_id, detail, mois);

-- 10. Volumes et état de chaque couche du pipeline (page Qualité)
CREATE TABLE IF NOT EXISTS urbanhub.gold_pipeline
(
    couche          LowCardinality(String),   -- bronze / silver
    source          LowCardinality(String),
    objets          UInt64,                   -- fichiers dans Silo
    taille_mo       Float64,
    lignes          Nullable(UInt64),
    lignes_valides  Nullable(UInt64),
    derniere_maj    String                    -- horodatage UTC du dernier traitement (manifeste)
)
ENGINE = MergeTree
ORDER BY (couche, source);
