-- 40_gold : table Lakehouse Gold gold_ville_heure et vues métier croisées P4 (Q1 à Q4 + Synthèse)
-- Exécuté en dernier. Responsable : commun / P4.

CREATE DATABASE IF NOT EXISTS urbanhub;

CREATE TABLE IF NOT EXISTS urbanhub.gold_ville_heure (
    ville_id                  LowCardinality(String),
    heure_utc                 DateTime64(3, 'UTC'),
    temperature               Nullable(Float64),
    dew_point                 Nullable(Float64),
    relative_humidity         Nullable(Float64),
    pressure                  Nullable(Float64),
    wind_speed                Nullable(Float64),
    precipitation             Nullable(Float64),
    visibility                Nullable(Float64),
    nb_stations_velo          Nullable(Float64),
    velos_dispo_moyens        Nullable(Float64),
    taux_utilisation_velo     Nullable(Float64),
    taux_occupation_velo      Nullable(Float64),
    pct_stations_vides        Nullable(Float64),
    pct_stations_pleines      Nullable(Float64),
    pct_stations_critiques    Nullable(Float64),
    pm25                      Nullable(Float64),
    pm10                      Nullable(Float64),
    no2                       Nullable(Float64),
    o3                        Nullable(Float64),
    co                        Nullable(Float64),
    indice_pollution_oms      Nullable(Float64),
    polluant_dominant         LowCardinality(String) DEFAULT '',
    indice_confort_meteo      Float32,
    score_mobilite_douce      Float32,
    condition_favorable_velo  UInt8,
    score_interaction_urbaine Float32,
    regime_interaction        LowCardinality(String) DEFAULT 'normal'
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(heure_utc)
ORDER BY (ville_id, heure_utc);

-- Q1 (P4) : Relation Météo × Pollution par régime de vent et de pluie
CREATE OR REPLACE VIEW urbanhub.v_gold_meteo_pollution AS
SELECT
    multiIf(
        wind_speed < 2.0, '1. Calme (<2 m/s)',
        wind_speed < 5.0, '2. Faible (2-5 m/s)',
        wind_speed < 8.0, '3. Modéré (5-8 m/s)',
        '4. Fort (>=8 m/s)'
    ) AS classe_vent,
    if(coalesce(precipitation, 0) > 0.2, 'Pluie (>0.2 mm/h)', 'Temps sec') AS classe_pluie,
    round(avg(pm25), 2) AS pm25_moyen,
    round(avg(pm10), 2) AS pm10_moyen,
    round(avg(no2), 2) AS no2_moyen,
    round(avg(o3), 2) AS o3_moyen,
    round(avg(indice_pollution_oms), 2) AS indice_oms_moyen,
    count() AS nb_heures
FROM urbanhub.gold_ville_heure FINAL
WHERE wind_speed IS NOT NULL AND indice_pollution_oms IS NOT NULL
GROUP BY classe_vent, classe_pluie;

-- Q2 (P4) : Influence des conditions météo sur l'utilisation des vélos urbains
CREATE OR REPLACE VIEW urbanhub.v_gold_meteo_velos AS
SELECT
    ville_id,
    multiIf(
        coalesce(precipitation, 0) <= 0.1, '1. Sec',
        precipitation <= 1.0, '2. Pluie faible (0.1-1 mm/h)',
        '3. Pluie forte (>1 mm/h)'
    ) AS classe_pluie,
    round(avg(taux_utilisation_velo) * 100.0, 1) AS taux_utilisation_pct,
    round(avg(taux_occupation_velo) * 100.0, 1) AS taux_occupation_pct,
    round(avg(pct_stations_critiques) * 100.0, 1) AS pct_stations_critiques,
    count() AS nb_heures
FROM urbanhub.gold_ville_heure FINAL
WHERE temperature IS NOT NULL AND taux_utilisation_velo IS NOT NULL
GROUP BY ville_id, classe_pluie;

-- Q3 (P4) : Conditions favorables à la mobilité douce par ville
CREATE OR REPLACE VIEW urbanhub.v_gold_mobilite_douce AS
SELECT
    ville_id,
    round(avg(score_mobilite_douce), 1) AS score_mobilite_moyen,
    round(avg(indice_confort_meteo), 1) AS indice_confort_meteo_moyen,
    round(avg(condition_favorable_velo) * 100.0, 1) AS pct_heures_favorables,
    round(avgIf(taux_utilisation_velo, condition_favorable_velo = 1) * 100.0, 1) AS util_velo_favorable_pct,
    round(avgIf(taux_utilisation_velo, condition_favorable_velo = 0) * 100.0, 1) AS util_velo_defavorable_pct,
    count() AS nb_heures
FROM urbanhub.gold_ville_heure FINAL
GROUP BY ville_id;

-- Q4 (P4) : Régimes et périodes d'interactions tripartites fortes
CREATE OR REPLACE VIEW urbanhub.v_gold_interactions_fortes AS
SELECT
    regime_interaction,
    count() AS nb_heures,
    round(avg(temperature), 1) AS temperature_moy,
    round(avg(wind_speed), 1) AS vent_moy,
    round(avg(taux_utilisation_velo) * 100.0, 1) AS taux_utilisation_velo_pct,
    round(avg(indice_pollution_oms), 2) AS indice_oms_moy,
    round(avg(score_interaction_urbaine), 1) AS score_interaction_moy
FROM urbanhub.gold_ville_heure FINAL
GROUP BY regime_interaction
ORDER BY score_interaction_moy DESC;

-- Synthèse exécutive Smart City par métropole (table de bord décideurs publics)
CREATE OR REPLACE VIEW urbanhub.v_gold_synthese_villes AS
SELECT
    g.ville_id AS ville_id,
    anyLast(v.nom) AS ville_nom,
    count() AS nb_heures_gold,
    min(g.heure_utc) AS periode_debut_utc,
    max(g.heure_utc) AS periode_fin_utc,
    round(avg(g.temperature), 1) AS temperature_moy_c,
    round(avg(g.taux_utilisation_velo) * 100.0, 1) AS taux_utilisation_velo_pct,
    round(avg(g.pct_stations_critiques) * 100.0, 1) AS pct_stations_critiques,
    round(avg(g.indice_pollution_oms), 2) AS indice_pollution_oms_moy,
    topK(1)(g.polluant_dominant)[1] AS polluant_dominant,
    round(avg(g.score_mobilite_douce), 1) AS score_mobilite_douce_moy,
    round(avg(g.condition_favorable_velo) * 100.0, 1) AS pct_creneaux_favorables,
    round(max(g.score_interaction_urbaine), 1) AS score_interaction_max
FROM urbanhub.gold_ville_heure AS g FINAL
LEFT JOIN urbanhub.ref_ville AS v FINAL ON g.ville_id = v.ville_id
GROUP BY g.ville_id
ORDER BY g.ville_id;
