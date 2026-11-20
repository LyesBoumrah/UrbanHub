"""Tests unitaires P4 — jointure Gold (ville_id, heure_utc), indicateurs croisés et analyses Q1–Q4.

Responsable : commun / P4.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common.storage import LocalBronzeStorage
from p4_croise.analytics import (
    favorable_cycling_conditions,
    pollution_by_weather_regime,
    strong_tripartite_interactions,
    weather_bike_usage_impact,
    weather_pollution_correlations,
)
from p4_croise.build_gold import (
    GOLD_COLS,
    GOLD_JSON_KEY,
    GOLD_SYNTHESE_KEY,
    aggregate_velos_ville_heure,
    build_gold,
    enrich_gold_indicators,
    join_gold_dataframes,
    load_gold_df,
)


def _sample_tripartite_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    hours = pd.date_range("2026-05-01", periods=72, freq="1h", tz="UTC")
    n = len(hours)
    h_of_day = np.array([ts.hour for ts in hours])

    # Météo : doux et sec les 48 premières heures, puis épisode pluvieux/venteux ou stagnant
    temp = 16.0 + 5.0 * np.sin(2 * np.pi * (h_of_day - 6) / 24.0)
    wind = np.where(np.arange(n) < 24, 1.5, np.where(np.arange(n) < 48, 3.8, 9.0))
    precip = np.where(np.arange(n) >= 56, 2.5, 0.0)
    vis = np.where(precip > 0, 3500.0, 15000.0)

    meteo_df = pd.DataFrame({
        "ville_id": "paris",
        "heure_utc": hours,
        "temperature": temp,
        "dew_point": temp - 4.0,
        "relative_humidity": 65.0,
        "pressure": np.where(wind < 2.0, 1024.0, 1012.0),
        "wind_speed": wind,
        "precipitation": precip,
        "visibility": vis,
    })

    # Vélos : utilisation plus forte par temps sec/doux qu'en cas de pluie forte
    is_peak = np.isin(h_of_day, [8, 9, 17, 18])
    util = np.where(precip > 0, 0.18, np.where(is_peak, 0.58, 0.42))
    velos_df = pd.DataFrame({
        "ville_id": "paris",
        "station_id": "st-1",
        "timestamp": hours,
        "bikes_available": np.round((1.0 - util) * 30).astype(int),
        "free_slots": np.round(util * 30).astype(int),
        "capacity": 30,
        "taux_utilisation": util,
        "taux_occupation": 1.0 - util,
        "est_vide": (util > 0.9).astype(int),
        "est_pleine": (util < 0.1).astype(int),
        "est_critique": (util > 0.55).astype(int),
    })

    # Pollution : plus élevée quand le vent est calme (< 2 m/s), plus basse sous vent fort / pluie
    pm25 = np.where(wind < 2.0, 28.0, np.where(precip > 0, 7.0, 12.0))
    no2 = np.where(wind < 2.0, 48.0, 20.0)
    o3 = np.where(temp > 19.0, 95.0, 55.0)
    pollution_df = pd.DataFrame({
        "ville_id": "paris",
        "heure_utc": hours,
        "pm25": pm25,
        "pm10": pm25 * 1.6,
        "no2": no2,
        "o3": o3,
        "co": 350.0,
        "indice_pollution_oms": np.maximum(pm25 / 15.0, no2 / 25.0),
        "polluant_dominant": "no2",
    })
    return meteo_df, velos_df, pollution_df


def test_jointure_gold_et_indicateurs():
    meteo_df, velos_df, pollution_df = _sample_tripartite_data()
    velos_agg = aggregate_velos_ville_heure(velos_df)
    assert len(velos_agg) == 72

    gold = join_gold_dataframes(meteo_df=meteo_df, velos_df=velos_df, pollution_df=pollution_df)
    assert len(gold) == 72
    assert list(gold.columns) == GOLD_COLS
    assert gold["indice_confort_meteo"].between(0, 100).all()
    assert gold["score_mobilite_douce"].between(0, 100).all()
    assert set(gold["condition_favorable_velo"].unique()).issubset({0, 1})
    assert "exposition_cyclistes_pointe" in set(gold["regime_interaction"])


def test_analyses_croisees_q1_a_q4():
    meteo_df, velos_df, pollution_df = _sample_tripartite_data()
    gold = join_gold_dataframes(meteo_df=meteo_df, velos_df=velos_df, pollution_df=pollution_df)
    gold = enrich_gold_indicators(gold)

    # Q1 : Météo x Pollution
    corrs = weather_pollution_correlations(gold)
    assert not corrs.empty
    wind_pm25 = corrs[(corrs["meteo_var"] == "wind_speed") & (corrs["polluant"] == "pm25")]["correlation"].iloc[0]
    assert wind_pm25 < 0  # le vent disperse les PM2.5

    regimes_p = pollution_by_weather_regime(gold)
    assert not regimes_p["par_vent_pluie"].empty
    assert not regimes_p["par_temperature"].empty

    # Q2 : Météo x Vélos
    impact_v = weather_bike_usage_impact(gold)
    assert not impact_v["par_temperature_pluie"].empty
    assert not impact_v["par_ville_pluie"].empty

    # Q3 : Conditions favorables à la mobilité douce
    fav = favorable_cycling_conditions(gold)
    assert not fav["par_ville"].empty
    assert fav["par_ville"].iloc[0]["gain_utilisation_pts"] > 0  # plus de vélos utilisés quand la météo est favorable

    # Q4 : Interactions tripartites fortes
    inter = strong_tripartite_interactions(gold, min_score=50.0)
    assert not inter["heures_critiques"].empty
    assert not inter["episodes"].empty
    assert not inter["repartition_regimes"].empty


def test_couche_gold_uniquement_json_metier(tmp_path):
    """Vérifie que silver/ ne contient que du Parquet et gold/ uniquement des JSON métier P1 à P4."""
    st = LocalBronzeStorage(base_dir=tmp_path)
    meteo_df, velos_df, _ = _sample_tripartite_data()

    # Préparation de la couche silver (uniquement des Parquet issus du nettoyage bronze)
    meteo_obs = meteo_df.assign(
        station_id="07149099999",
        timestamp=meteo_df["heure_utc"],
        wind_direction=210.0,
        latitude=48.8566,
        longitude=2.3522,
        quality_flags="",
        is_valid=1,
    )
    st.put_parquet("silver/meteo/meteo_obs.parquet", meteo_obs)

    velos_silver = velos_df.assign(
        network_id="velib",
        station_name="Châtelet",
        latitude=48.858,
        longitude=2.347,
    )
    st.put_parquet("silver/velos/velo_releve_20260501.parquet", velos_silver)

    hours = meteo_df["heure_utc"]
    air_rows = []
    for ts in hours:
        air_rows.append({
            "sensor_id": 7775825,
            "ville_id": "paris",
            "location_id": 2162591,
            "location_name": "PARIS 1er Les Halles",
            "pollutant": "pm25",
            "pollutant_label": "PM2.5",
            "timestamp": ts,
            "heure_utc": ts,
            "value": 24.0,
            "unit": "µg/m³",
            "latitude": 48.862,
            "longitude": 2.344,
            "ratio_oms": 1.6,
            "depasse_oms": 1,
            "depasse_episode": 0,
            "quality_flags": "",
            "is_valid": 1,
        })
    st.put_parquet("silver/pollution/air_mesure.parquet", pd.DataFrame(air_rows))

    # Génération de la couche Gold depuis la couche Silver
    res = build_gold(use_local=True, storage=st)
    assert res["gold_rows"] == 72

    silver_files = st.list("silver/")
    gold_files = st.list("gold/")

    # Règle stricte : silver/ = Parquet uniquement ; gold/ = JSON uniquement
    assert len(silver_files) > 0 and all(k.endswith(".parquet") for k in silver_files)
    assert len(gold_files) >= 19 and all(k.endswith(".json") for k in gold_files)
    assert not any(k.endswith(".parquet") for k in gold_files)

    expected_keys = {
        "gold/p1_meteo/q1_periodes_anormales.json",
        "gold/p1_meteo/q2_correlation_visibilite.json",
        "gold/p1_meteo/q3_evolution_saisonniere.json",
        "gold/p1_meteo/q4_jours_extremes.json",
        "gold/p2_velos/q1_utilisation_stations.json",
        "gold/p2_velos/q2_zones_sous_offre.json",
        "gold/p2_velos/q3_pics_journaliers.json",
        "gold/p2_velos/q4_desequilibres_geographiques.json",
        "gold/p2_velos/q5_stations_critiques_reequilibrage.json",
        "gold/p3_pollution/q1_zones_plus_polluees.json",
        "gold/p3_pollution/q2_variation_journaliere.json",
        "gold/p3_pollution/q3_polluants_dominants.json",
        "gold/p3_pollution/q4_episodes_pollution_anormale.json",
        "gold/p4_croise/q1_meteo_pollution.json",
        "gold/p4_croise/q2_meteo_velos.json",
        "gold/p4_croise/q3_conditions_favorables_mobilite_douce.json",
        "gold/p4_croise/q4_interactions_fortes.json",
        GOLD_JSON_KEY,
        GOLD_SYNTHESE_KEY,
    }
    assert expected_keys.issubset(set(gold_files))

    # Vérification de la structure JSON métier enrichie (P1 à P4 + synthèse décideurs)
    p1_q1 = st.get_json("gold/p1_meteo/q1_periodes_anormales.json")
    assert "jours_anomalies_marquees" in p1_q1["resultats"]
    p1_q3 = st.get_json("gold/p1_meteo/q3_evolution_saisonniere.json")
    assert "mensuel_ville" in p1_q3["resultats"] and "mensuel_france" in p1_q3["resultats"]

    p2_q1 = st.get_json("gold/p2_velos/q1_utilisation_stations.json")
    assert "paris" in p2_q1["resultats"]["top_stations_par_ville"]
    p2_q3 = st.get_json("gold/p2_velos/q3_pics_journaliers.json")
    assert len(p2_q3["resultats"]["pics_detectes"]) >= 1
    p2_q5 = st.get_json("gold/p2_velos/q5_stations_critiques_reequilibrage.json")
    assert "plan_reequilibrage_par_ville" in p2_q5["resultats"]

    p3_q1 = st.get_json("gold/p3_pollution/q1_zones_plus_polluees.json")
    assert set(p3_q1["resultats"]["top_par_polluant"].keys()) == {"pm25", "pm10", "no2", "o3", "co"}
    p3_q2 = st.get_json("gold/p3_pollution/q2_variation_journaliere.json")
    assert len(p3_q2["resultats"]["heures_de_pic_par_polluant"]) >= 1
    p3_q4 = st.get_json("gold/p3_pollution/q4_episodes_pollution_anormale.json")
    assert "pics_horaires_depassement_oms" in p3_q4["resultats"]

    q1_p4 = st.get_json("gold/p4_croise/q1_meteo_pollution.json")
    assert q1_p4["partie"] == "P4" and q1_p4["question_id"] == "Q1"
    assert q1_p4["resultats"]["indicateurs_cles"]["nb_heures_croisees"] == 72
    q3_p4 = st.get_json("gold/p4_croise/q3_conditions_favorables_mobilite_douce.json")
    assert "criteres_favorables" in q3_p4["resultats"]
    assert len(q3_p4["resultats"]["par_heure_locale"]) == 24
    q4_p4 = st.get_json("gold/p4_croise/q4_interactions_fortes.json")
    assert len(q4_p4["resultats"]["recommandations_decideurs"]) >= 1

    synthese = st.get_json(GOLD_SYNTHESE_KEY)
    assert len(synthese["questions_metier_couvertes"]) == 17
    assert synthese["couverture_flux"]["croisement_complet_3_flux_heures"] == 72
    assert len(synthese["indicateurs_par_ville"]) == 1
    assert synthese["kpis_globaux_france"]["nb_villes_actives"] == 1

    reloaded = load_gold_df(use_local=True, storage=st)
    assert len(reloaded) == 72
