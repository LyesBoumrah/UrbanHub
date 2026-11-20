"""Tests unitaires P3 — conversion d'unités, règles de qualité, nettoyage bronze->silver et analyses Q1–Q4.

Responsable : P3.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from p3_pollution.analytics import (
    city_pollution_ranking,
    detect_pollution_episodes,
    diurnal_pollution_profile,
    dominant_pollutants_by_city,
    most_polluted_zones,
)
from p3_pollution.cleaning import (
    MESURE_COLS,
    aggregate_ville_heure,
    build_sensor_table,
    clean_all,
    clean_messages,
)
from p3_pollution.quality import (
    CANONICAL_UNIT,
    check_freshness,
    check_quality_df,
    compute_who_ratio,
    convert_to_ugm3,
    normalize_pollutant,
    pollutant_label,
    quality_summary,
    validate_coordinates_france,
    validate_value_bounds,
)
from p3_pollution.storage import LocalStorage

SAMPLE_LOCATIONS = [
    {
        "ville_id": "paris",
        "location_id": 2162591,
        "name": "PARIS 1er Les Halles",
        "provider": "EEA",
        "latitude": 48.86212,
        "longitude": 2.34462,
        "distance_m": 829,
        "sensors": [
            {"sensor_id": 7773850, "pollutant": "no2", "unit": "µg/m³"},
            {"sensor_id": 7775825, "pollutant": "pm25", "unit": "µg/m³"},
            {"sensor_id": 7773420, "pollutant": "o3", "unit": "µg/m³"},
            {"sensor_id": 7773042, "pollutant": "co", "unit": "µg/m³"},
        ],
    },
    {
        "ville_id": "lyon",
        "location_id": 2696,
        "name": "LYON Centre",
        "provider": "EEA",
        "latitude": 45.7578,
        "longitude": 4.8320,
        "distance_m": 600,
        "sensors": [
            {"sensor_id": 8556, "pollutant": "no2", "unit": "ppb"},
            {"sensor_id": 8559, "pollutant": "pm10", "unit": "µg/m³"},
        ],
    },
]


def test_normalisation_polluant_et_conversion_unites():
    assert normalize_pollutant("PM2.5") == "pm25"
    assert normalize_pollutant("o3") == "o3"
    assert pollutant_label("pm25") == "PM2.5"

    # µg/m³ -> identique
    val, err = convert_to_ugm3(24.5, "PM2.5", "ug/m3")
    assert val == 24.5 and err == ""

    # mg/m³ -> µg/m³ (cas fréquent du CO)
    val_co, err_co = convert_to_ugm3(1.2, "CO", "mg/m³")
    assert val_co == 1200.0 and err_co == ""

    # ppb -> µg/m³ pour NO2 (46.0055 / 24.055 ≈ 1.9125)
    val_no2, err_no2 = convert_to_ugm3(10.0, "no2", "ppb")
    assert err_no2 == "" and 19.1 <= val_no2 <= 19.2

    # ppm -> µg/m³ pour CO (28.0101 / 24.055 * 1000 ≈ 1164.4)
    val_ppm, err_ppm = convert_to_ugm3(1.0, "co", "ppm")
    assert err_ppm == "" and 1160.0 <= val_ppm <= 1170.0

    # ppb sur des particules PM2.5 -> incompatible
    val_bad, err_bad = convert_to_ugm3(10.0, "pm25", "ppb")
    assert val_bad is None and err_bad == "unite_incompatible"


def test_bornes_coordonnees_et_fraicheur():
    assert validate_value_bounds(-0.5, "pm25") == "valeur_negative"
    assert validate_value_bounds(1500.0, "pm25") == "valeur_hors_bornes"
    assert validate_value_bounds(18.0, "pm25") == ""

    assert validate_coordinates_france(48.85, 2.35) == ""
    assert validate_coordinates_france(16.25, -61.5) == "coordonnees_hors_france"

    now = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)
    assert check_freshness(now - timedelta(hours=3), now_utc=now) is True
    assert check_freshness(now - timedelta(hours=40), now_utc=now) is False
    assert compute_who_ratio(30.0, "pm25") == 2.0


def test_check_quality_df_et_resume():
    df = pd.DataFrame({
        "sensor_id": [1, 2, 3],
        "pollutant": ["PM2.5", "NO2", "PM10"],
        "value": [12.0, -3.0, 2500.0],
        "unit": ["µg/m³", "µg/m³", "µg/m³"],
        "latitude": [48.86, 48.86, 48.86],
        "longitude": [2.34, 2.34, 2.34],
        "timestamp": [pd.Timestamp("2026-11-19T12:00:00Z")] * 3,
    })
    checked = check_quality_df(df)
    assert list(checked["is_valid"]) == [True, False, False]
    summary = quality_summary(checked)
    assert summary["total"] == 3 and summary["valides"] == 1 and summary["invalides"] == 2
    assert summary["motifs"]["valeur_negative"] == 1
    assert summary["motifs"]["valeur_hors_bornes"] == 1


def test_nettoyage_messages_dedoublonnage_et_7_variables(tmp_path):
    messages = [
        # Message latest
        {
            "source": "openaq",
            "mode": "latest",
            "ville_id": "paris",
            "location_id": 2162591,
            "sensor_id": 7775825,
            "pollutant": "pm25",
            "unit": "µg/m³",
            "payload": {"datetime": {"utc": "2026-11-19T12:00:00Z"}, "value": 18.0},
        },
        # Doublon backfill sur le même (sensor_id, timestamp) avec format period.datetimeFrom
        {
            "source": "openaq",
            "mode": "backfill",
            "ville_id": "paris",
            "location_id": 2162591,
            "sensor_id": 7775825,
            "pollutant": "pm25",
            "unit": "µg/m³",
            "payload": {"period": {"datetimeFrom": {"utc": "2026-11-19T12:00:00Z"}}, "value": 18.4},
        },
        # Mesure en ppb à convertir en µg/m³
        {
            "source": "openaq",
            "mode": "latest",
            "ville_id": "lyon",
            "location_id": 2696,
            "sensor_id": 8556,
            "pollutant": "no2",
            "unit": "ppb",
            "payload": {"datetime": {"utc": "2026-11-19T12:00:00Z"}, "value": 20.0},
        },
        # Mesure négative à rejeter
        {
            "source": "openaq",
            "mode": "latest",
            "ville_id": "lyon",
            "location_id": 2696,
            "sensor_id": 8559,
            "pollutant": "pm10",
            "unit": "µg/m³",
            "payload": {"datetime": {"utc": "2026-11-19T12:00:00Z"}, "value": -2.5},
        },
    ]

    clean_df, summary = clean_messages(messages, locations=SAMPLE_LOCATIONS)
    assert summary["total"] == 4 and summary["valides"] == 3 and summary["invalides"] == 1
    assert len(clean_df) == 2  # dédoublonné sur (7775825, 12:00) + capteur 8556
    assert list(clean_df.columns) == MESURE_COLS

    # Les 7 variables de l'énoncé sont toutes présentes et renseignées
    for required in ("sensor_id", "longitude", "unit", "pollutant", "timestamp", "latitude", "value"):
        assert required in clean_df.columns
        assert clean_df[required].notna().all()

    assert (clean_df["unit"] == CANONICAL_UNIT).all()

    piv = aggregate_ville_heure(clean_df)
    assert len(piv) == 2
    assert set(piv["ville_id"]) == {"paris", "lyon"}

    # Sans fichier bronze : aucun fichier écrit en couche silver
    empty_st = LocalStorage(tmp_path / "empty")
    empty_res = clean_all(storage=empty_st)
    assert empty_res["mesures_valides"] == 0
    assert empty_st.list_keys("silver/") == []

    # Test du pipeline clean_all sur LocalStorage : silver strictement généré depuis bronze
    st = LocalStorage(tmp_path / "with_bronze")
    st.write_json("bronze/openaq/2162591/date=2026-11-19/120000.json", messages[:2])
    st.write_json("bronze/openaq_backfill/8556/year=2026/full.json.gz", messages[2:], compress=True)
    res = clean_all(storage=st)
    assert res["mesures_valides"] == 2
    assert set(st.list_keys("silver/")) == {
        "silver/pollution/air_capteur.parquet",
        "silver/pollution/air_mesure.parquet",
    }


def test_analyses_pollution_q1_a_q4():
    cap_table = build_sensor_table(SAMPLE_LOCATIONS)
    assert len(cap_table) == 6

    hours = pd.date_range("2026-11-10", periods=48, freq="1h", tz="UTC")
    rows = []
    for h_idx, ts in enumerate(hours):
        # Pic d'épisode de pollution PM2.5 à Paris entre h=20 et h=26
        pm25_val = 42.0 if 20 <= h_idx <= 26 else 11.0 + (h_idx % 5)
        no2_val = 38.0 if (ts.hour in (7, 8, 17, 18)) else 16.0
        rows.append({
            "sensor_id": 7775825,
            "ville_id": "paris",
            "location_id": 2162591,
            "location_name": "PARIS 1er Les Halles",
            "pollutant": "pm25",
            "timestamp": ts,
            "value": pm25_val,
            "unit": "µg/m³",
            "latitude": 48.8621,
            "longitude": 2.3446,
        })
        rows.append({
            "sensor_id": 7773850,
            "ville_id": "paris",
            "location_id": 2162591,
            "location_name": "PARIS 1er Les Halles",
            "pollutant": "no2",
            "timestamp": ts,
            "value": no2_val,
            "unit": "µg/m³",
            "latitude": 48.8621,
            "longitude": 2.3446,
        })
    df = pd.DataFrame(rows)

    q1_zones = most_polluted_zones(df)
    assert not q1_zones.empty
    q1_cities = city_pollution_ranking(df)
    assert q1_cities.iloc[0]["ville_id"] == "paris"

    q2_prof = diurnal_pollution_profile(df)
    assert not q2_prof.empty and "heure_locale" in q2_prof.columns

    q3_dom = dominant_pollutants_by_city(df)
    assert not q3_dom["par_polluant"].empty
    assert not q3_dom["synthese_ville"].empty

    q4_ep = detect_pollution_episodes(df, min_consecutive_hours=3)
    assert not q4_ep.empty
    assert q4_ep.iloc[0]["pollutant"] == "pm25"
    assert q4_ep.iloc[0]["duree_heures"] >= 5
