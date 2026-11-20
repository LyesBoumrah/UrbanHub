"""Tests du Transform P1 (bronze Open-Meteo → silver Parquet)."""
import io
import json
from datetime import date

import pandas as pd

from common.storage import LocalBronzeStorage
from p1_meteo import transform as t
from p1_meteo.extract import bronze_key

PAYLOAD = {"hourly": {
    "time": ["2021-11-01T00:00", "2021-11-01T01:00", "2021-11-01T02:00"],
    "temperature_2m": [14.1, 99.0, None],        # 99 °C : hors bornes ; None : manquant
    "relative_humidity_2m": [91, 91, 92],
    "precipitation": [0.5, 0.0, -1.0],           # -1 mm : hors bornes
    "pressure_msl": [1008.5, 1008.0, 1007.8],
    "cloud_cover": [100, 100, 100],
    "wind_speed_10m": [36.0, 18.0, 0.0],         # km/h
    "wind_direction_10m": [230, 208, 211],
}}


def test_parse_utc_et_conversion_du_vent():
    df = t.parse_openmeteo(PAYLOAD, "bordeaux")
    assert str(df["heure_utc"].dt.tz) == "UTC"
    assert df["heure_utc"].iloc[0] == pd.Timestamp("2021-11-01T00:00", tz="UTC")
    assert df["vent_ms"].tolist() == [10.0, 5.0, 0.0]  # 36 km/h = 10 m/s
    assert set(t.BOUNDS) <= set(df.columns) and (df["ville_id"] == "bordeaux").all()


def test_qualite_marque_sans_supprimer():
    df = t.apply_quality(t.parse_openmeteo(PAYLOAD, "bordeaux"))
    assert len(df) == 3                                    # rien n'est supprimé
    assert df["is_valid"].tolist() == [True, False, False]
    assert df.loc[1, "quality_flag"] == "temperature_c_hors_bornes"
    assert set(df.loc[2, "quality_flag"].split(";")) == {"temperature_c_manquant", "precipitation_mm_hors_bornes"}
    assert df.loc[1, "temperature_c"] == 99.0              # la valeur d'origine est conservée


def test_couverture_par_mois():
    df = t.apply_quality(t.parse_openmeteo(PAYLOAD, "bordeaux"))
    cov = t.coverage_by_month(df, date(2021, 11, 1), date(2021, 11, 30))
    row = cov.iloc[0]
    assert (row["mois"], row["heures_attendues"], row["heures_presentes"], row["heures_valides"]) == \
           ("2021-11", 720, 3, 1)


def test_transform_bout_en_bout_et_idempotence(tmp_path):
    st = LocalBronzeStorage(tmp_path)
    start, end = date(2021, 11, 1), date(2026, 10, 31)
    st.put_bytes(bronze_key("bordeaux", start, end), json.dumps(PAYLOAD).encode())
    villes = [{"ville_id": "bordeaux"}, {"ville_id": "lille"}]  # lille : pas de bronze

    stats = t.transform(st, villes, start, end)
    assert stats["lignes_silver"] == 3 and stats["lignes_valides"] == 1 and stats["villes_sans_bronze"] == ["lille"]
    df = pd.read_parquet(io.BytesIO(st.get_bytes("silver/meteo/year=2021/meteo_heure.parquet")))
    assert len(df) == 3 and df["source"].eq("open-meteo").all()

    again = t.transform(st, villes, start, end)  # relance : mêmes fichiers, pas de doublon
    assert again["lignes_silver"] == 3
