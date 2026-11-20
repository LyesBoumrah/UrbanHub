"""Tests du Transform P3 (bronze OpenAQ CSV → silver Parquet)."""
import gzip
import io
from datetime import date

import pandas as pd

from common.storage import LocalBronzeStorage
from p3_pollution import transform as t
from p3_pollution.ingestion import BRONZE_PREFIX

LOCS = [{"location_id": 2162724, "predecessor_location_ids": [3985], "ville_id": "marseille",
         "name": "MARSEILLE ST LOUIS", "latitude": 43.34, "longitude": 5.36, "distance_m": 5800,
         "provider": "EEA France", "sensors": [{"sensor_id": 1, "pollutant": "no2", "unit": "µg/m³"}]}]
HEADER = "location_id,sensors_id,location,datetime,lat,lon,parameter,units,value\n"


def csv(rows):
    return HEADER + "".join(",".join(map(str, r)) + "\n" for r in rows)


def test_heure_reportee_au_debut_et_en_utc():
    raw = t.parse_csv_text(csv([(3985, 11, "x", "2023-01-01T01:00:00+01:00", 0, 0, "no2", "µg/m³", 20.5)]))
    df = t.normalize(raw, t.station_index(LOCS))
    # fin d'heure 01:00+01:00 = 00:00 UTC  →  début d'heure = 23:00 UTC la veille
    assert df.loc[0, "heure_utc"] == pd.Timestamp("2022-12-31T23:00", tz="UTC")
    assert (df.loc[0, "station_id"], df.loc[0, "location_id_source"]) == (2162724, 3985)  # prédécesseur rattaché


def test_polluants_hors_sujet_ecartes():
    raw = t.parse_csv_text(csv([(2162724, 1, "x", "2025-01-01T01:00:00+01:00", 0, 0, p, "µg/m³", 5)
                                for p in ("no2", "no", "so2", "pm25")]))
    assert sorted(t.normalize(raw, t.station_index(LOCS))["polluant"]) == ["no2", "pm25"]


def test_dedoublonnage_priorite_station_actuelle():
    rows = [(3985, 11, "x", "2024-01-15T10:00:00+01:00", 0, 0, "no2", "µg/m³", 30.0),      # ancien id
            (2162724, 1, "x", "2024-01-15T10:00:00+01:00", 0, 0, "no2", "µg/m³", 31.0)]    # actuel, même heure
    df = t.deduplicate(t.normalize(t.parse_csv_text(csv(rows)), t.station_index(LOCS)))
    assert len(df) == 1 and df.iloc[0]["valeur"] == 31.0


def test_conversion_unites():
    v, unknown = t.to_ugm3(pd.Series([1.0, 1.0, 10.0, 5.0]), pd.Series(["mg/m³", "ppb", "µg/m³", "lux"]),
                           pd.Series(["co", "no2", "o3", "no2"]))
    assert v.round(2).tolist() == [1000.0, 1.88, 10.0, 5.0] and unknown.tolist() == [False, False, False, True]


def test_qualite_marque_sans_supprimer():
    df = pd.DataFrame({"valeur": [12.0, -0.4, None, 5000.0], "polluant": ["no2", "o3", "pm25", "pm25"],
                       "_unite_inconnue": [False] * 4})
    out = t.apply_quality(df)
    assert len(out) == 4 and out["is_valid"].tolist() == [True, False, False, False]
    assert out["quality_flag"].tolist() == ["", "valeur_negative", "valeur_manquante", "au_dessus_plafond"]


def test_transform_bout_en_bout(tmp_path):
    st = LocalBronzeStorage(tmp_path)
    for lid, day, val in ((3985, "20230101", 20.0), (2162724, "20250101", 8.0)):
        key = f"{BRONZE_PREFIX}/locationid={lid}/year={day[:4]}/month=01/location-{lid}-{day}.csv.gz"
        ts = f"{day[:4]}-01-01T12:00:00+01:00"
        st.put_bytes(key, gzip.compress(csv([(lid, 9, "x", ts, 0, 0, "no2", "µg/m³", val),
                                             (lid, 9, "x", ts, 0, 0, "so2", "µg/m³", 1.0)]).encode()))
    stats = t.transform(st, LOCS, date(2021, 11, 1), date(2026, 10, 31), workers=2)
    assert stats["fichiers_bronze_lus"] == 2 and stats["lignes_silver"] == 2
    assert stats["lignes_issues_des_predecesseurs"] == 1
    y2023 = pd.read_parquet(io.BytesIO(st.get_bytes("silver/pollution/year=2023/pollution_heure.parquet")))
    assert y2023.iloc[0]["station_id"] == 2162724 and y2023.iloc[0]["heure_utc"].hour == 10
    assert len(pd.read_parquet(io.BytesIO(st.get_bytes(t.STATIONS_KEY)))) == 1
