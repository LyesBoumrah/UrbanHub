"""Tests unitaires P1 — ingestion parallèle Open-Meteo (5 ans + récent), conversion d'unités,
règles de qualité et analyses Q1–Q4."""

from __future__ import annotations

from datetime import date
import threading
import time

import numpy as np
import pandas as pd

from common.storage import LocalBronzeStorage
from p1_meteo import ingestion as p1_ingestion
from p1_meteo.analytics import (
    anomalous_periods,
    daily_aggregates,
    daily_anomalies,
    extreme_days,
    seasonal_temperature,
    visibility_by_class,
    visibility_correlations,
)
from p1_meteo.cleaning import (
    SILVER_COLS,
    SILVER_KEY,
    STATION_SILVER_KEY,
    aggregate_ville_heure,
    build_station_table,
    clean_all,
    clean_openmeteo_json,
    convert_openmeteo_units,
)
from p1_meteo.ingestion import (
    bronze_key,
    default_period,
    merge_archive_and_recent,
    station_id_for_ville,
)
from p1_meteo.quality import (
    check_quality,
    completeness,
    flag_temperature_spikes,
    nullify_out_of_bounds,
    quality_summary,
)


# ---------------------------------------------------------------- Tests qualité de base

def test_temperature_hors_bornes():
    df = pd.DataFrame(
        {
            "heure_utc": [pd.Timestamp("2026-01-01T00:00:00Z")],
            "temperature": [99.0],
        }
    )
    result = check_quality(df)
    assert result.loc[0, "is_valid"] == False  # noqa: E712
    assert "temperature_hors_bornes" in result.loc[0, "quality_flags"]


def test_donnees_valides():
    df = pd.DataFrame(
        {
            "heure_utc": [pd.Timestamp("2026-01-01T00:00:00Z")],
            "temperature": [12.5],
            "pressure": [1013.2],
            "wind_speed": [4.1],
            "visibility": [16000.0],
            "precipitation": [0.0],
        }
    )
    result = check_quality(df)
    assert result.loc[0, "is_valid"] == True  # noqa: E712
    assert result.loc[0, "quality_flags"] == ""


def test_precipitation_negative():
    df = pd.DataFrame(
        {
            "heure_utc": [pd.Timestamp("2026-01-01T00:00:00Z")],
            "precipitation": [-1.0],
        }
    )
    result = check_quality(df)
    assert result.loc[0, "is_valid"] == False  # noqa: E712
    assert "precipitation_negative" in result.loc[0, "quality_flags"]


def test_resume_qualite():
    df = pd.DataFrame({"is_valid": [True, False, True]})
    assert quality_summary(df) == {
        "total": 3,
        "valides": 2,
        "invalides": 1,
        "taux_validite": 66.67,
    }


def test_bornes_et_pics_thermiques():
    df = pd.DataFrame(
        {
            "station_id": ["07149099999", "07149099999", "07149099999"],
            "heure_utc": pd.date_range("2026-07-01", periods=3, freq="1h", tz="UTC"),
            "temperature": [20.0, 48.0, 21.0],  # pic isolé +28 °C puis -27 °C
            "dew_point": [12.0, 12.0, 25.0],    # rosée > température sur la 3e ligne
            "pressure": [1015.0, 750.0, 1014.0],
        }
    )
    spikes = flag_temperature_spikes(df)
    assert list(spikes) == [False, True, False]

    nullified = nullify_out_of_bounds(df)
    assert np.isnan(nullified.loc[1, "pressure"])
    assert np.isnan(nullified.loc[2, "dew_point"])
    comp = completeness(nullified, ["temperature", "pressure"])
    assert comp["temperature"] == 100.0 and comp["pressure"] == 66.67


# ---------------------------------------------------------------- Tests ingestion & nettoyage Open-Meteo

def test_periode_5_ans_et_fusion_donnees_recentes():
    start, end = default_period(5, today=date(2026, 11, 20))
    assert start == "2021-11-20" and end == "2026-11-20"
    assert bronze_key("paris", start, end) == "bronze/meteo/paris/2021-11-20_2026-11-20.json"
    assert station_id_for_ville({"ville_id": "paris"}) == "FR-PARIS"

    archive = {
        "hourly": {
            "time": ["2026-11-19T22:00", "2026-11-19T23:00"],
            "temperature_2m": [11.0, 10.5],
            "visibility": [None, None],
        }
    }
    recent = {
        "hourly": {
            "time": ["2026-11-19T23:00", "2026-11-20T00:00"],
            "temperature_2m": [10.6, 10.2],
            "visibility": [14000.0, 15000.0],
        }
    }
    merged = merge_archive_and_recent(archive, recent)
    assert merged["hourly"]["time"] == ["2026-11-19T22:00", "2026-11-19T23:00", "2026-11-20T00:00"]
    assert merged["hourly"]["temperature_2m"] == [11.0, 10.6, 10.2]
    assert merged["hourly"]["visibility"] == [None, 14000.0, 15000.0]


def test_clean_openmeteo_json_conversion_unites_et_8_variables(tmp_path):
    raw_df = pd.DataFrame({"wind_speed": [36.0], "temperature": [68.0], "visibility": [12.0]})
    conv = convert_openmeteo_units(
        raw_df,
        {"wind_speed_10m": "km/h", "temperature_2m": "°F", "visibility": "km"},
    )
    assert conv.loc[0, "wind_speed"] == 10.0
    assert conv.loc[0, "temperature"] == 20.0
    assert conv.loc[0, "visibility"] == 12000.0

    payload = {
        "latitude": 48.8566,
        "longitude": 2.3522,
        "hourly_units": {"wind_speed_10m": "km/h", "temperature_2m": "°C"},
        "hourly": {
            "time": ["2026-11-19T10:00", "2026-11-19T11:00", "2026-11-19T12:00"],
            "temperature_2m": [14.0, None, 16.0],  # valeur manquante interpolée à 15.0
            "relative_humidity_2m": [80.0, 82.0, 75.0],
            "dew_point_2m": [10.6, 11.9, 11.6],
            "precipitation": [0.0, None, 0.4],
            "pressure_msl": [1015.0, 1014.5, 1014.0],
            "wind_speed_10m": [18.0, 21.6, 14.4],  # km/h -> 5.0, 6.0, 4.0 m/s
            "wind_direction_10m": [240.0, 250.0, 245.0],
            "visibility": [None, 18000.0, 12000.0],  # 1re heure estimée physiquement
        },
    }
    cleaned = clean_openmeteo_json(payload, ville_id="paris")
    assert len(cleaned) == 3
    assert list(cleaned.columns) == SILVER_COLS
    assert cleaned.loc[0, "wind_speed"] == 5.0
    assert cleaned.loc[1, "temperature"] == 15.0
    assert cleaned.loc[0, "visibility"] > 0

    # Vérification des 8 variables exigées par l'énoncé
    for required in (
        "station_id", "timestamp", "wind_speed", "pressure",
        "wind_direction", "precipitation", "temperature", "visibility",
    ):
        assert required in cleaned.columns
        assert cleaned[required].notna().all()

    # Sans fichier bronze : rien n'est écrit en couche silver
    empty_storage = LocalBronzeStorage(base_dir=tmp_path / "empty")
    assert clean_all(storage=empty_storage).empty
    assert empty_storage.list("silver/") == []

    # Avec fichier bronze : seuls les Parquet silver issus du bronze sont créés
    storage = LocalBronzeStorage(base_dir=tmp_path / "with_bronze")
    storage.put_json("bronze/meteo/paris/2021-11-20_2026-11-20.json", payload)
    all_df = clean_all(storage=storage)
    assert len(all_df) == 3
    assert storage.exists(SILVER_KEY)
    assert storage.exists(STATION_SILVER_KEY)
    assert set(storage.list("silver/")) == {SILVER_KEY, STATION_SILVER_KEY}


# ---------------------------------------------------------------- Tests analytiques Q1–Q4

def test_analyses_q1_a_q4():
    rng = np.random.default_rng(42)
    hours = pd.date_range("2026-01-01", periods=24 * 60, freq="1h", tz="UTC")
    temp = 10.0 + 6.0 * np.sin(2 * np.pi * np.arange(len(hours)) / 24.0) + rng.normal(0, 0.5, len(hours))
    # Vague de chaleur anormale sur 5 jours (jours 30 à 34)
    temp[24 * 30:24 * 35] += 26.0
    rh = np.clip(95.0 - 1.5 * temp, 35.0, 100.0)
    vis = np.clip(25000.0 - 220.0 * rh, 150.0, 25000.0)
    hourly = pd.DataFrame({
        "station_id": "07149099999",
        "ville_id": "paris",
        "heure_utc": hours,
        "temperature": temp,
        "dew_point": temp - 3.0,
        "relative_humidity": rh,
        "pressure": 1013.0,
        "wind_speed": 3.5,
        "wind_direction": 220.0,
        "precipitation": 0.1,
        "visibility": vis,
    })

    st_table = build_station_table(obs=hourly)
    agg = aggregate_ville_heure(hourly, st_table)
    assert len(agg) == len(hours)

    daily = daily_aggregates(hourly)
    anom = daily_anomalies(daily)
    periods = anomalous_periods(anom, var="tmoy", z_threshold=1.2, min_days=3)
    assert not periods.empty

    corrs = visibility_correlations(hourly)
    assert not corrs.empty
    vis_cls = visibility_by_class(hourly)
    assert "humidite" in vis_cls

    seas = seasonal_temperature(hourly)
    assert not seas["mensuel_ville"].empty

    extremes = extreme_days(daily)
    assert not extremes.empty and "canicule" in " ".join(extremes["criteres"])


def test_ingestion_p1_vraiment_parallele(tmp_path, monkeypatch):
    """Vérifie que `ingest()` télécharge plusieurs villes simultanément (pas une par une)."""
    active = 0
    max_concurrent = 0
    lock = threading.Lock()

    def fake_fetch(ville, start_date, end_date, storage, **kwargs):
        nonlocal active, max_concurrent
        with lock:
            active += 1
            max_concurrent = max(max_concurrent, active)
        time.sleep(0.08)
        key = bronze_key(ville["ville_id"], start_date, end_date)
        storage.put_json(key, {"hourly": {"time": ["2026-11-19T12:00"], "temperature_2m": [12.0]}})
        with lock:
            active -= 1
        return key

    monkeypatch.setattr(p1_ingestion, "fetch_ville_meteo", fake_fetch)
    storage = LocalBronzeStorage(base_dir=tmp_path)
    written = p1_ingestion.ingest(
        nb_years=5,
        max_workers=4,
        limit_villes=4,
        skip_existing=False,
        delay_seconds=0.0,
        storage=storage,
    )
    assert len(written) == 4
    assert max_concurrent >= 2  # preuve d'exécution parallèle effective


def test_minio_storage_ensure_bucket_sans_deadlock(monkeypatch):
    """Vérifie que MinIOStorage.ensure_bucket() crée les 3 buckets Medallion sans deadlock."""
    import sys
    import types
    from common.storage import MinIOStorage

    created = []

    class FakeMinio:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def bucket_exists(self, bucket):
            return bucket in created

        def make_bucket(self, bucket):
            created.append(bucket)

    fake_mod = types.SimpleNamespace(Minio=FakeMinio)
    monkeypatch.setitem(sys.modules, "minio", fake_mod)

    st = MinIOStorage(endpoint="minio:9000", access_key="u", secret_key="p")
    st.ensure_bucket()
    st.ensure_bucket()  # idempotent
    assert created == ["bronze", "silver", "gold"]


def test_minio_storage_routage_trois_buckets(monkeypatch):
    """Vérifie le routage physique vers les 3 buckets dédiés bronze, silver et gold (common + P3)."""
    import io
    import sys
    import types
    from common.storage import MinIOStorage
    from p3_pollution.storage import MinioStorage as P3MinioStorage

    store: dict[str, dict[str, bytes]] = {}

    class FakeResp:
        def __init__(self, raw: bytes):
            self._raw = raw

        def read(self):
            return self._raw

        def close(self):
            pass

        def release_conn(self):
            pass

    class FakeMinio:
        def __init__(self, *args, **kwargs):
            del args, kwargs

        def bucket_exists(self, bucket):
            return bucket in store

        def make_bucket(self, bucket):
            store.setdefault(bucket, {})

        def put_object(self, bucket_name, object_name, data, length, content_type=""):
            del length, content_type
            store.setdefault(bucket_name, {})[object_name] = data.read()

        def get_object(self, bucket_name, object_name):
            return FakeResp(store[bucket_name][object_name])

        def stat_object(self, bucket_name, object_name):
            if bucket_name not in store or object_name not in store[bucket_name]:
                raise KeyError(f"{bucket_name}/{object_name}")
            return True

        def list_objects(self, bucket_name, prefix="", recursive=True):
            del recursive
            return [
                types.SimpleNamespace(object_name=k)
                for k in sorted(store.get(bucket_name, {}))
                if k.startswith(prefix)
            ]

    fake_mod = types.SimpleNamespace(Minio=FakeMinio)
    fake_err = types.SimpleNamespace(S3Error=KeyError)
    monkeypatch.setitem(sys.modules, "minio", fake_mod)
    monkeypatch.setitem(sys.modules, "minio.error", fake_err)

    st = MinIOStorage(endpoint="minio:9000", access_key="u", secret_key="p")
    st.put_json("bronze/meteo/paris/2026.json", {"ok": 1})
    st.put_bytes("silver/meteo/meteo_obs.parquet", b"PAR1")
    st.put_json("gold/synthese_indicateurs_urbains.json", {"villes": 12})

    assert set(store.keys()) == {"bronze", "silver", "gold"}
    assert "meteo/paris/2026.json" in store["bronze"]
    assert "meteo/meteo_obs.parquet" in store["silver"]
    assert "synthese_indicateurs_urbains.json" in store["gold"]
    assert st.list("bronze/") == ["bronze/meteo/paris/2026.json"]
    assert st.list("silver/") == ["silver/meteo/meteo_obs.parquet"]
    assert st.list("gold/") == ["gold/synthese_indicateurs_urbains.json"]
    assert st.get_json("gold/synthese_indicateurs_urbains.json") == {"villes": 12}

    p3 = P3MinioStorage("minio:9000", "u", "p")
    uri = p3.write_json("bronze/openaq/2162591/latest.json", [{"value": 12.5}])
    assert uri == "s3://bronze/openaq/2162591/latest.json"
    assert "openaq/2162591/latest.json" in store["bronze"]
    assert "bronze/openaq/2162591/latest.json" in p3.list_keys("bronze/openaq/")
    assert p3.read_json("bronze/openaq/2162591/latest.json") == [{"value": 12.5}]
    del io


def test_clip_hourly_payload_et_clean_sans_heures_futures():
    """Vérifie le respect strict de la période demandée (pas de débordement past_days ni d'heures futures)."""
    from datetime import datetime, timezone
    from p1_meteo.ingestion import clip_hourly_payload

    payload = {
        "hourly": {
            "time": [
                "2026-11-06T00:00",  # antérieur à start_date (1 semaine = 2026-11-13)
                "2026-11-13T00:00",  # borne début valide
                "2026-11-20T11:00",  # heure courante valide
                "2026-11-20T23:00",  # heure future de prévision -> à exclure
            ],
            "temperature_2m": [8.0, 10.0, 12.0, 15.0],
        }
    }
    now_ref = datetime(2026, 11, 20, 11, 30, tzinfo=timezone.utc)
    clipped = clip_hourly_payload(payload, start_date="2026-11-13", end_date="2026-11-20", now_utc=now_ref)
    assert clipped["hourly"]["time"] == ["2026-11-13T00:00", "2026-11-20T11:00"]
    assert clipped["hourly"]["temperature_2m"] == [10.0, 12.0]

    cleaned = clean_openmeteo_json(
        payload,
        ville_id="paris",
        start_date="2026-11-13",
        end_date="2026-11-20",
        now_utc=pd.Timestamp("2026-11-20T11:30:00Z"),
    )
    assert len(cleaned) == 2
    assert cleaned["heure_utc"].min() == pd.Timestamp("2026-11-13T00:00:00Z")
    assert cleaned["heure_utc"].max() == pd.Timestamp("2026-11-20T11:00:00Z")
