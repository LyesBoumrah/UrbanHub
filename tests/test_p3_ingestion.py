"""Tests des fonctions pures de p3_pollution/ingestion.py et storage.py. Responsable : P3."""
import gzip
import json
from datetime import datetime, timezone

from p3_pollution.ingestion import (backfill_key, build_latest_messages, is_complete_year, latest_key,
                                    silent_sensors, year_chunks)
from p3_pollution.storage import LocalStorage, encode_json

UTC = timezone.utc
NOW = datetime(2026, 11, 19, 13, 0, 5, tzinfo=UTC)

LOCATION = {
    "ville_id": "lyon", "location_id": 2696, "name": "LYON Centre",
    "sensors": [
        {"sensor_id": 8556, "pollutant": "no2", "unit": "µg/m³"},
        {"sensor_id": 8559, "pollutant": "pm25", "unit": "µg/m³"},
    ],
}
LATEST = [
    {"datetime": {"utc": "2026-11-19T12:00:00Z"}, "value": 17.9, "sensorsId": 8556, "locationsId": 2696},
    {"datetime": {"utc": "2026-11-19T12:00:00Z"}, "value": 3.1, "sensorsId": 5623, "locationsId": 2696},  # so2
    {"datetime": {"utc": "2026-11-18T01:00:00Z"}, "value": 8.0, "sensorsId": 8559, "locationsId": 2696},  # vieux
]


def test_build_latest_messages_enveloppe_et_filtre():
    msgs = build_latest_messages(LOCATION, LATEST, NOW, "batch-1")
    assert [m["sensor_id"] for m in msgs] == [8556, 8559]  # so2 (hors config) ignoré
    m = msgs[0]
    assert m["source"] == "openaq" and m["mode"] == "latest" and m["batch_id"] == "batch-1"
    assert m["fetched_at"] == "2026-11-19T13:00:05Z"
    assert (m["ville_id"], m["pollutant"], m["unit"]) == ("lyon", "no2", "µg/m³")
    assert m["payload"] == LATEST[0]  # payload brut intact


def test_silent_sensors():
    assert silent_sensors(LOCATION, LATEST, NOW) == [8559]  # dernière mesure il y a 36 h (> 30 h)
    assert silent_sensors(LOCATION, [], NOW) == [8556, 8559]  # absents de /latest


def test_cles_bronze():
    assert latest_key(2696, NOW) == "bronze/openaq/2696/date=2026-11-19/130005.json"
    y2023 = (datetime(2023, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, tzinfo=UTC))
    assert backfill_key(8556, 2023, *y2023) == "bronze/openaq_backfill/8556/year=2023/full.json.gz"


def test_cle_partielle_n_ecrase_pas_l_annee_complete():
    lo = datetime(2026, 11, 17, 13, 0, tzinfo=UTC)
    key = backfill_key(8556, 2026, lo, NOW)
    assert key == "bronze/openaq_backfill/8556/year=2026/from=20261117T1300_to=20261119T1300.json.gz"
    assert not key.endswith("full.json.gz")


def test_year_chunks_et_annees_completes():
    start = datetime(2024, 6, 1, tzinfo=UTC)
    chunks = year_chunks(start, NOW)
    assert [c[0] for c in chunks] == [2024, 2025, 2026]
    assert chunks[0][1] == start and chunks[-1][2] == NOW
    complete = [is_complete_year(*c) for c in chunks]
    assert complete == [False, True, False]  # 2024 partielle, 2025 entière, 2026 en cours


def test_year_chunks_quelques_jours():
    chunks = year_chunks(datetime(2026, 11, 12, tzinfo=UTC), NOW)
    assert len(chunks) == 1 and not is_complete_year(*chunks[0])


def test_local_storage_gzip(tmp_path):
    st = LocalStorage(tmp_path)
    key = backfill_key(1, 2025, datetime(2025, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC))
    assert not st.exists(key)
    st.write_json(key, [{"v": "µg/m³"}], compress=True)
    assert st.exists(key)
    assert json.loads(gzip.decompress((tmp_path / key).read_bytes())) == [{"v": "µg/m³"}]


def test_encode_json_compact():
    assert encode_json({"a": 1}) == b'{"a":1}'
