"""Tests unitaires P2 (règles de qualité pures, normalisation, dédoublonnage, stockage local).

Conçus pour tourner avec uniquement la bibliothèque standard Python + pytest (CI).
Responsable : P2.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from common.alerts import emit_alert
from common.storage import LocalBronzeStorage
from p2_velos.backfill import (
    compute_backfill_window,
    filter_dumps_for_window,
    group_dumps_by_year,
    parse_dump_filename,
)
from p2_velos.cleaning import (
    build_silver_historical_key,
    build_silver_streaming_key,
    clean_bronze_payload,
    deduplicate_records,
    extract_raw_records,
    parse_historical_bronze_key,
    replay_bronze_to_silver,
)
from p2_velos.quality import (
    classify_station_status,
    filter_and_audit_records,
    parse_citybikes_timestamp,
    validate_coordinates,
    validate_station_record,
)

PARIS_BBOX = {"lat_min": 48.65, "lat_max": 49.05, "lon_min": 2.05, "lon_max": 2.65}
REF_NOW = datetime(2026, 11, 19, 11, 30, 0, tzinfo=timezone.utc)


def _valid_record(**overrides):
    base = {
        "ville_id": "paris",
        "network_id": "velib",
        "station_id": "0003131bffcf527ba3a560fc46d38b6f",
        "station_name": "Argenson - Château",
        "latitude": 48.888559,
        "longitude": 2.264200,
        "bikes_available": 19,
        "free_slots": 6,
        "declared_slots": 25,
        "timestamp": "2026-11-19T11:27:15.476329+00:00Z",
    }
    base.update(overrides)
    return base


# ============================================================================
# 1. Parsing des timestamps CityBikes
# ============================================================================
def test_parse_citybikes_double_utc_suffix():
    dt = parse_citybikes_timestamp("2026-11-19T11:27:15.476329+00:00Z")
    assert dt.tzinfo == timezone.utc
    assert (dt.year, dt.month, dt.day, dt.hour, dt.minute) == (2026, 11, 19, 11, 27)


def test_parse_citybikes_standard_z_and_offset():
    dt_z = parse_citybikes_timestamp("2026-11-19T11:27:15Z")
    dt_offset = parse_citybikes_timestamp("2026-11-19T12:27:15+01:00")
    assert dt_z == dt_offset == datetime(2026, 11, 19, 11, 27, 15, tzinfo=timezone.utc)


def test_parse_citybikes_invalid_timestamp():
    with pytest.raises(ValueError):
        parse_citybikes_timestamp("not-a-timestamp")
    with pytest.raises(ValueError):
        parse_citybikes_timestamp("")


# ============================================================================
# 2. Validation des enregistrements stations
# ============================================================================
def test_valid_record_passes():
    ok, reasons = validate_station_record(_valid_record(), bbox=PARIS_BBOX, now_utc=REF_NOW)
    assert ok is True
    assert reasons == []


def test_missing_required_fields():
    rec = _valid_record(station_name=None, bikes_available=None)
    ok, reasons = validate_station_record(rec, bbox=PARIS_BBOX, now_utc=REF_NOW)
    assert ok is False
    assert "missing_station_name" in reasons
    assert "missing_bikes_available" in reasons


def test_negative_counters_rejected():
    ok_bikes, reasons_bikes = validate_station_record(
        _valid_record(bikes_available=-1), bbox=PARIS_BBOX, now_utc=REF_NOW
    )
    ok_slots, reasons_slots = validate_station_record(
        _valid_record(free_slots=-3), bbox=PARIS_BBOX, now_utc=REF_NOW
    )
    assert ok_bikes is False and "negative_bikes_available" in reasons_bikes
    assert ok_slots is False and "negative_free_slots" in reasons_slots


def test_zero_capacity_and_declared_capacity_mismatch():
    ok_zero, reasons_zero = validate_station_record(
        _valid_record(bikes_available=0, free_slots=0, declared_slots=0),
        bbox=PARIS_BBOX,
        now_utc=REF_NOW,
    )
    assert ok_zero is False and "zero_capacity" in reasons_zero

    ok_over, reasons_over = validate_station_record(
        _valid_record(bikes_available=40, free_slots=10, declared_slots=20),
        bbox=PARIS_BBOX,
        now_utc=REF_NOW,
    )
    assert ok_over is False and "capacity_exceeds_declared_slots" in reasons_over


def test_coordinates_validation():
    assert validate_coordinates(0.0, 0.0, bbox=PARIS_BBOX) == ["null_island_coordinates"]
    assert validate_coordinates(95.0, 2.3, bbox=PARIS_BBOX) == ["coordinates_out_of_world_bounds"]
    # Coordonnées de Lyon testées contre la bbox de Paris
    assert validate_coordinates(45.7640, 4.8357, bbox=PARIS_BBOX) == ["coordinates_outside_city_bbox"]
    assert validate_coordinates(48.8566, 2.3522, bbox=PARIS_BBOX) == []


def test_freshness_validation():
    stale_ts = (REF_NOW - timedelta(minutes=45)).isoformat()
    future_ts = (REF_NOW + timedelta(minutes=15)).isoformat()

    ok_stale, reasons_stale = validate_station_record(
        _valid_record(timestamp=stale_ts), bbox=PARIS_BBOX, now_utc=REF_NOW, max_age_minutes=30
    )
    ok_future, reasons_future = validate_station_record(
        _valid_record(timestamp=future_ts), bbox=PARIS_BBOX, now_utc=REF_NOW, max_age_minutes=30
    )
    assert ok_stale is False and "stale_timestamp" in reasons_stale
    assert ok_future is False and "timestamp_in_future" in reasons_future


# ============================================================================
# 3. Classification métier & rééquilibrage (Q5)
# ============================================================================
def test_classify_station_status():
    empty = classify_station_status(bikes_available=0, free_slots=20)
    assert empty["statut"] == "PENURIE_CRITIQUE"
    assert empty["action_reequilibrage"] == "REAPPROVISIONNER"
    assert empty["est_vide"] == 1
    assert empty["est_critique"] == 1
    assert empty["delta_velos_equilibre"] == 10

    full = classify_station_status(bikes_available=19, free_slots=1)
    assert full["statut"] == "SATURATION_CRITIQUE"
    assert full["action_reequilibrage"] == "DESENGORGER"
    assert full["est_critique"] == 1
    assert full["delta_velos_equilibre"] == -9

    normal = classify_station_status(bikes_available=10, free_slots=10)
    assert normal["statut"] == "NORMAL"
    assert normal["action_reequilibrage"] == "AUCUNE"
    assert normal["est_critique"] == 0
    assert normal["taux_utilisation"] == 0.5


# ============================================================================
# 4. Audit de lot & pipeline bronze -> silver
# ============================================================================
def test_filter_and_audit_records():
    batch = [
        _valid_record(station_id="s1"),
        _valid_record(station_id="s2", bikes_available=-2),
        _valid_record(station_id="s3", latitude=0.0, longitude=0.0),
    ]
    report = filter_and_audit_records(batch, bbox=PARIS_BBOX, now_utc=REF_NOW)
    assert report.total_count == 3
    assert report.valid_count == 1
    assert report.rejected_count == 2
    assert report.rejection_rate == pytest.approx(0.6667, abs=1e-4)
    assert report.reasons_summary["negative_bikes_available"] == 1
    assert report.reasons_summary["null_island_coordinates"] == 1


def test_clean_bronze_payload_end_to_end():
    bronze_envelope = {
        "meta": {
            "ville_id": "paris",
            "network_id": "velib",
            "collected_at_utc": REF_NOW.isoformat(),
        },
        "payload": {
            "network": {
                "id": "velib",
                "stations": [
                    {
                        "id": "st_1",
                        "name": "République",
                        "latitude": 48.8675,
                        "longitude": 2.3638,
                        "timestamp": "2026-11-19T11:28:00.000000+00:00Z",
                        "free_bikes": 2,
                        "empty_slots": 28,
                        "extra": {"slots": 30, "virtual": False},
                    },
                    # Doublon exact
                    {
                        "id": "st_1",
                        "name": "République",
                        "latitude": 48.8675,
                        "longitude": 2.3638,
                        "timestamp": "2026-11-19T11:28:00.000000+00:00Z",
                        "free_bikes": 2,
                        "empty_slots": 28,
                        "extra": {"slots": 30, "virtual": False},
                    },
                    # Station invalide (hors bbox)
                    {
                        "id": "st_bad",
                        "name": "Station Perdue",
                        "latitude": 43.2965,
                        "longitude": 5.3698,
                        "timestamp": "2026-11-19T11:28:00Z",
                        "free_bikes": 5,
                        "empty_slots": 5,
                        "extra": {"slots": 10},
                    },
                ],
            }
        },
    }

    raw = extract_raw_records(bronze_envelope)
    assert len(raw) == 3
    assert len(deduplicate_records(raw)) == 2

    stations, releves, report = clean_bronze_payload(
        bronze_envelope,
        bbox=PARIS_BBOX,
        now_utc=REF_NOW,
    )
    assert report.rejected_count == 1
    assert len(stations) == 1
    assert len(releves) == 1
    releve = releves[0]
    assert releve["station_id"] == "st_1"
    assert releve["bikes_available"] == 2
    assert releve["free_slots"] == 28
    assert releve["capacity"] == 30
    assert releve["est_critique"] == 1
    assert releve["heure_utc"] == datetime(2026, 11, 19, 11, 0, 0, tzinfo=timezone.utc)


def test_local_bronze_storage_and_alerts(tmp_path: Path):
    storage = LocalBronzeStorage(base_dir=tmp_path)
    key = "bronze/velos/paris/dt=2026-11-19/hr=11/velib_20261119T113000Z.json"
    storage.put_json(key, {"ok": True})
    assert storage.exists(key)
    assert storage.get_json(key) == {"ok": True}
    assert key in storage.list("bronze/velos/paris")

    bin_key = "bronze/velos_historique/paris/2025/202501-velib-stats.parquet"
    storage.put_bytes(bin_key, b"PAR1test")
    assert storage.get_bytes(bin_key) == b"PAR1test"

    alert = emit_alert("p2_velos", "WARNING", "Test alerte", ville_id="paris")
    assert alert["niveau"] == "WARNING"
    with pytest.raises(ValueError):
        emit_alert("p2_velos", "UNKNOWN", "Niveau invalide")


# ============================================================================
# 5. Backfill historique 5 ans (data.citybik.es)
# ============================================================================
def test_backfill_window_and_dump_filtering():
    start_ym, end_ym = compute_backfill_window(years=5, now_utc=REF_NOW)
    assert (start_ym, end_ym) == ("202111", "202611")

    assert parse_dump_filename("202503-velib-stats.parquet") == ("202503", 2025, 3, "velib")
    assert parse_dump_filename("202503-le-velo-star-stats.parquet") == ("202503", 2025, 3, "le-velo-star")
    assert parse_dump_filename("invalid_file.csv") is None

    entries = {
        2020: [{"name": "202010-velib-stats.parquet", "size": 100}],  # hors fenêtre 5 ans
        2024: [
            {"name": "202411-velib-stats.parquet", "size": 200},
            {"name": "202411-other-net-stats.parquet", "size": 300},
        ],
        2026: [{"name": "202609-velov-stats.parquet", "size": 400}],
    }
    dumps = filter_dumps_for_window(
        entries_by_year=entries,
        network_to_ville={"velib": "paris", "velov": "lyon"},
        years=5,
        now_utc=REF_NOW,
    )
    assert [(d.ville_id, d.ym) for d in dumps] == [("paris", "202411"), ("lyon", "202609")]
    assert dumps[0].url.endswith("/2024/202411-velib-stats.parquet")

    grouped = group_dumps_by_year(dumps)
    assert list(grouped.keys()) == [2024, 2026]
    assert [d.ym for d in grouped[2024]] == ["202411"]
    assert [d.ym for d in grouped[2026]] == ["202609"]


def test_parse_historical_bronze_key_and_replay(tmp_path: Path):
    parsed = parse_historical_bronze_key(
        "bronze/velos_historique/rennes/2025/202506-le-velo-star-stats.parquet"
    )
    assert parsed == {
        "ville_id": "rennes",
        "year": "2025",
        "ym": "202506",
        "network_id": "le-velo-star",
    }
    assert parse_historical_bronze_key("bronze/velos/paris/snapshot.json") is None

    # Test du rejeu streaming sur une archive ancienne (> 30 min -> acceptée en rejeu)
    storage = LocalBronzeStorage(base_dir=tmp_path)
    storage.put_json(
        "bronze/velos/paris/dt=2025-01-10/hr=08/velib_20250110T080000Z.json",
        {
            "meta": {"ville_id": "paris", "network_id": "velib"},
            "payload": {
                "network": {
                    "id": "velib",
                    "stations": [
                        {
                            "id": "st_old",
                            "name": "Bastille",
                            "latitude": 48.8531,
                            "longitude": 2.3691,
                            "timestamp": "2025-01-10T08:00:00+00:00Z",
                            "free_bikes": 10,
                            "empty_slots": 10,
                            "extra": {"slots": 20},
                        }
                    ],
                }
            },
        },
    )
    summary = replay_bronze_to_silver(
        scope="streaming",
        cities=["paris"],
        use_local=True,
        storage=storage,
        local_silver_base=tmp_path / "silver",
    )
    assert summary["streaming_files"] == 1
    assert summary["streaming_silver_rows"] == 1
    assert summary["errors"] == []

    assert build_silver_streaming_key(REF_NOW) == (
        "silver/velos/dt=2026-11-19/hr=11/velo_releve_20261119T113000Z.parquet"
    )
    assert build_silver_historical_key("paris", 2025, "202506") == (
        "silver/velos_historique/paris/2025/velo_releve_202506.parquet"
    )
