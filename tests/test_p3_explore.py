"""Tests des fonctions pures de p3_pollution/explore.py. Responsable : P3."""
from datetime import datetime, timedelta, timezone

from p3_pollution.explore import is_active, parse_utc, propose_selection, target_sensors

NOW = datetime(2026, 11, 19, 12, 0, tzinfo=timezone.utc)
MAX_AGE = timedelta(hours=48)


def _loc(last_utc, mobile=False):
    return {"isMobile": mobile, "datetimeLast": {"utc": last_utc} if last_utc else None}


def test_parse_utc():
    assert parse_utc("2026-11-19T10:00:00Z") == datetime(2026, 11, 19, 10, tzinfo=timezone.utc)
    assert parse_utc(None) is None


def test_is_active():
    assert is_active(_loc("2026-11-19T10:00:00Z"), NOW, MAX_AGE)
    assert not is_active(_loc("2016-12-05T00:00:00Z"), NOW, MAX_AGE)  # site arrêté
    assert not is_active(_loc("2026-11-19T10:00:00Z", mobile=True), NOW, MAX_AGE)  # capteur mobile
    assert not is_active(_loc(None), NOW, MAX_AGE)


def test_target_sensors_garde_les_5_polluants():
    loc = {"sensors": [
        {"id": 1, "parameter": {"name": "pm25", "units": "µg/m³"}},
        {"id": 2, "parameter": {"name": "temperature", "units": "c"}},
        {"id": 3, "parameter": {"name": "no2", "units": "ppb"}},
    ]}
    assert [s["sensor_id"] for s in target_sensors(loc)] == [1, 3]


def _site(loc_id, dist, pollutants):
    return {"location_id": loc_id, "distance_m": dist, "fresh_pollutants": set(pollutants)}


def test_propose_selection_maximise_la_couverture():
    sites = [
        _site(1, 1000, ["pm25", "no2"]),
        _site(2, 2000, ["pm25", "no2", "pm10"]),
        _site(3, 9000, ["o3"]),
        _site(4, 500, []),  # aucun polluant frais : jamais retenu
    ]
    selected = [s["location_id"] for s in propose_selection(sites, max_sites=2)]
    assert selected == [2, 3]  # le site 3 apporte O3, absent ailleurs


def test_propose_selection_complete_jusqu_au_max():
    sites = [_site(1, 1000, ["pm25"]), _site(2, 2000, ["pm25"]), _site(3, 3000, ["pm25"])]
    assert len(propose_selection(sites, max_sites=2)) == 2
