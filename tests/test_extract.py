"""Tests de l'étape EXTRACT (période commune, P1 météo, P2 vélos), sans réseau."""
import json
from dataclasses import dataclass
from datetime import date

import pytest

from common.period import study_period, year_month
from common.storage import LocalBronzeStorage
from p1_meteo import extract as p1
from p2_velos import extract as p2

START, END = date(2021, 11, 1), date(2026, 10, 31)


def test_periode_commune():
    assert study_period(5, today=date(2026, 11, 20)) == (START, END)
    assert year_month(date(2024, 11, 3)) == "202411"


# ------------------------------------------------------------------ P1 météo

VILLES = [{"ville_id": "paris", "lat": 48.85, "lon": 2.35}, {"ville_id": "lyon", "lat": 45.76, "lon": 4.83}]


def fake_meteo(ville, start, end, user_agent):
    return json.dumps({"hourly": {"time": ["2021-11-01T00:00", "2021-11-01T01:00"],
                                  "temperature_2m": [8.1, 7.9]}}).encode()


def test_p1_cle_et_parametres():
    assert p1.bronze_key("paris", START, END) == "bronze/meteo/ville=paris/2021-11-01_2026-10-31.json"
    params = p1.request_params(VILLES[0], START, END)
    assert params["timezone"] == "UTC" and "visibility" not in params["hourly"]
    assert "relative_humidity_2m" in params["hourly"] and "cloud_cover" in params["hourly"]


def test_p1_reponse_vide_rejetee():
    with pytest.raises(ValueError):
        p1.validate_payload(b'{"error": true, "reason": "quota"}')


def test_p1_extract_copie_brute_et_reprise(tmp_path):
    st = LocalBronzeStorage(tmp_path)
    first = p1.extract(st, VILLES, START, END, "test", fetch=fake_meteo, delay_s=0)
    assert first["files_downloaded"] == 2 and first["errors"] == []
    assert st.get_bytes(p1.bronze_key("paris", START, END)) == fake_meteo(None, None, None, None)  # tel quel
    second = p1.extract(st, VILLES, START, END, "test", fetch=fake_meteo, delay_s=0)
    assert second["files_downloaded"] == 0 and second["files_skipped"] == 2


# ------------------------------------------------------------------ P2 vélos

@dataclass(frozen=True)
class Dump:
    ville_id: str
    year: int
    ym: str
    filename: str
    url: str = ""
    size_bytes: int = 0


DUMPS = [Dump("paris", 2024, "202410", "202410-velib-stats.parquet"),  # dans la période
         Dump("paris", 2024, "202411", "202411-velib-stats.parquet"),  # dans la période
         Dump("lyon", 2026, "202611", "202611-velov-stats.parquet")]  # après le 31/10/2026 : exclu


def test_p2_cle_et_filtre_periode():
    assert p2.bronze_key(DUMPS[1]) == "bronze/velos_historique/paris/2024/202411-velib-stats.parquet"
    kept = [d.ym for d in p2.in_period(DUMPS, START, END)]
    assert kept == ["202410", "202411"]


def test_p2_extract_copie_brute_et_reprise(tmp_path):
    st = LocalBronzeStorage(tmp_path)
    first = p2.extract(st, DUMPS, START, END, "test", fetch=lambda d, ua: b"PAR1" + d.filename.encode())
    assert first["files_downloaded"] == 2 and first["villes"]["paris"]["months"] == 2
    assert st.get_bytes(p2.bronze_key(DUMPS[1])) == b"PAR1202411-velib-stats.parquet"
    second = p2.extract(st, DUMPS, START, END, "test", fetch=lambda d, ua: b"")
    assert second["files_downloaded"] == 0 and second["files_skipped"] == 2
