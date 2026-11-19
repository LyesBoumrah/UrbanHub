"""Tests du limiteur de débit partagé (p3_pollution/openaq_client.py). Responsable : P3."""
import threading
import time

import pytest

from p3_pollution.openaq_client import OpenAQClient, SharedRateLimiter


def test_pas_de_pause_si_quota_large():
    lim = SharedRateLimiter(margin=5)
    lim.update({"X-Ratelimit-Remaining": "40", "X-Ratelimit-Reset": "30"})
    t0 = time.monotonic()
    lim.wait()
    assert time.monotonic() - t0 < 0.05


def test_pause_commune_quand_quota_presque_atteint(monkeypatch):
    lim = SharedRateLimiter(margin=5)
    lim.update({"X-Ratelimit-Remaining": "3", "X-Ratelimit-Reset": "0"})  # pause de 0 + 1 s
    t0 = time.monotonic()
    lim.wait()
    assert time.monotonic() - t0 >= 0.9


def test_la_pause_bloque_tous_les_threads():
    lim = SharedRateLimiter()
    lim.pause(0.5, "test")
    durations = []

    def worker():
        t0 = time.monotonic()
        lim.wait()
        durations.append(time.monotonic() - t0)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(durations) == 4 and min(durations) >= 0.4


def test_la_plus_longue_pause_l_emporte():
    lim = SharedRateLimiter()
    lim.pause(0.5, "longue")
    lim.pause(0.1, "courte")  # ne doit pas raccourcir la pause en cours
    t0 = time.monotonic()
    lim.wait()
    assert time.monotonic() - t0 >= 0.4


def test_cle_api_obligatoire():
    with pytest.raises(ValueError):
        OpenAQClient("")


def test_une_session_http_par_thread():
    client = OpenAQClient("cle-factice")
    sessions = []
    t = threading.Thread(target=lambda: sessions.append(client.session))
    t.start()
    t.join()
    assert sessions[0] is not client.session
