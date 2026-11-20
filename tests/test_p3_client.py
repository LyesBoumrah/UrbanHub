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
    t_pause = time.monotonic()
    lim.pause(0.5, "test")
    released_at = []

    def worker():
        lim.wait()
        released_at.append(time.monotonic() - t_pause)  # mesuré depuis la pause, pas depuis le thread

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(released_at) == 4 and min(released_at) >= 0.45  # aucun thread libéré avant la fin


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
