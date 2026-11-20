"""Tests des analyses P1 (fonctions pures sur des données synthétiques)."""
import numpy as np
import pandas as pd

from p1_meteo import analytics as a


def hourly(days=730, ville="paris", hot=None):
    """Série horaire synthétique : cycle annuel sinusoïdal ; `hot` = (jour_début, nb_jours) d'une vague à +12 °C."""
    h = pd.date_range("2022-01-01", periods=days * 24, freq="h", tz="UTC")
    doy = h.dayofyear.values
    t = 12 + 8 * np.sin(2 * np.pi * (doy - 110) / 365) + np.random.default_rng(0).normal(0, 1.0, len(h))
    if hot:
        start = pd.Timestamp("2022-01-01", tz="UTC") + pd.Timedelta(days=hot[0])
        t = np.where((h >= start) & (h < start + pd.Timedelta(days=hot[1])), t + 12, t)
    return pd.DataFrame({"ville_id": ville, "heure_utc": h, "temperature_c": t, "precipitation_mm": 0.1,
                         "vent_ms": 3.0, "humidite_pct": 70.0, "nebulosite_pct": 50.0, "pression_hpa": 1015.0,
                         "is_valid": True})


def test_daily_en_date_locale_et_jours_complets():
    d = a.daily(hourly(days=10))
    assert d["heures"].between(20, 25).all()
    assert d["pluie_mm"].round(1).eq(2.4).sum() >= 8          # 24 h × 0,1 mm


def test_vague_de_chaleur_detectee_comme_episode():
    ep = a.episodes(a.anomalies(a.daily(hourly(hot=(200, 6)))))
    chaud = ep[ep["type"] == "chaud"].iloc[0]
    assert chaud["jours"] >= 5 and chaud["anomalie_moy_c"] > 8
    assert chaud["debut"] >= pd.Timestamp("2022-07-19") and chaud["fin"] <= pd.Timestamp("2022-07-26")


def test_jours_extremes_seuils_relatifs_a_la_ville():
    day = pd.concat([a.daily(hourly(ville="lille")), a.daily(hourly(ville="marseille").assign(
        temperature_c=lambda d: d["temperature_c"] + 6))])
    ex = a.extreme_days(day)
    chaleur = ex[ex["type"] == "chaleur"]
    assert set(chaleur["ville_id"]) == {"lille", "marseille"}   # chaque ville a SES jours extrêmes
    assert chaleur.groupby("ville_id")["seuil"].first().diff().iloc[-1] > 4


def test_annees_d_etude_completes_seulement():
    y = a.study_years(a.daily(hourly(days=800)))
    assert y["periode"].tolist() == ["2022-23"] and y["jours"].iloc[0] >= 360


def test_climatologie_amplitude():
    c = a.climatology(a.daily(hourly()))
    assert c.loc["paris", "amplitude"] > 12 and c.loc["paris", 7] > c.loc["paris", 1]
