"""Tests des analyses P3 (pollution) sur des données synthétiques."""
import pandas as pd

from p3_pollution import analytics as a


def silver(polluant, values, start="2025-01-06 00:00", station=1, ville="paris", step="h"):
    h = pd.date_range(start, periods=len(values), freq=step, tz="UTC")
    return pd.DataFrame({"ville_id": ville, "station_id": station, "polluant": polluant, "heure_utc": h,
                         "valeur": values, "is_valid": True})


def test_jour_incomplet_ecarte():
    d = a.valid(pd.concat([silver("no2", [30.0] * 24), silver("no2", [30.0] * 10, start="2025-01-08 00:00")]))
    day = a.daily(d)
    assert len(day) == 1                                         # le jour à 10 h de mesure est écarté


def test_o3_max_8h_glissantes():
    vals = [50.0] * 10 + [130.0] * 8 + [50.0] * 6                # 8 h à 130 dans la journée
    day = a.daily(a.valid(silver("o3", vals, start="2025-07-01 22:00")))   # 22:00 UTC = minuit local
    assert day["o3_8h"].max() == 130.0


def test_ratio_oms_et_polluant_dominant():
    st = pd.DataFrame({"ville_id": ["paris"] * 3, "station_id": 1, "polluant": ["pm25", "pm10", "no2"],
                       "moyenne": [10.0, 15.0, 30.0]})
    r = a.who_ratio(st)
    assert r.loc["paris"].to_dict() == {"pm25": 2.0, "pm10": 1.0, "no2": 3.0}
    assert a.dominant_pollutant(r)["paris"] == "NO2"


def test_depassement_journalier_et_episode_multi_villes():
    rows = []
    for v in ("paris", "lyon", "lille", "nantes"):
        for i, val in enumerate([10.0, 30.0, 30.0, 10.0]):          # 2 jours au-dessus de 15 (PM2.5)
            rows.append(silver("pm25", [val] * 24, start=f"2025-01-0{6 + i} 00:00", ville=v, station=hash(v) % 99))
    cday = a.city_daily(a.daily(a.valid(pd.concat(rows))))
    ex = a.exceedance_days(cday)
    assert (ex["jours_depassement"] == 2).all()
    ep = a.episodes(cday, "pm25", min_villes=4)
    assert len(ep) == 1 and ep.iloc[0]["jours"] == 2 and ep.iloc[0]["villes_max"] == 4


def test_type_station_par_le_nom():
    t = a.station_type(pd.Series(["Bld peripherique Est", "PARIS 18eme", "Rouen Proxi Quai de Paris"]))
    assert list(t) == ["trafic", "fond urbain", "trafic"]
