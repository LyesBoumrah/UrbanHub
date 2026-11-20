"""Tests des analyses P2 (agrégation map → reduce et indicateurs par station)."""
import numpy as np
import pandas as pd

from p2_velos import analytics as a


def silver(station, start, hours, activite, vide=False, pleine=False, ville="lyon"):
    h = pd.date_range(start, periods=hours, freq="h", tz="UTC")
    return pd.DataFrame({"ville_id": ville, "station_id": station, "heure_utc": h, "activite": activite,
                         "taux_remplissage": 0.5, "est_vide": vide, "est_pleine": pleine, "etat_observe": True,
                         "is_valid": True})


def test_map_reduce_egal_au_calcul_direct():
    f1 = silver("A", "2025-01-06", 24 * 20, 2)                         # deux « fichiers » (mois)
    f2 = silver("A", "2025-02-03", 24 * 20, 4)
    agg = a.combine([a.aggregate_file(f1), a.aggregate_file(f2)])
    st = agg["station"].loc[("lyon", "A")]
    assert st["heures"] == 24 * 40 and st["activite"] == 24 * 20 * 2 + 24 * 20 * 4


def test_heures_de_pointe_en_heure_locale_jours_ouvres():
    d = a.prepare(silver("A", "2025-01-06 06:00", 1, 1))               # lundi 06:00 UTC = 07:00 Paris
    assert d["h"].iloc[0] == 7 and d["pointe"].iloc[0] and d["matin"].iloc[0]
    d = a.prepare(silver("A", "2025-01-11 06:00", 1, 1))               # samedi : pas une heure de pointe
    assert not d["pointe"].iloc[0]


def indicators(df):
    stations = pd.DataFrame({"ville_id": "lyon", "station_id": ["A", "B", "C", "D"],
                             "station_nom": ["Opéra", "", "Morte", "Fantôme"], "latitude": 45.76,
                             "longitude": 4.84, "capacite_declaree": [20, 20, 20, 10]})
    villes = [{"ville_id": "lyon", "lat": 45.764, "lon": 4.8357}]
    return a.station_indicators(a.combine([a.aggregate_file(df)]), stations, villes)


def test_statuts_des_stations():
    df = pd.concat([silver("A", "2025-01-06", 24 * 40, 3), silver("B", "2025-01-06", 24 * 40, 3),
                    silver("C", "2025-01-06", 24 * 40, 0, vide=True), silver("D", "2025-01-06", 24 * 40, 100)])
    ind = indicators(df).set_index("station_id")
    assert ind["statut_station"].to_dict() == {"A": "exploitable", "B": "technique", "C": "inactive",
                                               "D": "aberrante"}       # D : 2 400 mouvements/jour pour 10 places
    assert ind.loc["A", "activite_jour"] == 72 and ind.loc["C", "pct_vide"] == 100


def test_criticite_heures_de_pointe():
    df = silver("A", "2025-01-06", 24 * 40, 3)
    local_h = df["heure_utc"].dt.tz_convert("Europe/Paris").dt.hour
    df["est_vide"] = local_h.isin([7, 8, 9]).values                   # vide chaque matin de pointe
    ind = indicators(df)
    crit = a.critical_stations(ind).iloc[0]
    assert crit["action"] == "réapprovisionner" and 40 < crit["pct_vide_pointe"] < 60   # matins ouvrés / pointe


def test_zones_de_500_m():
    ind = pd.DataFrame({"ville_id": "lyon", "station_id": ["A", "B", "C"], "latitude": [45.7600, 45.7601, 45.80],
                        "longitude": [4.8400, 4.8401, 4.84], "heures": 100, "vide": [10, 30, 0],
                        "activite_jour": 5.0})
    cells = a.grid_cells(ind)
    assert len(cells) == 2 and np.isclose(cells["pct_vide"].max(), 20.0)  # A et B dans la même zone
