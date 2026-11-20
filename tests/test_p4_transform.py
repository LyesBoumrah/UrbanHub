"""Tests du croisement P4 (silver météo × pollution × vélos → ville × heure)."""
from datetime import date

import pandas as pd

from p4_croise import transform as t

H0, H1 = pd.Timestamp("2025-01-01 07:00", tz="UTC"), pd.Timestamp("2025-01-01 08:00", tz="UTC")


def meteo():
    return pd.DataFrame({"ville_id": ["paris", "paris"], "heure_utc": [H0, H1], "temperature_c": [3.0, 99.0],
                         **{c: [1.0, 1.0] for c in t.METEO_COLS if c != "temperature_c"},
                         "is_valid": [True, False]})                     # la 2e heure est invalide


def pollution():
    return pd.DataFrame({"ville_id": ["paris"] * 4, "heure_utc": [H0, H0, H0, H0],
                         "polluant": ["no2", "no2", "no2", "pm25"], "valeur": [20.0, 40.0, -5.0, 8.0],
                         "is_valid": [True, True, False, True]})


def velos():
    return pd.DataFrame({"ville_id": ["paris", "paris"], "heure_utc": [H0, H0], "velos_dispo": [5, 15],
                         "activite": [2, 6], "taux_remplissage": [0.25, 0.75], "est_vide": [False, False],
                         "est_pleine": [False, True], "etat_observe": [True, False], "is_valid": [True, True]})


def test_agregation_pollution_moyenne_des_stations_valides():
    a = t.aggregate_pollution(pollution()).iloc[0]
    assert a["no2"] == 30.0 and a["nb_stations_no2"] == 2          # -5 (invalide) exclu
    assert a["pm25"] == 8.0 and pd.isna(a["o3"]) and a["nb_stations_o3"] == 0


def test_agregation_velos_reseau():
    a = t.aggregate_velos(velos()).iloc[0]
    assert (a["velo_stations"], a["velos_dispo_total"], a["activite_totale"]) == (2, 20, 8)
    assert a["activite_par_station"] == 4.0 and a["remplissage_moyen"] == 0.5
    assert a["part_stations_pleines"] == 0.5 and a["part_heures_observees"] == 0.5


def test_grille_complete_et_rien_n_est_comble():
    grid = t.city_hour_grid(["paris", "lyon"], date(2025, 1, 1), date(2025, 1, 1))
    df = t.build(grid, meteo(), t.aggregate_pollution(pollution()), t.aggregate_velos(velos()))
    assert len(df) == 2 * 24
    paris = df[df["ville_id"] == "paris"].set_index("heure_utc")
    assert paris.loc[H0, ["a_meteo", "a_pollution", "a_velos"]].tolist() == [True, True, True]
    assert not paris.loc[H1, "a_meteo"] and pd.isna(paris.loc[H1, "temperature_c"])  # invalide → vide
    assert not df[df["ville_id"] == "lyon"][["a_meteo", "a_pollution", "a_velos"]].any().any()


def test_calendrier_en_heure_locale():
    df = t.add_calendar(pd.DataFrame({"heure_utc": [H0, pd.Timestamp("2025-07-05 06:00", tz="UTC")]}))
    assert df["heure_locale"].tolist() == [8, 8]                  # UTC+1 en hiver, UTC+2 en été
    assert df["est_weekend"].tolist() == [False, True] and df["saison"].tolist() == ["hiver", "ete"]
