"""Tests de l'analyse croisée P4 (indices relatifs, favorabilité, effets)."""
import numpy as np
import pandas as pd

from p4_croise import analytics as a


def test_indice_relatif_annule_le_cycle_horaire():
    # NO2 deux fois plus élevé à 8 h qu'à 3 h, mais chaque heure « normale » vaut 1
    df = pd.DataFrame({"ville_id": "paris", "mois": 1, "heure_locale": [8] * 40 + [3] * 40,
                       "no2": [40.0] * 40 + [20.0] * 40})
    i = a.relative_index(df, "no2")
    assert np.allclose(i, 1.0)


def test_indice_vide_si_normale_trop_fragile():
    df = pd.DataFrame({"ville_id": "paris", "mois": 1, "heure_locale": 8, "no2": [30.0] * 5})
    assert a.relative_index(df, "no2").isna().all()


def test_score_de_favorabilite():
    df = pd.DataFrame({"temperature_c": [18, 18, 35, 2], "precipitation_mm": [0, 1.2, 0, 0.5],
                       "vent_ms": [2, 2, 2, 9]})
    assert a.favorability(df).tolist() == [3, 2, 2, 0]


def test_effet_de_la_pluie_par_classes():
    v = pd.DataFrame({"heure_locale": 9, "precipitation_mm": [0.0] * 50 + [3.0] * 50,
                      "i_velo": [1.0] * 50 + [0.6] * 50})
    r = a.rain_effect(v).set_index("precipitation_mm")["moyenne"]
    assert r["sec"] == 1.0 and r["forte pluie (> 2 mm)"] == 0.6


def test_jours_combines_marquage():
    df = pd.DataFrame({"ville_id": "lyon", "date_locale": "2024-12-26", "a_velos": True, "pm25": [30.0] * 3,
                       "o3": 10.0, "temperature_c": 2.0, "precipitation_mm": 0.0, "vent_ms": 0.5})
    v = pd.DataFrame({"ville_id": "lyon", "date_locale": "2024-12-26", "i_velo": [0.3] * 3})
    day = a.combined_days(df, v).iloc[0]
    assert day["pollution_elevee"] and day["meteo_defavorable"] and day["velo_atypique"]
