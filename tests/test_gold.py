"""Tests de la construction du gold (sans ClickHouse)."""
import numpy as np
import pandas as pd

from p4_croise import build_gold as g


def test_le_fichier_sql_declare_les_10_tables_gold():
    ddl = g.ddl_statements(g.SQL_FILE.read_text(encoding="utf-8"))
    assert set(ddl) == {"gold_ville_heure", "gold_ville_jour", "gold_station_velo", "gold_zone_velo",
                        "gold_velo_profil", "gold_station_pollution", "gold_pollution_profil", "gold_kpi_ville",
                        "gold_couverture", "gold_pipeline"}
    assert all("ENGINE = MergeTree" in s and "ORDER BY" in s for s in ddl.values())


def test_conversion_vers_les_types_clickhouse():
    df = pd.DataFrame({"a": [1.5, np.nan], "b": ["x", "y"], "c": [3.0, 4.0], "d": [1, 0],
                       "t": pd.to_datetime(["2025-01-01", "2025-01-02"], utc=True)})
    out = g.to_clickhouse(df, [("t", "DateTime('UTC')"), ("a", "Nullable(Float64)"), ("c", "UInt8"),
                               ("d", "Bool"), ("b", "String")])
    assert list(out.columns) == ["t", "a", "c", "d", "b"]          # ordre du schéma
    assert out["a"].tolist() == [1.5, None]                         # NaN → NULL
    assert out["c"].dtype == "int64" and out["d"].tolist() == [True, False]


def test_criticite_et_rang_des_stations():
    ind = pd.DataFrame({"ville_id": "lyon", "station_id": ["A", "B", "C"], "station_nom": ["a", "b", None],
                        "statut_station": ["exploitable", "exploitable", "inactive"],
                        "activite_jour": [50.0, 80.0, 0.2], "pct_vide_pointe": [40.0, 5.0, 100.0],
                        "pct_pleine_pointe": [0.0, 1.0, 0.0], "jours": 100.0, "rempl_matin": 0.4, "rempl_soir": 0.5})
    s = g.build_station_velo(ind).set_index("station_id")
    assert s["action"].to_dict() == {"A": "réapprovisionner", "B": "aucune", "C": "aucune"}  # C : hors service
    assert s["rang_activite_ville"].to_dict() == {"A": 2, "B": 1, "C": 0}
    assert pd.isna(s.loc["C", "score_criticite"]) and s.loc["C", "station_nom"] == ""


def test_zones_et_profil():
    z = g.build_zone_velo(pd.DataFrame({"cell_lat": [1, 2], "cell_lon": [3, 4], "pct_vide": [20.0, 5.0]}))
    assert z["zone_id"].tolist() == ["1_3", "2_4"] and z["sous_offre"].tolist() == [True, False]
    w = g.build_velo_profil(pd.DataFrame({"ville_id": "lyon", "wd": [0, 0], "h": [8, 3],
                                          "activite_station_h": [3.0, 1.0]}))
    assert w["indice"].tolist() == [1.5, 0.5]


def test_couverture_unifiee():
    meteo = pd.DataFrame({"ville_id": "lyon", "mois": ["2025-01"], "pct_presentes": [100.0], "pct_valides": [100.0]})
    pol = pd.DataFrame({"ville_id": "lyon", "station_id": [1, 2], "polluant": "no2", "mois": "2025-01",
                        "pct_presentes": [80.0, 60.0], "pct_valides": [70.0, 50.0]})
    velos = pd.DataFrame({"ville_id": "lyon", "mois": ["2025-01"], "pct_presentes": [90.0], "heures_valides": [450],
                          "heures_station_attendues": [500]})
    c = g.build_couverture(meteo, pol, velos).set_index("source")
    assert c.loc["pollution", "pct_presentes"] == 70.0 and c.loc["pollution", "detail"] == "no2"  # moyenne stations
    assert c.loc["vélos", "pct_valides"] == 90.0 and str(c.loc["météo", "mois"]) == "2025-01-01"
