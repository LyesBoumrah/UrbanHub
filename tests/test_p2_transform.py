"""Tests du Transform P2 (événements CityBikes → état horaire par station)."""
import io

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from common.storage import LocalBronzeStorage
from p2_velos import transform as t

BBOX = {"lat_min": 48.0, "lat_max": 48.22, "lon_min": -1.85, "lon_max": -1.52}
M_START, M_END = t.month_bounds("202411")


def ev(rows):
    """rows : (station, 'JJ HH:MM', vélos, bornes[, capacité déclarée, lat])."""
    out = []
    for r in rows:
        sid, when, b, f = r[:4]
        out.append({"station_id": sid, "station_nom": f"S{sid}", "latitude": r[5] if len(r) > 5 else 48.1,
                    "longitude": -1.66, "velos": b, "bornes": f, "ts": pd.Timestamp(f"2024-11-{when}", tz="UTC"),
                    "capacite_declaree": r[4] if len(r) > 4 else 20})
    return t.validate_events(pd.DataFrame(out), BBOX)


def test_etat_horaire_activite_et_report():
    e = ev([("A", "01 10:05", 5, 15), ("A", "01 10:20", 3, 17), ("A", "01 10:50", 6, 14),  # 3 événements à 10 h
            ("A", "01 13:10", 6, 14)])
    rows, last = t.hourly_states(e, M_START, M_END)
    r = rows.set_index("heure_utc")
    h10, h11 = pd.Timestamp("2024-11-01 10:00", tz="UTC"), pd.Timestamp("2024-11-01 11:00", tz="UTC")
    assert r.loc[h10, "velos_dispo"] == 6 and r.loc[h10, "nb_evenements"] == 3
    assert r.loc[h10, "activite"] == 2 + 3            # |5→3| + |3→6| (le 1er événement n'a pas de Δ)
    assert r.loc[h11, "velos_dispo"] == 6 and not r.loc[h11, "etat_observe"] and r.loc[h11, "activite"] == 0
    assert r.index.min() == h10                        # pas de ligne avant la 1re observation
    assert r.loc[h10, "taux_remplissage"] == 0.3 and r.loc[h10, "capacite"] == 20
    assert last.iloc[0]["velos"] == 6


def test_report_limite_a_24h_on_ne_comble_pas_les_trous():
    rows, _ = t.hourly_states(ev([("A", "01 00:30", 4, 16)]), M_START, M_END)
    assert len(rows) == 1 + 24                         # l'heure observée + 24 h reportées, puis plus rien


def test_etat_reporte_au_mois_suivant():
    _, last_oct = t.hourly_states(ev([("A", "01 08:00", 7, 13)]), M_START, M_END)
    dec_start, dec_end = t.month_bounds("202412")
    first = ev([("A", "01 08:00", 7, 13)]).assign(ts=pd.Timestamp("2024-12-01 02:30", tz="UTC"), velos=4)
    prev = last_oct.assign(ts=pd.Timestamp("2024-11-30 23:10", tz="UTC"))
    rows, _ = t.hourly_states(first, dec_start, dec_end, prev=prev)
    r = rows.set_index("heure_utc")
    assert r.loc[dec_start, "velos_dispo"] == 7 and not r.loc[dec_start, "etat_observe"]  # état reporté
    assert r.loc[pd.Timestamp("2024-12-01 02:00", tz="UTC"), "activite"] == 3            # Δ avec le mois précédent


def test_evenements_invalides_ecartes_et_marquages():
    e = ev([("A", "01 10:00", -1, 15), ("B", "01 10:00", 0, 0),           # compteur négatif, capacité nulle
            ("C", "01 10:00", 30, 5, 20), ("D", "01 10:00", 2, 18, 20, 43.3)])  # capacité incohérente, hors zone
    assert e["utilisable"].tolist() == [False, False, True, True]
    rows, _ = t.hourly_states(e, M_START, M_END)
    flags = rows[rows["heure_utc"] == pd.Timestamp("2024-11-01 10:00", tz="UTC")].set_index("station_id")
    assert flags.loc["C", "quality_flag"] == "capacite_incoherente" and flags.loc["D", "quality_flag"] == "hors_zone"
    assert set(rows["station_id"]) == {"C", "D"}


def test_lecture_dump_parquet_et_extra_slots(tmp_path):
    table = pa.table({"tag": ["x"], "id": ["s1"], "nuid": ["1"], "name": ["Gare"], "latitude": [48.1],
                      "longitude": [-1.6], "bikes": pa.array([3], pa.int32()), "free": pa.array([16], pa.int32()),
                      "extra": [b'{"slots":19,"online":true}'],
                      "timestamp": pa.array([pd.Timestamp("2024-11-01 15:13:17")], pa.timestamp("us"))})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    e = t.read_events(buf.getvalue())
    assert e.loc[0, "capacite_declaree"] == 19 and str(e["ts"].dt.tz) == "UTC"

    st = LocalBronzeStorage(tmp_path)
    st.put_bytes("bronze/velos_historique/rennes/2024/202411-le-velo-star-stats.parquet", buf.getvalue())
    stats = t.transform(st, villes=[{"ville_id": "rennes", "bbox": BBOX}])
    assert stats["villes"]["rennes"]["lignes_silver"] == 25 and stats["stations"] == 1
    assert st.exists("silver/velos/ville=rennes/year=2024/month=11/velos_heure.parquet")
