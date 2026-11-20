"""P2 — Transform : bronze CityBikes (Parquet mensuels d'événements) → silver vélos (Parquet) dans Silo.

Entrée  : bronze/velos_historique/<ville_id>/<année>/<AAAAMM>-<réseau>-stats.parquet
          Une ligne = un CHANGEMENT d'état d'une station (emprunt / retour), à horodatage irrégulier.
Sorties : silver/velos/ville=<id>/year=<YYYY>/month=<MM>/velos_heure.parquet   1 ligne par station × heure UTC
          silver/velos/stations.parquet                                        dimension des stations
          silver/_couverture/velos.parquet                                     couverture par ville × mois
          silver/_manifests/velos_<horodatage>.json

Traitements :
  1. lecture fichier par fichier (un réseau × un mois) pour tenir en mémoire ; les mois d'une ville sont
     traités dans l'ordre chronologique et l'état final de chaque station est REPORTÉ au mois suivant ;
  2. validité de chaque événement (règles reprises de p2_velos/cleaning.py) : champs présents, compteurs ≥ 0,
     capacité observée > 0, coordonnées non nulles. Un événement invalide ne peut pas servir à
     reconstruire l'état : il est écarté du calcul mais COMPTÉ dans le manifeste ;
  3. reconstitution de l'état HORAIRE : dernier état connu dans l'heure ; une heure sans événement reprend
     l'état de l'heure précédente (`etat_observe = False`), sur 24 h au maximum — au-delà, la station est
     considérée hors service ou sans donnée et aucune ligne n'est produite (on ne comble pas les trous) ;
  4. indicateurs : `activite` = somme des |Δ vélos| entre événements consécutifs dans l'heure (proxy des
     emprunts + retours ; inclut aussi les rééquilibrages par camion), `taux_remplissage` = vélos / (vélos +
     bornes libres), `est_vide`, `est_pleine` ;
  5. qualité, par marquage (`quality_flag`) : station hors de la zone de la ville (bbox de ref_ville),
     capacité observée supérieure à la capacité déclarée (`extra.slots`) + 25 % de tolérance.

Responsable : P2 (Transform de l'ETL local).
"""
from __future__ import annotations

import io
import re
import time
from collections import defaultdict
from datetime import datetime, timezone

import pandas as pd
import pyarrow.parquet as pq

from common.config import load_ref_ville
from common.log import get_logger
from p2_velos.cleaning import _SLOTS_JSON_RE
from p2_velos.extract import BRONZE_PREFIX

SILVER_PREFIX = "silver/velos"
STATIONS_KEY = f"{SILVER_PREFIX}/stations.parquet"
COVERAGE_KEY = "silver/_couverture/velos.parquet"
MAX_CARRY_HOURS = 24          # report d'état maximal sur des heures sans événement
CAPACITY_TOLERANCE = 0.25     # même tolérance que p2_velos/cleaning.py (min 2 places)
READ_COLUMNS = ["id", "name", "latitude", "longitude", "bikes", "free", "timestamp", "extra"]
KEY_RE = re.compile(r"/(?P<ym>\d{6})-(?P<network>.+)-stats\.parquet$")

log = get_logger("p2.transform")


# ---------------------------------------------------------------- fonctions pures (testées)

def read_events(raw: bytes) -> pd.DataFrame:
    """Parquet CityBikes -> événements typés (timestamp UTC, capacité déclarée extraite de `extra`)."""
    available = set(pq.ParquetFile(io.BytesIO(raw)).schema_arrow.names)
    df = pq.read_table(io.BytesIO(raw), columns=[c for c in READ_COLUMNS if c in available]).to_pandas()
    out = pd.DataFrame({
        "station_id": df["id"].astype(str).str.strip(),
        "station_nom": df["name"].astype(str).str.strip(),
        "latitude": pd.to_numeric(df["latitude"], errors="coerce"),
        "longitude": pd.to_numeric(df["longitude"], errors="coerce"),
        "velos": pd.to_numeric(df["bikes"], errors="coerce"),
        "bornes": pd.to_numeric(df["free"], errors="coerce"),
        "ts": pd.to_datetime(df["timestamp"], errors="coerce", utc=True),   # horodatage CityBikes = UTC
    })
    if "extra" in df:
        extra = df["extra"].map(lambda b: b.decode("utf-8", "replace") if isinstance(b, bytes) else str(b))
        out["capacite_declaree"] = pd.to_numeric(extra.str.extract(_SLOTS_JSON_RE, expand=False), errors="coerce")
    else:
        out["capacite_declaree"] = float("nan")
    return out


def validate_events(ev: pd.DataFrame, bbox: dict | None) -> pd.DataFrame:
    """Ajoute `utilisable` (l'événement peut servir à l'état) et les marqueurs qualité de l'événement."""
    out = ev.copy()
    out["utilisable"] = (
        (out["station_id"] != "") & out["ts"].notna()
        & out["velos"].notna() & out["bornes"].notna()
        & (out["velos"] >= 0) & (out["bornes"] >= 0) & ((out["velos"] + out["bornes"]) > 0)
        & out["latitude"].notna() & out["longitude"].notna()
        & ~((out["latitude"] == 0) & (out["longitude"] == 0))
    )
    if bbox:
        out["hors_zone"] = ~(out["latitude"].between(bbox["lat_min"], bbox["lat_max"])
                             & out["longitude"].between(bbox["lon_min"], bbox["lon_max"]))
    else:
        out["hors_zone"] = False
    decl = out["capacite_declaree"]
    tol = (decl * CAPACITY_TOLERANCE).fillna(0).astype(int).clip(lower=2)
    out["capacite_incoherente"] = decl.notna() & (decl > 0) & ((out["velos"] + out["bornes"]) > decl + tol)
    return out


def hourly_states(ev: pd.DataFrame, month_start: pd.Timestamp, month_end: pd.Timestamp,
                  prev: pd.DataFrame | None = None, max_carry: int = MAX_CARRY_HOURS):
    """Événements valides d'un mois -> état horaire par station. Renvoie (lignes horaires, état final).

    `prev` : dernier événement de chaque station au mois précédent (report d'état et premier Δ du mois).
    """
    ok = ev[ev["utilisable"]].sort_values(["station_id", "ts"])
    cols = ["station_id", "ts", "velos", "bornes", "capacite_declaree", "hors_zone", "capacite_incoherente"]
    seq = ok[cols].assign(_prev=False)
    if prev is not None and len(prev):
        seq = pd.concat([prev[cols].assign(_prev=True), seq], ignore_index=True).sort_values(["station_id", "ts"])
    seq["delta"] = seq.groupby("station_id")["velos"].diff().abs()
    last_state = seq.groupby("station_id").tail(1)[cols].reset_index(drop=True)

    cur = seq[~seq["_prev"]].copy()
    cur["heure_utc"] = cur["ts"].dt.floor("h")
    agg = cur.groupby(["station_id", "heure_utc"]).agg(
        velos_dispo=("velos", "last"), bornes_libres=("bornes", "last"),
        capacite_declaree=("capacite_declaree", "last"),
        activite=("delta", "sum"), nb_evenements=("ts", "size"),
        hors_zone=("hors_zone", "max"), capacite_incoherente=("capacite_incoherente", "max"),
    )

    # grille complète station × heure du mois, puis report d'état limité à `max_carry` heures
    hours = pd.date_range(month_start, month_end, freq="h", tz="UTC")
    stations = sorted(set(agg.index.get_level_values(0)) | (set(prev["station_id"]) if prev is not None else set()))
    if not stations:
        return pd.DataFrame(), last_state
    grid = pd.MultiIndex.from_product([stations, hours], names=["station_id", "heure_utc"])
    full = agg.reindex(grid)
    if prev is not None and len(prev):  # amorce : état de fin du mois précédent, juste avant la 1re heure
        seed = prev.set_index("station_id")[["velos", "bornes", "capacite_declaree", "hors_zone",
                                            "capacite_incoherente"]]
        seed = seed.rename(columns={"velos": "velos_dispo", "bornes": "bornes_libres"})
        seed.index = pd.MultiIndex.from_arrays([seed.index, [month_start - pd.Timedelta(hours=1)] * len(seed)],
                                               names=["station_id", "heure_utc"])
        full = pd.concat([seed, full]).sort_index()
    carried = ["velos_dispo", "bornes_libres", "capacite_declaree", "hors_zone", "capacite_incoherente"]
    full[carried] = full.groupby(level="station_id")[carried].ffill(limit=max_carry)
    full = full[full.index.get_level_values("heure_utc") >= month_start]
    full = full[full["velos_dispo"].notna()].reset_index()

    full["etat_observe"] = full["nb_evenements"].notna()
    full["nb_evenements"] = full["nb_evenements"].fillna(0).astype("int32")
    full["activite"] = full["activite"].fillna(0).astype("int32")
    full["velos_dispo"] = full["velos_dispo"].astype("int32")
    full["bornes_libres"] = full["bornes_libres"].astype("int32")
    total = full["velos_dispo"] + full["bornes_libres"]
    full["capacite"] = full["capacite_declaree"].where(full["capacite_declaree"] > 0, total).astype("int32")
    full["taux_remplissage"] = (full["velos_dispo"] / total).round(4).astype("float32")
    full["est_vide"] = full["velos_dispo"] == 0
    full["est_pleine"] = full["bornes_libres"] == 0
    flags = pd.Series("", index=full.index, dtype=object)
    for col, label in (("hors_zone", "hors_zone"), ("capacite_incoherente", "capacite_incoherente")):
        m = full[col].eq(True)  # NaN (pas d'information) -> False, sans conversion implicite
        flags = flags.mask(m, flags.where(flags == "", flags + ";") + label)
    full["quality_flag"] = flags
    full["is_valid"] = flags == ""
    return full.drop(columns=["capacite_declaree", "hors_zone", "capacite_incoherente"]), last_state


def month_bounds(ym: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(f"{ym[:4]}-{ym[4:]}-01", tz="UTC")
    return start, start + pd.offsets.MonthBegin(1) - pd.Timedelta(hours=1)


def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    return buf.getvalue()


# ---------------------------------------------------------------- transformation

def transform(storage, villes=None, only=None) -> dict:
    """Transforme le bronze vélos ville par ville, mois par mois (ordre chronologique)."""
    villes = villes or load_ref_ville()
    if only:
        villes = [v for v in villes if v["ville_id"] in set(only)]
    t0 = time.monotonic()
    keys = [k for k in storage.list(f"{BRONZE_PREFIX}/") if k.endswith(".parquet")]
    by_city = defaultdict(list)
    for k in keys:
        m = KEY_RE.search(k)
        if m:
            by_city[k.split("/")[2]].append((m["ym"], m["network"], k))

    stations_rows, coverage_rows, per_city = [], [], {}
    for v in villes:
        vid, files = v["ville_id"], sorted(by_city.get(v["ville_id"], []))
        prev, n_ev, n_bad, n_rows = None, 0, 0, 0
        for ym, network, key in files:
            ev = validate_events(read_events(storage.get_bytes(key)), v.get("bbox"))
            n_ev += len(ev)
            n_bad += int((~ev["utilisable"]).sum())
            m_start, m_end = month_bounds(ym)
            rows, prev = hourly_states(ev, m_start, m_end, prev)
            if rows.empty:
                continue
            rows.insert(0, "network_id", network)
            rows.insert(0, "ville_id", vid)
            rows["source"] = "citybikes-dump"
            out_key = f"{SILVER_PREFIX}/ville={vid}/year={ym[:4]}/month={ym[4:]}/velos_heure.parquet"
            storage.put_bytes(out_key, to_parquet_bytes(rows), content_type="application/vnd.apache.parquet")
            n_rows += len(rows)
            n_stations = rows["station_id"].nunique()
            coverage_rows.append({
                "ville_id": vid, "mois": f"{ym[:4]}-{ym[4:]}", "stations": n_stations,
                "heures_station_attendues": n_stations * len(pd.date_range(m_start, m_end, freq="h")),
                "heures_station_presentes": len(rows),
                "heures_station_observees": int(rows["etat_observe"].sum()),
                "heures_valides": int(rows["is_valid"].sum()),
            })
            ok = ev[ev["utilisable"]]
            stations_rows.append(ok.groupby("station_id").agg(
                station_nom=("station_nom", "last"), latitude=("latitude", "median"),
                longitude=("longitude", "median"), capacite_declaree=("capacite_declaree", "median"),
                premiere_vue=("ts", "min"), derniere_vue=("ts", "max")).reset_index().assign(
                ville_id=vid, network_id=network))
            log.info("%s %s : %d événements (%d inutilisables) → %d lignes station×heure, %d stations",
                     vid, ym, len(ev), int((~ev["utilisable"]).sum()), len(rows), n_stations)
        per_city[vid] = {"fichiers": len(files), "evenements": n_ev, "evenements_inutilisables": n_bad,
                         "lignes_silver": n_rows, "mois": [f[0] for f in files]}

    st = pd.concat(stations_rows, ignore_index=True)
    st = (st.sort_values("derniere_vue").groupby(["ville_id", "station_id"])
            .agg(network_id=("network_id", "last"), station_nom=("station_nom", "last"),
                 latitude=("latitude", "median"), longitude=("longitude", "median"),
                 capacite_declaree=("capacite_declaree", "median"),
                 premiere_vue=("premiere_vue", "min"), derniere_vue=("derniere_vue", "max")).reset_index())
    storage.put_bytes(STATIONS_KEY, to_parquet_bytes(st), content_type="application/vnd.apache.parquet")
    cov = pd.DataFrame(coverage_rows)
    cov["pct_presentes"] = (100 * cov["heures_station_presentes"] / cov["heures_station_attendues"]).round(1)
    cov["pct_observees"] = (100 * cov["heures_station_observees"] / cov["heures_station_attendues"]).round(1)
    storage.put_bytes(COVERAGE_KEY, to_parquet_bytes(cov), content_type="application/vnd.apache.parquet")

    stats = {
        "source": "bronze/velos_historique (CityBikes)",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "report_etat_max_heures": MAX_CARRY_HOURS,
        "evenements": sum(c["evenements"] for c in per_city.values()),
        "evenements_inutilisables": sum(c["evenements_inutilisables"] for c in per_city.values()),
        "lignes_silver": sum(c["lignes_silver"] for c in per_city.values()),
        "stations": len(st),
        "villes": per_city,
        "duration_s": round(time.monotonic() - t0, 1),
    }
    storage.put_json(f"silver/_manifests/velos_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json", stats)
    log.info("Silver vélos : %d événements (%d inutilisables) → %d lignes station×heure, %d stations, %ss",
             stats["evenements"], stats["evenements_inutilisables"], stats["lignes_silver"], stats["stations"],
             stats["duration_s"])
    return stats


if __name__ == "__main__":
    import argparse

    from common.storage import get_storage

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--local", action="store_true", help="lire/écrire dans data/ au lieu de Silo")
    parser.add_argument("--villes", nargs="*", help="limiter à ces villes (tests)")
    args = parser.parse_args()
    transform(get_storage(use_local=args.local), only=args.villes)
