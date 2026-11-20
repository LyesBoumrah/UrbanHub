"""P4 — Transform : croisement des 3 silver → silver/croise (Parquet), 1 ligne par ville × heure UTC.

Entrées : silver/meteo/…          (ville × heure)
          silver/pollution/…      (station × polluant × heure)  → moyenne des stations de la ville
          silver/velos/…          (station × heure)             → agrégats du réseau de la ville
Sortie  : silver/croise/year=<YYYY>/ville_heure.parquet
          silver/_manifests/croise_<horodatage>.json

Principes :
  * grille COMPLÈTE ville × heure sur la période d'étude ; chaque source y est jointe sur (ville_id, heure_utc) ;
    une heure sans donnée pour une source reste vide (NaN) pour cette source — on ne comble rien ;
  * seules les lignes `is_valid` des silver sont agrégées ;
  * pollution : moyenne simple des stations de la ville (chaque station pèse autant) + nombre de stations ;
  * vélos : agrégats du réseau (stations présentes, vélos disponibles, activité totale et par station,
    remplissage moyen, part de stations vides / pleines, part d'heures réellement observées) ;
  * indicateurs de présence `a_meteo`, `a_pollution`, `a_velos`, et variables calendaires en heure LOCALE
    (Europe/Paris) : les comportements (trafic, trajets) suivent l'heure locale, pas l'heure UTC.

Responsable : commun (Transform de l'ETL local).
"""
from __future__ import annotations

import io
import time
from datetime import datetime, timezone

import pandas as pd

from common.config import load_villes
from common.log import get_logger
from common.period import study_period

SILVER_PREFIX = "silver/croise"
POLLUTANTS = ("pm25", "pm10", "no2", "o3", "co")
METEO_COLS = ["temperature_c", "humidite_pct", "precipitation_mm", "pression_hpa", "nebulosite_pct",
              "vent_ms", "vent_direction_deg"]
VELO_COLS = ["ville_id", "heure_utc", "velos_dispo", "activite", "taux_remplissage", "est_vide", "est_pleine",
             "etat_observe", "is_valid"]

log = get_logger("p4.transform")


# ---------------------------------------------------------------- fonctions pures (testées)

def city_hour_grid(villes, start, end) -> pd.DataFrame:
    hours = pd.date_range(pd.Timestamp(start, tz="UTC"), pd.Timestamp(end, tz="UTC") + pd.Timedelta(hours=23),
                          freq="h")
    idx = pd.MultiIndex.from_product([sorted(villes), hours], names=["ville_id", "heure_utc"])
    return pd.DataFrame(index=idx).reset_index()


def aggregate_pollution(df: pd.DataFrame) -> pd.DataFrame:
    """station × polluant × heure → ville × heure : moyenne des stations + nb de stations par polluant."""
    v = df[df["is_valid"]]
    g = v.groupby(["ville_id", "heure_utc", "polluant"])["valeur"].agg(["mean", "size"]).unstack("polluant")
    out = pd.DataFrame(index=g.index)
    for p in POLLUTANTS:
        out[p] = g[("mean", p)].round(2) if ("mean", p) in g else float("nan")
        out[f"nb_stations_{p}"] = g[("size", p)].fillna(0).astype("int16") if ("size", p) in g else 0
    return out.reset_index()


def aggregate_velos(df: pd.DataFrame) -> pd.DataFrame:
    """station × heure → ville × heure (réseau de la ville)."""
    v = df[df["is_valid"]]
    g = v.groupby(["ville_id", "heure_utc"]).agg(
        velo_stations=("velos_dispo", "size"), velos_dispo_total=("velos_dispo", "sum"),
        activite_totale=("activite", "sum"), activite_par_station=("activite", "mean"),
        remplissage_moyen=("taux_remplissage", "mean"), part_stations_vides=("est_vide", "mean"),
        part_stations_pleines=("est_pleine", "mean"), part_heures_observees=("etat_observe", "mean"))
    for c in ("activite_par_station", "remplissage_moyen", "part_stations_vides", "part_stations_pleines",
              "part_heures_observees"):
        g[c] = g[c].astype("float64").round(4)
    return g.reset_index()


def add_calendar(df: pd.DataFrame) -> pd.DataFrame:
    local = df["heure_utc"].dt.tz_convert("Europe/Paris")
    out = df.copy()
    out["heure_locale"] = local.dt.hour.astype("int8")
    out["jour_semaine"] = local.dt.dayofweek.astype("int8")      # 0 = lundi
    out["est_weekend"] = out["jour_semaine"] >= 5
    out["date_locale"] = local.dt.date.astype(str)
    out["mois"] = local.dt.month.astype("int8")
    out["saison"] = out["mois"].map({12: "hiver", 1: "hiver", 2: "hiver", 3: "printemps", 4: "printemps",
                                     5: "printemps", 6: "ete", 7: "ete", 8: "ete", 9: "automne",
                                     10: "automne", 11: "automne"})
    return out


def build(grid, meteo, pollution_agg, velos_agg) -> pd.DataFrame:
    keys = ["ville_id", "heure_utc"]
    m = meteo[meteo["is_valid"]][keys + METEO_COLS]
    out = grid.merge(m, on=keys, how="left").merge(pollution_agg, on=keys, how="left") \
              .merge(velos_agg, on=keys, how="left")
    out["a_meteo"] = out["temperature_c"].notna()
    out["a_pollution"] = out[list(POLLUTANTS)].notna().any(axis=1)
    out["a_velos"] = out["velo_stations"].notna()
    for p in POLLUTANTS:
        out[f"nb_stations_{p}"] = out[f"nb_stations_{p}"].fillna(0).astype("int16")
    return add_calendar(out)


def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    return buf.getvalue()


# ---------------------------------------------------------------- lecture du silver

def _read(storage, prefix, columns=None) -> pd.DataFrame:
    keys = [k for k in storage.list(prefix) if k.endswith(".parquet") and "/year=" in k]
    return pd.concat([pd.read_parquet(io.BytesIO(storage.get_bytes(k)), columns=columns) for k in keys],
                     ignore_index=True)


def read_velos_aggregated(storage) -> pd.DataFrame:
    """48 M lignes station × heure : agrégées fichier par fichier pour tenir en mémoire."""
    keys = [k for k in storage.list("silver/velos/") if k.endswith("velos_heure.parquet")]
    parts = [aggregate_velos(pd.read_parquet(io.BytesIO(storage.get_bytes(k)), columns=VELO_COLS)) for k in keys]
    return pd.concat(parts, ignore_index=True)


# ---------------------------------------------------------------- transformation

def transform(storage, villes=None, start=None, end=None) -> dict:
    if start is None or end is None:
        start, end = study_period()
    villes = villes or [v["ville_id"] for v in load_villes()]
    t0 = time.monotonic()
    meteo = _read(storage, "silver/meteo/")
    pollution = aggregate_pollution(_read(storage, "silver/pollution/",
                                          columns=["ville_id", "heure_utc", "polluant", "valeur", "is_valid"]))
    velos = read_velos_aggregated(storage)
    df = build(city_hour_grid(villes, start, end), meteo, pollution, velos)

    written = {}
    for year, part in df.groupby(df["heure_utc"].dt.year):
        key = f"{SILVER_PREFIX}/year={year}/ville_heure.parquet"
        storage.put_bytes(key, to_parquet_bytes(part), content_type="application/vnd.apache.parquet")
        written[key] = len(part)

    three = df["a_meteo"] & df["a_pollution"] & df["a_velos"]
    with_velos = df.loc[df["a_velos"], "heure_utc"]
    stats = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "period": {"start": str(start), "end": str(end)},
        "lignes": len(df),
        "colonnes": list(df.columns),
        "pct_heures_avec_meteo": round(100 * df["a_meteo"].mean(), 1),
        "pct_heures_avec_pollution": round(100 * df["a_pollution"].mean(), 1),
        "pct_heures_avec_velos": round(100 * df["a_velos"].mean(), 1),
        "heures_3_sources": int(three.sum()),
        "periode_velos": [str(with_velos.min()), str(with_velos.max())] if len(with_velos) else None,
        "heures_3_sources_par_ville": {k: int(v) for k, v in three.groupby(df["ville_id"]).sum().items()},
        "fichiers": written,
        "duration_s": round(time.monotonic() - t0, 1),
    }
    storage.put_json(f"silver/_manifests/croise_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json", stats)
    log.info("Silver croisé : %d lignes ville×heure | météo %.1f %% | pollution %.1f %% | vélos %.1f %% | "
             "3 sources : %d heures | %ss", len(df), stats["pct_heures_avec_meteo"],
             stats["pct_heures_avec_pollution"], stats["pct_heures_avec_velos"], stats["heures_3_sources"],
             stats["duration_s"])
    return stats


if __name__ == "__main__":
    import argparse

    from common.storage import get_storage

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--local", action="store_true", help="lire/écrire dans data/ au lieu de Silo")
    args = parser.parse_args()
    transform(get_storage(use_local=args.local))
