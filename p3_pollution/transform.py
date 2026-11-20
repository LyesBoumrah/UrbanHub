"""P3 — Transform : bronze OpenAQ (CSV.gz journaliers) → silver pollution (Parquet) dans Silo.

Entrée  : bronze/openaq_archive/locationid=<id>/year=…/month=…/location-<id>-<AAAAMMJJ>.csv.gz
          (≈ 69 000 petits fichiers : un par station et par jour)
Sorties : silver/pollution/year=<YYYY>/pollution_heure.parquet   1 ligne par station × polluant × heure UTC
          silver/pollution/stations.parquet                      dimension des 55 stations
          silver/_couverture/pollution.parquet                   % d'heures présentes / valides
                                                                 par station × polluant × mois
          silver/_manifests/pollution_<horodatage>.json

Traitements :
  1. regroupement des petits fichiers : lecture station par station (téléchargements parallèles),
     concaténation des CSV puis un seul parsing par station ;
  2. on ne garde que les 5 polluants du sujet (pm25, pm10, no2, o3, co) ; NO et SO2 sont écartés ;
  3. horodatage : l'archive date chaque mesure par la FIN de l'heure, en heure locale avec décalage
     (« 2023-01-01T01:00:00+01:00 » = moyenne de 23:00 à 00:00 UTC). On convertit en UTC puis on
     ramène au DÉBUT de l'heure (− 1 h) : `heure_utc` a la même convention que la météo ;
  4. renumérotation EEA 2024 : les anciens identifiants (`predecessor_location_ids`) sont rattachés à la
     station actuelle (`station_id`) ; l'identifiant d'origine est conservé (`location_id_source`) ;
  5. dédoublonnage sur (station_id, polluant, heure_utc), en gardant la mesure de la station ACTUELLE
     si l'ancien et le nouvel identifiant mesurent la même heure ;
  6. unités harmonisées en µg/m³ (contrôle : toute autre unité est convertie ou signalée) ;
  7. qualité : valeur manquante, négative ou au-dessus d'un plafond physique → MARQUÉE (`quality_flag`),
     jamais supprimée ;
  8. filtrage sur la période d'étude, écriture partitionnée par année (idempotente).

Responsable : P3 (Transform de l'ETL local).
"""
from __future__ import annotations

import gzip
import io
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pandas as pd

from common.log import get_logger
from common.period import study_period
from p3_pollution.ingestion import BRONZE_PREFIX, archive_ids, load_locations

SILVER_PREFIX = "silver/pollution"
STATIONS_KEY = f"{SILVER_PREFIX}/stations.parquet"
COVERAGE_KEY = "silver/_couverture/pollution.parquet"
POLLUTANTS = ("pm25", "pm10", "no2", "o3", "co")
UNIT = "µg/m³"

# plafonds physiques (µg/m³) : au-delà, la valeur est presque certainement une erreur de capteur
UPPER_BOUNDS = {"pm25": 1000.0, "pm10": 2000.0, "no2": 1000.0, "o3": 800.0, "co": 50000.0}

# conversions vers µg/m³ (à 25 °C, 1 atm : µg/m³ = ppb × masse molaire / 24,45)
MOLAR_MASS = {"no2": 46.01, "o3": 48.00, "co": 28.01}

log = get_logger("p3.transform")


# ---------------------------------------------------------------- fonctions pures (testées)

def station_index(locations):
    """archive_id -> (station actuelle, rang) ; rang 0 = identifiant actuel, 1+ = prédécesseurs."""
    index = {}
    for loc in locations:
        for rank, aid in enumerate(archive_ids(loc)):
            index[aid] = (loc, rank)
    return index


def to_ugm3(values: pd.Series, units: pd.Series, pollutant: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Convertit en µg/m³ ; renvoie (valeurs converties, masque des unités inconnues)."""
    out = values.astype("float64").copy()
    u = units.str.lower().str.replace("µ", "u", regex=False).str.strip()
    mm = pollutant.map(MOLAR_MASS)
    out[u == "mg/m³"] = values[u == "mg/m³"] * 1000
    out[u == "ppb"] = values[u == "ppb"] * mm[u == "ppb"] / 24.45
    out[u == "ppm"] = values[u == "ppm"] * 1000 * mm[u == "ppm"] / 24.45
    known = u.isin(["ug/m³", "mg/m³"]) | (u.isin(["ppb", "ppm"]) & mm.notna())
    return out, ~known


def parse_csv_text(text: str) -> pd.DataFrame:
    return pd.read_csv(io.StringIO(text), dtype={"location_id": "int64", "sensors_id": "int64",
                                                 "parameter": "string", "units": "string"})


def normalize(raw: pd.DataFrame, index) -> pd.DataFrame:
    """CSV bruts d'archive -> lignes silver (avant dédoublonnage et qualité)."""
    df = raw[raw["parameter"].isin(POLLUTANTS)].copy()
    if df.empty:
        return pd.DataFrame()
    ts_end = pd.to_datetime(df["datetime"], utc=True, format="ISO8601")   # fin de l'heure, en UTC
    value, unknown_unit = to_ugm3(pd.to_numeric(df["value"], errors="coerce"), df["units"], df["parameter"])
    meta = df["location_id"].map(lambda a: index[a])
    out = pd.DataFrame({
        "ville_id": meta.map(lambda m: m[0]["ville_id"]),
        "station_id": meta.map(lambda m: m[0]["location_id"]).astype("int64"),
        "location_id_source": df["location_id"].astype("int64"),
        "sensor_id": df["sensors_id"].astype("int64"),
        "polluant": df["parameter"].astype(str),
        "heure_utc": ts_end - pd.Timedelta(hours=1),                       # début de l'heure
        "valeur": value,
        "unite": UNIT,
        "_rang": meta.map(lambda m: m[1]),
        "_unite_inconnue": unknown_unit.values,
    })
    return out.reset_index(drop=True)


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Une mesure par (station, polluant, heure) ; priorité à l'identifiant actuel (rang 0)."""
    return (df.sort_values(["station_id", "polluant", "heure_utc", "_rang"])
              .drop_duplicates(subset=["station_id", "polluant", "heure_utc"], keep="first"))


def apply_quality(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    flags = pd.Series("", index=out.index, dtype=object)

    def add(mask, label):
        nonlocal flags
        flags = flags.mask(mask, flags.where(flags == "", flags + ";") + label)

    add(out["valeur"].isna(), "valeur_manquante")
    add(out["valeur"] < 0, "valeur_negative")
    add(out["valeur"] > out["polluant"].map(UPPER_BOUNDS), "au_dessus_plafond")
    add(out["_unite_inconnue"], "unite_inconnue")
    out["quality_flag"] = flags
    out["is_valid"] = flags == ""
    return out


def coverage(df: pd.DataFrame, start, end) -> pd.DataFrame:
    hours = pd.date_range(pd.Timestamp(start, tz="UTC"), pd.Timestamp(end, tz="UTC") + pd.Timedelta(hours=23),
                          freq="h")
    expected = pd.Series(1, index=hours).groupby(hours.strftime("%Y-%m")).size().rename("heures_attendues")
    d = df.assign(mois=df["heure_utc"].dt.strftime("%Y-%m"))
    g = (d.groupby(["ville_id", "station_id", "polluant", "mois"])
          .agg(heures_presentes=("heure_utc", "size"), heures_valides=("is_valid", "sum")).reset_index())
    g = g.merge(expected, left_on="mois", right_index=True, how="left")
    g["pct_presentes"] = (100 * g["heures_presentes"] / g["heures_attendues"]).round(1)
    g["pct_valides"] = (100 * g["heures_valides"] / g["heures_attendues"]).round(1)
    return g


def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    return buf.getvalue()


# ---------------------------------------------------------------- lecture du bronze

def read_station(storage, loc, pool) -> tuple[pd.DataFrame, int]:
    """Télécharge en parallèle tous les CSV d'une station (et de ses prédécesseurs), un seul parsing."""
    keys = [k for aid in archive_ids(loc) for k in storage.list(f"{BRONZE_PREFIX}/locationid={aid}/")
            if k.endswith(".csv.gz")]
    texts = list(pool.map(lambda k: gzip.decompress(storage.get_bytes(k)).decode("utf-8"), keys))
    if not texts:
        return pd.DataFrame(), 0
    header, bodies = texts[0].split("\n", 1)[0], [t.split("\n", 1)[1] if "\n" in t else "" for t in texts]
    return parse_csv_text(header + "\n" + "".join(b if b.endswith("\n") else b + "\n" for b in bodies)), len(keys)


# ---------------------------------------------------------------- transformation

def transform(storage, locations=None, start=None, end=None, workers: int = 16) -> dict:
    if start is None or end is None:
        start, end = study_period()
    locations = locations or load_locations()
    index = station_index(locations)
    t0 = time.monotonic()
    lo, hi = pd.Timestamp(start, tz="UTC"), pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1)

    frames, n_files, n_raw = [], 0, 0
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="lecture") as pool:
        for i, loc in enumerate(locations, 1):
            raw, nf = read_station(storage, loc, pool)
            n_files += nf
            n_raw += len(raw)
            part = normalize(raw, index) if len(raw) else pd.DataFrame()
            if len(part):
                frames.append(part[(part["heure_utc"] >= lo) & (part["heure_utc"] < hi)])
            if i % 10 == 0 or i == len(locations):
                log.info("… %d / %d stations lues (%d fichiers)", i, len(locations), n_files)

    df = pd.concat(frames, ignore_index=True)
    n_before = len(df)
    n_pred = int((df["_rang"] > 0).sum())
    df = deduplicate(df)
    df = apply_quality(df).drop(columns=["_rang", "_unite_inconnue"])
    df["source"] = "openaq-archive"
    df = df.sort_values(["ville_id", "station_id", "polluant", "heure_utc"]).reset_index(drop=True)

    written = {}
    for year, part in df.groupby(df["heure_utc"].dt.year):
        key = f"{SILVER_PREFIX}/year={year}/pollution_heure.parquet"
        storage.put_bytes(key, to_parquet_bytes(part), content_type="application/vnd.apache.parquet")
        written[key] = len(part)

    stations = pd.DataFrame([{
        "station_id": loc["location_id"], "ville_id": loc["ville_id"], "station_nom": loc["name"],
        "latitude": loc["latitude"], "longitude": loc["longitude"], "distance_centre_m": loc["distance_m"],
        "fournisseur": loc["provider"], "identifiants_archive": ",".join(map(str, archive_ids(loc))),
        "polluants": ",".join(sorted({s["pollutant"] for s in loc["sensors"]})),
    } for loc in locations])
    storage.put_bytes(STATIONS_KEY, to_parquet_bytes(stations), content_type="application/vnd.apache.parquet")
    cov = coverage(df, start, end)
    storage.put_bytes(COVERAGE_KEY, to_parquet_bytes(cov), content_type="application/vnd.apache.parquet")

    flags = df.loc[~df["is_valid"], "quality_flag"].str.split(";").explode().value_counts()
    stats = {
        "source": "bronze/openaq_archive",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "period": {"start": str(start), "end": str(end)},
        "fichiers_bronze_lus": n_files,
        "lignes_bronze": n_raw,
        "lignes_5_polluants_dans_la_periode": n_before,
        "lignes_issues_des_predecesseurs": n_pred,
        "doublons_supprimes": n_before - len(df),
        "lignes_silver": len(df),
        "lignes_valides": int(df["is_valid"].sum()),
        "motifs_qualite": {k: int(v) for k, v in flags.items()},
        "lignes_par_polluant": {k: int(v) for k, v in df["polluant"].value_counts().items()},
        "fichiers": written,
        "duration_s": round(time.monotonic() - t0, 1),
    }
    storage.put_json(f"silver/_manifests/pollution_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json", stats)
    log.info("Silver pollution : %d fichiers bronze → %d lignes silver (%d valides), %d doublons, %ss",
             n_files, stats["lignes_silver"], stats["lignes_valides"], stats["doublons_supprimes"],
             stats["duration_s"])
    return stats


if __name__ == "__main__":
    import argparse

    from common.storage import get_storage

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--local", action="store_true", help="lire/écrire dans data/ au lieu de Silo")
    args = parser.parse_args()
    transform(get_storage(use_local=args.local))
