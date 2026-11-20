"""P1 — Transform : bronze Open-Meteo (JSON) → silver météo (Parquet) dans Silo.

Entrée  : bronze/meteo/ville=<ville_id>/<début>_<fin>.json   (réponses brutes de l'Extract)
Sorties : silver/meteo/year=<YYYY>/meteo_heure.parquet       1 ligne par ville × heure UTC
          silver/_couverture/meteo.parquet                   % d'heures présentes / valides par ville × mois
          silver/_manifests/meteo_<horodatage>.json           bilan de la transformation

Traitements :
  * renommage en français + unités harmonisées (vent km/h → m/s) ;
  * horodatage converti en datetime UTC (timezone-aware) ;
  * contrôle de plausibilité par variable : les valeurs suspectes sont MARQUÉES dans `quality_flag`
    (liste de motifs séparés par « ; »), jamais supprimées ; `is_valid` = aucun motif ;
  * dédoublonnage (ville_id, heure_utc) par sécurité ;
  * écriture idempotente : relancer réécrit les mêmes clés.

Responsable : P1 (Transform de l'ETL local).
"""
from __future__ import annotations

import io
import json
import time
from datetime import datetime, timezone

import pandas as pd

from common.log import get_logger
from common.period import study_period
from p1_meteo.extract import BRONZE_PREFIX, bronze_key

SILVER_PREFIX = "silver/meteo"
COVERAGE_KEY = "silver/_couverture/meteo.parquet"

# colonne Open-Meteo -> (colonne silver, facteur de conversion)
COLUMNS = {
    "temperature_2m": ("temperature_c", 1.0),
    "relative_humidity_2m": ("humidite_pct", 1.0),
    "precipitation": ("precipitation_mm", 1.0),
    "pressure_msl": ("pression_hpa", 1.0),
    "cloud_cover": ("nebulosite_pct", 1.0),
    "wind_speed_10m": ("vent_ms", 1 / 3.6),          # km/h -> m/s
    "wind_direction_10m": ("vent_direction_deg", 1.0),
}

# bornes de plausibilité (incluses) ; au-delà : valeur marquée suspecte
BOUNDS = {
    "temperature_c": (-40.0, 50.0),
    "humidite_pct": (0.0, 100.0),
    "precipitation_mm": (0.0, 200.0),
    "pression_hpa": (900.0, 1080.0),
    "nebulosite_pct": (0.0, 100.0),
    "vent_ms": (0.0, 75.0),
    "vent_direction_deg": (0.0, 360.0),
}

log = get_logger("p1.transform")


# ---------------------------------------------------------------- fonctions pures (testées)

def parse_openmeteo(payload: dict, ville_id: str) -> pd.DataFrame:
    """Réponse Open-Meteo -> DataFrame silver (avant contrôle qualité)."""
    hourly = payload.get("hourly") or {}
    if not hourly.get("time"):
        return pd.DataFrame()
    df = pd.DataFrame({"heure_utc": pd.to_datetime(hourly["time"], utc=True)})  # demandé en timezone=UTC
    df.insert(0, "ville_id", ville_id)
    for src, (dst, factor) in COLUMNS.items():
        values = pd.to_numeric(pd.Series(hourly.get(src, [None] * len(df))), errors="coerce")
        df[dst] = (values * factor).round(2) if factor != 1.0 else values.astype("float64")
    return df


def apply_quality(df: pd.DataFrame) -> pd.DataFrame:
    """Ajoute quality_flag (motifs) et is_valid. Les valeurs ne sont pas modifiées."""
    out = df.copy()
    reasons = pd.Series([[] for _ in range(len(out))], index=out.index, dtype=object)
    for col, (lo, hi) in BOUNDS.items():
        if col not in out:
            continue
        missing = out[col].isna()
        outside = ~missing & ((out[col] < lo) | (out[col] > hi))
        for mask, label in ((missing, f"{col}_manquant"), (outside, f"{col}_hors_bornes")):
            for i in out.index[mask]:
                reasons[i].append(label)
    out["quality_flag"] = reasons.map(";".join)
    out["is_valid"] = out["quality_flag"] == ""
    return out


def coverage_by_month(df: pd.DataFrame, start, end) -> pd.DataFrame:
    """Par ville × mois : heures attendues, présentes, valides et pourcentages."""
    hours = pd.date_range(pd.Timestamp(start, tz="UTC"), pd.Timestamp(end, tz="UTC") + pd.Timedelta(hours=23),
                          freq="h")
    expected = pd.Series(1, index=hours).groupby(hours.strftime("%Y-%m")).size().rename("heures_attendues")
    d = df.assign(mois=df["heure_utc"].dt.strftime("%Y-%m"))
    g = d.groupby(["ville_id", "mois"]).agg(heures_presentes=("heure_utc", "nunique"),
                                            heures_valides=("is_valid", "sum")).reset_index()
    g = g.merge(expected, left_on="mois", right_index=True, how="left")
    g["pct_presentes"] = (100 * g["heures_presentes"] / g["heures_attendues"]).round(1)
    g["pct_valides"] = (100 * g["heures_valides"] / g["heures_attendues"]).round(1)
    return g


def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    return buf.getvalue()


# ---------------------------------------------------------------- transformation

def transform(storage, villes, start=None, end=None) -> dict:
    """Lit le bronze météo de chaque ville et écrit le silver partitionné par année."""
    if start is None or end is None:
        start, end = study_period()
    t0 = time.monotonic()
    frames, missing = [], []
    for v in villes:
        key = bronze_key(v["ville_id"], start, end)
        if not storage.exists(key):
            missing.append(v["ville_id"])
            log.warning("Bronze absent pour %s : %s", v["ville_id"], key)
            continue
        frames.append(parse_openmeteo(json.loads(storage.get_bytes(key)), v["ville_id"]))
    if not frames:
        raise RuntimeError(f"aucun fichier bronze sous {BRONZE_PREFIX}/ pour la période {start} → {end}")

    df = pd.concat(frames, ignore_index=True)
    n_raw = len(df)
    df = df.drop_duplicates(subset=["ville_id", "heure_utc"], keep="first")
    df = apply_quality(df).sort_values(["ville_id", "heure_utc"]).reset_index(drop=True)
    df["source"] = "open-meteo"

    written = {}
    for year, part in df.groupby(df["heure_utc"].dt.year):
        key = f"{SILVER_PREFIX}/year={year}/meteo_heure.parquet"
        storage.put_bytes(key, to_parquet_bytes(part), content_type="application/vnd.apache.parquet")
        written[key] = len(part)
    cov = coverage_by_month(df, start, end)
    storage.put_bytes(COVERAGE_KEY, to_parquet_bytes(cov), content_type="application/vnd.apache.parquet")

    flags = df.loc[~df["is_valid"], "quality_flag"].str.split(";").explode().value_counts()
    stats = {
        "source": "bronze/meteo (Open-Meteo)",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "period": {"start": str(start), "end": str(end)},
        "villes": sorted(df["ville_id"].unique().tolist()),
        "villes_sans_bronze": missing,
        "lignes_bronze": n_raw,
        "doublons_supprimes": n_raw - len(df),
        "lignes_silver": len(df),
        "lignes_valides": int(df["is_valid"].sum()),
        "motifs_qualite": {k: int(v) for k, v in flags.items()},
        "couverture_min_pct": float(cov["pct_valides"].min()),
        "fichiers": written,
        "duration_s": round(time.monotonic() - t0, 1),
    }
    storage.put_json(f"silver/_manifests/meteo_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json", stats)
    log.info("Silver météo : %d lignes (%d valides), %d doublons supprimés, %d fichiers, couverture min %.1f %%",
             stats["lignes_silver"], stats["lignes_valides"], stats["doublons_supprimes"], len(written),
             stats["couverture_min_pct"])
    return stats


if __name__ == "__main__":
    import argparse

    from common.config import load_villes
    from common.storage import get_storage

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--local", action="store_true", help="lire/écrire dans data/ au lieu de Silo")
    args = parser.parse_args()
    transform(get_storage(use_local=args.local), load_villes())
