"""P3 — Nettoyage OpenAQ v3 (bronze latest + backfill) -> silver Parquet.

Opérations :
  1. Extraction des messages capteurs bruts (enveloppe `make_message` de `p3_pollution/ingestion.py`),
     compatible avec les réponses `/locations/{id}/latest` et `/sensors/{id}/hours`.
  2. Enrichissement spatial (`latitude`, `longitude`, `location_name`, `ville_id`) depuis
     `config/openaq_locations.json` lorsque le payload `/hours` omet les coordonnées.
  3. Harmonisation des unités (`ppb`, `ppm`, `mg/m³` -> `µg/m³`) et des codes polluants
     (`pm25`, `pm10`, `no2`, `o3`, `co`).
  4. Rejet des valeurs négatives ou hors bornes physiques (`is_valid = 0`).
  5. Dédoublonnage sur `(sensor_id, timestamp)` (le rattrapage `--backfill --days 2` recoupe `/latest`).
  6. Écriture Silver Parquet (strictement issue des fichiers bruts bronze) :
       - `silver/pollution/air_mesure.parquet`   (7 variables demandées + métadonnées)
       - `silver/pollution/air_capteur.parquet`  (capteurs observés dans la couche bronze)

Variables extraites conformément à l'énoncé :
  `sensor_id`, `longitude`, `unit`, `pollutant`, `timestamp`, `latitude`, `value`

Usage :
  python -m p3_pollution.cleaning [--local]

Responsable : P3.
"""

from __future__ import annotations

import argparse
from typing import Any

import numpy as np
import pandas as pd

from common.log import get_logger
from p3_pollution.ingestion import LOCATIONS_FILE, load_locations
from p3_pollution.quality import (
    CANONICAL_UNIT,
    EPISODE_THRESHOLDS_UGM3,
    TARGET_POLLUTANTS,
    WHO_THRESHOLDS_UGM3,
    compute_who_ratio,
    convert_to_ugm3,
    normalize_pollutant,
    pollutant_label,
    quality_summary,
    validate_coordinates_france,
    validate_value_bounds,
)
from p3_pollution.storage import storage_from_env

log = get_logger("p3.cleaning")

SILVER_PREFIX = "silver/pollution"
CAPTEUR_SILVER_KEY = f"{SILVER_PREFIX}/air_capteur.parquet"
MESURE_SILVER_KEY = f"{SILVER_PREFIX}/air_mesure.parquet"

MESURE_COLS = [
    "sensor_id",
    "ville_id",
    "location_id",
    "location_name",
    "pollutant",
    "pollutant_label",
    "timestamp",
    "heure_utc",
    "value",
    "unit",
    "latitude",
    "longitude",
    "ratio_oms",
    "depasse_oms",
    "depasse_episode",
    "quality_flags",
    "is_valid",
]

CAPTEUR_COLS = [
    "sensor_id",
    "location_id",
    "ville_id",
    "location_name",
    "provider",
    "pollutant",
    "pollutant_label",
    "unit",
    "latitude",
    "longitude",
    "distance_m",
]


def build_sensor_catalog(
    locations: list[dict[str, Any]] | None = None,
) -> tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]]]:
    """Construit deux index `(sensor_by_id, location_by_id)` depuis `config/openaq_locations.json`."""
    locs = locations if locations is not None else (load_locations() if LOCATIONS_FILE.exists() else [])
    sensor_by_id: dict[int, dict[str, Any]] = {}
    location_by_id: dict[int, dict[str, Any]] = {}

    for loc in locs:
        loc_id = int(loc["location_id"])
        location_by_id[loc_id] = loc
        for s in loc.get("sensors", []):
            sid = int(s["sensor_id"])
            pol = normalize_pollutant(s.get("pollutant")) or str(s.get("pollutant", ""))
            sensor_by_id[sid] = {
                "sensor_id": sid,
                "location_id": loc_id,
                "ville_id": loc["ville_id"],
                "location_name": loc.get("name", ""),
                "provider": loc.get("provider") or "EEA",
                "pollutant": pol,
                "pollutant_label": pollutant_label(pol),
                "unit": CANONICAL_UNIT,
                "unit_raw": s.get("unit", CANONICAL_UNIT),
                "latitude": float(loc["latitude"]),
                "longitude": float(loc["longitude"]),
                "distance_m": int(loc.get("distance_m") or 0),
            }
    return sensor_by_id, location_by_id


def build_sensor_table(locations: list[dict[str, Any]] | None = None) -> pd.DataFrame:
    """Construit la table référentielle `air_capteur` depuis `config/openaq_locations.json`."""
    sensor_by_id, _ = build_sensor_catalog(locations)
    if not sensor_by_id:
        return pd.DataFrame(columns=CAPTEUR_COLS)
    df = pd.DataFrame(list(sensor_by_id.values()))
    return df[CAPTEUR_COLS].sort_values(["ville_id", "location_id", "sensor_id"]).reset_index(drop=True)


def _extract_raw_timestamp(msg: dict[str, Any], payload: dict[str, Any]) -> Any:
    """Extrait l'horodatage UTC depuis un payload `/latest` ou `/hours`."""
    dt_obj = payload.get("datetime")
    if isinstance(dt_obj, dict) and dt_obj.get("utc"):
        return dt_obj["utc"]
    period = payload.get("period")
    if isinstance(period, dict):
        dt_from = period.get("datetimeFrom")
        if isinstance(dt_from, dict) and dt_from.get("utc"):
            return dt_from["utc"]
        dt_to = period.get("datetimeTo")
        if isinstance(dt_to, dict) and dt_to.get("utc"):
            return dt_to["utc"]
    return payload.get("timestamp") or msg.get("timestamp")


def _extract_raw_coordinates(
    msg: dict[str, Any],
    payload: dict[str, Any],
    sensor_meta: dict[str, Any] | None,
    loc_meta: dict[str, Any] | None,
) -> tuple[Any, Any]:
    """Extrait `(latitude, longitude)` du payload ou du référentiel des sites OpenAQ."""
    coords = payload.get("coordinates")
    if isinstance(coords, dict) and coords.get("latitude") is not None and coords.get("longitude") is not None:
        return coords.get("latitude"), coords.get("longitude")
    if msg.get("latitude") is not None and msg.get("longitude") is not None:
        return msg.get("latitude"), msg.get("longitude")
    if sensor_meta is not None:
        return sensor_meta.get("latitude"), sensor_meta.get("longitude")
    if loc_meta is not None:
        return loc_meta.get("latitude"), loc_meta.get("longitude")
    return None, None


def normalize_message(
    msg: dict[str, Any],
    sensor_by_id: dict[int, dict[str, Any]] | None = None,
    location_by_id: dict[int, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Normalise et audite un message bronze OpenAQ en un enregistrement silver."""
    payload = msg.get("payload") if isinstance(msg.get("payload"), dict) else msg
    raw_sid = msg.get("sensor_id") or payload.get("sensorsId") or payload.get("sensor_id")
    try:
        sid = int(raw_sid)
    except (TypeError, ValueError):
        sid = -1

    s_meta = (sensor_by_id or {}).get(sid)
    raw_loc_id = msg.get("location_id") or payload.get("locationsId") or (s_meta or {}).get("location_id") or 0
    try:
        loc_id = int(raw_loc_id)
    except (TypeError, ValueError):
        loc_id = 0
    l_meta = (location_by_id or {}).get(loc_id)

    ville_id = msg.get("ville_id") or (s_meta or {}).get("ville_id") or (l_meta or {}).get("ville_id") or ""
    loc_name = msg.get("location_name") or (s_meta or {}).get("location_name") or (l_meta or {}).get("name") or ""

    raw_pol = msg.get("pollutant") or (payload.get("parameter") or {}).get("name") or (s_meta or {}).get("pollutant")
    pol = normalize_pollutant(raw_pol)
    raw_unit = (
        msg.get("unit")
        or (payload.get("parameter") or {}).get("units")
        or (s_meta or {}).get("unit_raw")
        or CANONICAL_UNIT
    )

    raw_val = payload.get("value")
    if raw_val is None and isinstance(payload.get("summary"), dict):
        raw_val = payload["summary"].get("avg")
    if raw_val is None:
        raw_val = msg.get("value")

    raw_ts = _extract_raw_timestamp(msg, payload)
    ts = pd.to_datetime(raw_ts, errors="coerce", utc=True)

    lat_raw, lon_raw = _extract_raw_coordinates(msg, payload, s_meta, l_meta)

    flags: list[str] = []
    if sid <= 0:
        flags.append("sensor_id_invalide")
    if pol is None:
        flags.append("polluant_inconnu")
    if pd.isna(ts):
        flags.append("timestamp_manquant")

    val_ugm3, conv_err = convert_to_ugm3(raw_val, raw_pol, raw_unit)
    if conv_err:
        if conv_err not in flags:
            flags.append(conv_err)
    elif pol is not None:
        b_err = validate_value_bounds(val_ugm3, pol)
        if b_err:
            flags.append(b_err)

    coord_err = validate_coordinates_france(lat_raw, lon_raw)
    if coord_err:
        flags.append(coord_err)
        lat_f, lon_f = np.nan, np.nan
    else:
        lat_f, lon_f = round(float(lat_raw), 6), round(float(lon_raw), 6)

    is_valid = len(flags) == 0
    heure_utc = ts.floor("h") if not pd.isna(ts) else pd.NaT

    ratio_oms = compute_who_ratio(val_ugm3, pol) if (is_valid and pol) else None
    who_thresh = WHO_THRESHOLDS_UGM3.get(pol or "", float("inf"))
    ep_thresh = EPISODE_THRESHOLDS_UGM3.get(pol or "", float("inf"))
    depasse_oms = int(is_valid and val_ugm3 is not None and val_ugm3 > who_thresh)
    depasse_episode = int(is_valid and val_ugm3 is not None and val_ugm3 >= ep_thresh)

    return {
        "sensor_id": sid,
        "ville_id": str(ville_id),
        "location_id": loc_id,
        "location_name": str(loc_name),
        "pollutant": pol or str(raw_pol or ""),
        "pollutant_label": pollutant_label(pol or str(raw_pol or "")),
        "timestamp": ts,
        "heure_utc": heure_utc,
        "value": float(val_ugm3) if val_ugm3 is not None else np.nan,
        "unit": CANONICAL_UNIT,
        "latitude": lat_f,
        "longitude": lon_f,
        "ratio_oms": float(ratio_oms) if ratio_oms is not None else np.nan,
        "depasse_oms": depasse_oms,
        "depasse_episode": depasse_episode,
        "quality_flags": ";".join(flags),
        "is_valid": int(is_valid),
    }


def clean_messages(
    messages: list[dict[str, Any]],
    locations: list[dict[str, Any]] | None = None,
    keep_invalid: bool = False,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Transforme une liste de messages bronze en DataFrame silver dédoublonné + résumé qualité."""
    if not messages:
        empty = pd.DataFrame(columns=MESURE_COLS)
        return empty, quality_summary(empty)

    sensor_by_id, location_by_id = build_sensor_catalog(locations)
    rows = [normalize_message(m, sensor_by_id, location_by_id) for m in messages]
    df = pd.DataFrame(rows)

    summary = quality_summary(df.assign(is_valid=df["is_valid"].astype(bool)))

    if not keep_invalid:
        df = df[df["is_valid"] == 1].copy()

    if df.empty:
        return pd.DataFrame(columns=MESURE_COLS), summary

    # Dédoublonnage sur (sensor_id, timestamp) : on garde la dernière mesure valide reçue
    df = (
        df.sort_values(["sensor_id", "timestamp", "is_valid"])
        .drop_duplicates(subset=["sensor_id", "timestamp"], keep="last")
        .reset_index(drop=True)
    )
    return df[MESURE_COLS], summary


def aggregate_ville_heure(df: pd.DataFrame) -> pd.DataFrame:
    """Agrège les mesures valides par `(ville_id, heure_utc)` avec une colonne par polluant (pour P4)."""
    if df.empty:
        return pd.DataFrame(
            columns=["ville_id", "heure_utc", "pm25", "pm10", "no2", "o3", "co",
                     "indice_pollution_oms", "polluant_dominant", "nb_capteurs"]
        )

    valid = df[df["is_valid"] == 1].copy() if "is_valid" in df else df.copy()
    if valid.empty:
        return pd.DataFrame(
            columns=["ville_id", "heure_utc", "pm25", "pm10", "no2", "o3", "co",
                     "indice_pollution_oms", "polluant_dominant", "nb_capteurs"]
        )

    piv = (
        valid.pivot_table(
            index=["ville_id", "heure_utc"],
            columns="pollutant",
            values="value",
            aggfunc="mean",
        )
        .reset_index()
    )
    piv.columns.name = None
    for pol in TARGET_POLLUTANTS:
        if pol not in piv.columns:
            piv[pol] = np.nan
        else:
            piv[pol] = piv[pol].round(2)

    # Indice synthétique normalisé par les seuils OMS (max des ratios polluant / seuil OMS)
    ratio_cols = {}
    for pol in TARGET_POLLUTANTS:
        ratio_cols[pol] = piv[pol] / WHO_THRESHOLDS_UGM3[pol]
    ratio_df = pd.DataFrame(ratio_cols, index=piv.index)
    piv["indice_pollution_oms"] = ratio_df.max(axis=1).round(3)
    all_na = ratio_df.isna().all(axis=1)
    dominant = ratio_df.fillna(-1.0).idxmax(axis=1)
    piv["polluant_dominant"] = np.where(all_na, "", dominant)

    capteurs = (
        valid.groupby(["ville_id", "heure_utc"])["sensor_id"]
        .nunique()
        .rename("nb_capteurs")
        .reset_index()
    )
    out = piv.merge(capteurs, on=["ville_id", "heure_utc"], how="left")
    ordered_cols = [
        "ville_id", "heure_utc", "pm25", "pm10", "no2", "o3", "co",
        "indice_pollution_oms", "polluant_dominant", "nb_capteurs",
    ]
    return out[ordered_cols].sort_values(["ville_id", "heure_utc"]).reset_index(drop=True)


def clean_all(storage: Any = None, use_local: bool | None = None) -> dict[str, Any]:
    """Lit tous les fichiers bronze OpenAQ (`bronze/openaq/` + `bronze/openaq_backfill/`),
    nettoie, dédoublonne et écrit les Parquet silver strictement dérivés du bronze.

    Si aucun fichier brut bronze n'est présent ou aucune mesure valide n'en est extraite,
    aucun fichier n'est écrit dans la couche silver.
    """
    st = storage or storage_from_env(use_local=use_local)
    locations = load_locations() if LOCATIONS_FILE.exists() else []

    keys = sorted(
        k for k in (st.list_keys("bronze/openaq/") + st.list_keys("bronze/openaq_backfill/"))
        if k.endswith(".json") or k.endswith(".json.gz")
    )
    log.info("Fichiers bronze OpenAQ trouvés : %d", len(keys))

    if not keys:
        log.warning("Aucun fichier bronze OpenAQ trouvé : aucun Parquet silver écrit.")
        return {
            "bronze_files": 0,
            "raw_messages": 0,
            "capteurs": 0,
            "mesures_valides": 0,
            "quality": quality_summary(pd.DataFrame()),
        }

    all_messages: list[dict[str, Any]] = []
    for k in keys:
        try:
            payload = st.read_json(k)
            if isinstance(payload, list):
                all_messages.extend(payload)
            elif isinstance(payload, dict):
                all_messages.append(payload)
        except Exception as exc:  # noqa: BLE001
            log.warning("Fichier bronze illisible %s : %s", k, exc)

    mesures_df, q_summary = clean_messages(all_messages, locations=locations)
    if mesures_df.empty:
        log.warning("Aucune mesure valide issue du bronze OpenAQ : aucun Parquet silver écrit.")
        return {
            "bronze_files": len(keys),
            "raw_messages": len(all_messages),
            "capteurs": 0,
            "mesures_valides": 0,
            "quality": q_summary,
        }

    st.write_parquet(MESURE_SILVER_KEY, mesures_df)

    # Capteurs silver strictement limités aux capteurs présents dans les données bronze nettoyées
    present_sensors = set(mesures_df["sensor_id"].unique())
    capteurs_df = build_sensor_table(locations)
    capteurs_df = capteurs_df[capteurs_df["sensor_id"].isin(present_sensors)].reset_index(drop=True)
    st.write_parquet(CAPTEUR_SILVER_KEY, capteurs_df)

    result = {
        "bronze_files": len(keys),
        "raw_messages": len(all_messages),
        "capteurs": len(capteurs_df),
        "mesures_valides": len(mesures_df),
        "quality": q_summary,
    }
    log.info("Nettoyage P3 terminé (Bronze -> Silver Parquet) : %s", result)
    return result


def load_silver_mesures(storage: Any = None, use_local: bool | None = None) -> pd.DataFrame:
    """Charge le Parquet silver des mesures de pollution."""
    st = storage or storage_from_env(use_local=use_local)
    if st.exists(MESURE_SILVER_KEY):
        return st.read_parquet(MESURE_SILVER_KEY)
    return pd.DataFrame(columns=MESURE_COLS)


def main() -> None:
    parser = argparse.ArgumentParser(description="P3 — Nettoyage bronze OpenAQ -> silver Parquet")
    parser.add_argument("--local", action="store_true", help="Forcer le backend disque local (data/)")
    args = parser.parse_args()
    print(clean_all(use_local=True if args.local else None))


if __name__ == "__main__":
    main()
