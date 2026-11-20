"""P1 — Nettoyage Open-Meteo (JSON horaire 5 ans + récent) → Parquet silver.

Nettoyages appliqués :
  1. Extraction des 8 variables demandées :
     `station_id`, `timestamp` (`heure_utc`), `temperature`, `pressure`,
     `wind_speed`, `wind_direction`, `precipitation`, `visibility`
     + `ville_id`, `latitude`, `longitude`, `dew_point`, `relative_humidity`.
  2. Conversion du timestamp ISO en UTC (`DateTime('UTC')`) et dédoublonnage horaire.
  3. Conversion des unités :
     - Vitesse du vent : `km/h` → `m/s` (÷ 3,6) lorsque `hourly_units.wind_speed_10m == 'km/h'`
     - Température : `°F` → `°C` si nécessaire
     - Visibilité : `km` → `m` si nécessaire
  4. Traitement des valeurs manquantes :
     - Interpolation linéaire courte (<= 3 h) sur les variables continues (`temperature`, `pressure`)
     - Précipitations manquantes remplacées par `0.0`
     - Estimation physique de la visibilité horizontale (formule d'extinction hygrométrique/pluie)
       lorsque la réanalyse ERA5 renvoie `null` sur les années anciennes, tout en conservant les
       mesures directes fournies par Open-Meteo sur la fenêtre récente.
  5. Contrôle qualité (`check_quality`, neutralisation des valeurs hors bornes et des pics isolés).

Responsable : P1.
"""

from __future__ import annotations

import argparse
import io
import json
import re
from typing import Any

import numpy as np
import pandas as pd

from common.config import load_ref_ville, load_villes
from common.log import setup_logging
from common.storage import Storage, get_storage
from p1_meteo.ingestion import BRONZE_PREFIX, station_id_for_ville
from p1_meteo.quality import (
    check_quality,
    flag_temperature_spikes,
    nullify_out_of_bounds,
)

log = setup_logging(__name__)

SILVER_PREFIX = "silver/meteo"
SILVER_KEY = f"{SILVER_PREFIX}/meteo_obs.parquet"
STATION_SILVER_KEY = f"{SILVER_PREFIX}/meteo_station.parquet"
LEGACY_BRONZE_PREFIX = "p1_meteo/raw/openmeteo"

RENAMES = {
    "time": "heure_utc",
    "temperature_2m": "temperature",
    "relative_humidity_2m": "relative_humidity",
    "dew_point_2m": "dew_point",
    "precipitation": "precipitation",
    "pressure_msl": "pressure",
    "wind_speed_10m": "wind_speed",
    "wind_direction_10m": "wind_direction",
    "visibility": "visibility",
}

NUMERIC_COLS = [
    "temperature",
    "dew_point",
    "relative_humidity",
    "pressure",
    "wind_speed",
    "wind_direction",
    "precipitation",
    "visibility",
]

SILVER_COLS = [
    "station_id",
    "ville_id",
    "timestamp",
    "heure_utc",
    "temperature",
    "dew_point",
    "relative_humidity",
    "pressure",
    "wind_speed",
    "wind_direction",
    "precipitation",
    "visibility",
    "latitude",
    "longitude",
    "quality_flags",
    "is_valid",
]

REQUIRED_VARS = [
    "temperature",
    "pressure",
    "wind_speed",
    "wind_direction",
    "precipitation",
    "visibility",
]


def relative_humidity(temp: pd.Series, dew: pd.Series) -> pd.Series:
    """Calcule l'humidité relative (%) par la formule de Magnus."""
    a, b = 17.625, 243.04
    rh = 100.0 * np.exp(a * dew / (b + dew)) / np.exp(a * temp / (b + temp))
    return rh.clip(lower=0.0, upper=100.0).round(1)


def estimate_visibility_m(
    rh: pd.Series,
    precip: pd.Series,
    wind_ms: pd.Series,
) -> pd.Series:
    """Estime la visibilité horizontale (en mètres) à partir de l'humidité relative, de la pluie et du vent
    lorsque le champ `visibility` est absent de la réanalyse historique ERA5.
    """
    rh_clean = rh.fillna(70.0).clip(0.0, 100.0)
    pr_clean = precip.fillna(0.0).clip(lower=0.0)
    w_clean = wind_ms.fillna(3.0).clip(lower=0.0)

    # Extinction hygrométrique non linéaire : chute marquée au-delà de HR > 90 % (brume / brouillard)
    base_vis = 26000.0 - (rh_clean - 45.0).clip(lower=0.0) * 320.0
    fog_penalty = np.where(
        rh_clean >= 97.0,
        7500.0,
        np.where(rh_clean >= 93.0, 4500.0, np.where(rh_clean >= 88.0, 2000.0, 0.0)),
    )
    rain_penalty = pr_clean * 2400.0
    wind_dispersion = (w_clean - 2.0) * 420.0

    est = (base_vis - fog_penalty - rain_penalty + wind_dispersion).clip(lower=200.0, upper=30000.0)
    return pd.Series(est, index=rh.index).round(0)


def convert_openmeteo_units(df: pd.DataFrame, units: dict[str, Any] | None = None) -> pd.DataFrame:
    """Convertit les unités Open-Meteo vers le système standard (m/s, °C, hPa, mm, m)."""
    out = df.copy()
    u = units or {}

    # Vitesse du vent : km/h -> m/s (Open-Meteo renvoie km/h par défaut)
    w_unit = str(u.get("wind_speed_10m", "km/h")).lower().strip()
    if "wind_speed" in out and w_unit in ("km/h", "kmh"):
        out["wind_speed"] = (pd.to_numeric(out["wind_speed"], errors="coerce") / 3.6).round(2)

    # Température : °F -> °C si besoin
    t_unit = str(u.get("temperature_2m", "°c")).lower().strip()
    if "temperature" in out and t_unit in ("°f", "fahrenheit", "f"):
        out["temperature"] = ((pd.to_numeric(out["temperature"], errors="coerce") - 32.0) * 5.0 / 9.0).round(2)

    # Visibilité : km -> m si besoin
    v_unit = str(u.get("visibility", "m")).lower().strip()
    if "visibility" in out and v_unit == "km":
        out["visibility"] = (pd.to_numeric(out["visibility"], errors="coerce") * 1000.0).round(0)

    return out


_BRONZE_DATE_RANGE_RE = re.compile(r"(?P<start>\d{4}-\d{2}-\d{2})_(?P<end>\d{4}-\d{2}-\d{2})\.json$")


def clean_openmeteo_json(
    payload: dict[str, Any],
    ville_id: str,
    station_id: str | None = None,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    now_utc: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Transforme la réponse JSON Open-Meteo en DataFrame horaire nettoyé, converti et contrôlé."""
    hourly = payload.get("hourly", {})
    if not hourly or not hourly.get("time"):
        return pd.DataFrame(columns=SILVER_COLS)

    df = pd.DataFrame(hourly).rename(columns=RENAMES)
    df["heure_utc"] = pd.to_datetime(df["heure_utc"], errors="coerce", utc=True)
    df = df.dropna(subset=["heure_utc"]).copy()
    if df.empty:
        return pd.DataFrame(columns=SILVER_COLS)

    df["heure_utc"] = df["heure_utc"].dt.floor("h")

    # Exclure les heures futures de prévision et respecter la fenêtre demandée (ex. 1 semaine)
    upper_now = (now_utc if now_utc is not None else pd.Timestamp.now(tz="UTC")).floor("h")
    req_start = start_date or payload.get("requested_start_date")
    req_end = end_date or payload.get("requested_end_date")
    if req_start:
        start_ts = pd.Timestamp(f"{req_start}T00:00:00Z")
        df = df[df["heure_utc"] >= start_ts]
    upper_bound = upper_now
    if req_end:
        end_ts = pd.Timestamp(f"{req_end}T23:59:59Z")
        upper_bound = min(upper_bound, end_ts)
    df = df[df["heure_utc"] <= upper_bound].copy()
    if df.empty:
        return pd.DataFrame(columns=SILVER_COLS)

    df["timestamp"] = df["heure_utc"]
    df["ville_id"] = str(payload.get("ville_id") or ville_id)

    ref_map = {r["ville_id"]: r for r in load_ref_ville()}
    resolved_sid = (
        station_id
        or payload.get("station_id")
        or station_id_for_ville({"ville_id": df["ville_id"].iloc[0]}, ref_map)
    )
    df["station_id"] = str(resolved_sid)
    df["latitude"] = float(payload.get("latitude") or (ref_map.get(ville_id) or {}).get("lat") or np.nan)
    df["longitude"] = float(payload.get("longitude") or (ref_map.get(ville_id) or {}).get("lon") or np.nan)

    for col in NUMERIC_COLS:
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 1. Conversion des unités (ex. vent km/h -> m/s)
    df = convert_openmeteo_units(df, payload.get("hourly_units"))

    # 2. Dédoublonnage chronologique sur (station_id, heure_utc)
    df = (
        df.sort_values(["station_id", "heure_utc"])
        .drop_duplicates(subset=["station_id", "heure_utc"], keep="last")
        .reset_index(drop=True)
    )

    # 3. Traitement des valeurs manquantes
    df["precipitation"] = df["precipitation"].fillna(0.0)
    for cont_col in ("temperature", "dew_point", "pressure", "wind_speed"):
        df[cont_col] = df[cont_col].interpolate(method="linear", limit=3)

    if df["relative_humidity"].isna().any():
        calc_rh = relative_humidity(df["temperature"], df["dew_point"])
        df["relative_humidity"] = df["relative_humidity"].fillna(calc_rh)
    if df["dew_point"].isna().any() and df["relative_humidity"].notna().any():
        df["dew_point"] = df["dew_point"].fillna(
            (df["temperature"] - (100.0 - df["relative_humidity"]) / 5.0).round(2)
        )

    if df["visibility"].isna().any():
        est_vis = estimate_visibility_m(df["relative_humidity"], df["precipitation"], df["wind_speed"])
        df["visibility"] = df["visibility"].fillna(est_vis)

    # 4. Contrôle qualité, bornes physiques et détection des pics thermiques
    return clean_hourly(df)


def clean_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """Applique les bornes physiques, la détection de pics de température et les drapeaux qualité."""
    if df.empty:
        return pd.DataFrame(columns=SILVER_COLS)

    work = df.copy()
    if "ville_id" not in work.columns:
        work["ville_id"] = ""
    if "timestamp" not in work.columns and "heure_utc" in work.columns:
        work["timestamp"] = work["heure_utc"]
    for col in NUMERIC_COLS + ["latitude", "longitude"]:
        if col not in work.columns:
            work[col] = np.nan

    work = work.sort_values(["station_id", "heure_utc"]).reset_index(drop=True)
    flagged = check_quality(work)
    clean = nullify_out_of_bounds(work)
    spikes = flag_temperature_spikes(clean)
    clean.loc[spikes, "temperature"] = np.nan

    flags = flagged["quality_flags"].where(
        ~spikes,
        (flagged["quality_flags"] + ";pic_temperature").str.lstrip(";"),
    )
    clean["quality_flags"] = flags.astype(str)
    clean["is_valid"] = (clean["quality_flags"] == "").astype("uint8")
    clean["timestamp"] = clean["heure_utc"]
    if clean["relative_humidity"].isna().any():
        clean["relative_humidity"] = clean["relative_humidity"].fillna(
            relative_humidity(clean["temperature"], clean["dew_point"])
        )

    clean = clean.dropna(subset=REQUIRED_VARS, how="all")
    for col in NUMERIC_COLS:
        clean[col] = clean[col].astype("float64").round(2)
    return clean[SILVER_COLS].reset_index(drop=True)


def build_station_table(
    stations_meta: list[dict[str, Any]] | None = None,
    obs: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Construit la table référentielle `meteo_station`."""
    if stations_meta is None:
        ref_map = {r["ville_id"]: r for r in load_ref_ville()}
        stations_meta = [
            {
                "station_id": station_id_for_ville(v, ref_map),
                "nom": f"{v['nom']} (Open-Meteo)",
                "icao": "",
                "latitude": float(v["lat"]),
                "longitude": float(v["lon"]),
                "elevation": None,
                "ville_id": v["ville_id"],
                "distance_ville_km": 0.0,
            }
            for v in load_villes()
        ]

    df = pd.DataFrame(stations_meta)
    cols = [
        "station_id", "nom", "icao", "latitude", "longitude",
        "elevation", "ville_id", "distance_ville_km",
    ]
    if df.empty:
        return pd.DataFrame(columns=cols)
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols].copy()
    if obs is not None and not obs.empty:
        stats = (
            obs.groupby("station_id")
            .agg(
                nb_heures=("heure_utc", "size"),
                premiere_heure=("heure_utc", "min"),
                derniere_heure=("heure_utc", "max"),
            )
            .reset_index()
        )
        df = df.merge(stats, on="station_id", how="left")
    df["ville_id"] = df["ville_id"].fillna("").astype(str)
    return df


def aggregate_ville_heure(obs: pd.DataFrame, stations: pd.DataFrame | None = None) -> pd.DataFrame:
    """Moyenne horaire par `(ville_id, heure_utc)` (clé de jointure P4)."""
    if obs.empty:
        return pd.DataFrame()
    df = obs.copy()
    if ("ville_id" not in df.columns or (df["ville_id"] == "").all()) and stations is not None and not stations.empty:
        link = stations.loc[stations["ville_id"] != "", ["station_id", "ville_id"]]
        df = df.drop(columns=["ville_id"], errors="ignore").merge(link, on="station_id", how="inner")
    df = df[df["ville_id"] != ""].copy()
    if df.empty:
        return pd.DataFrame()

    rad = np.deg2rad(df["wind_direction"])
    df["_u"], df["_v"] = np.sin(rad), np.cos(rad)
    agg = (
        df.groupby(["ville_id", "heure_utc"], as_index=False)
        .agg(
            temperature=("temperature", "mean"),
            dew_point=("dew_point", "mean"),
            relative_humidity=("relative_humidity", "mean"),
            pressure=("pressure", "mean"),
            wind_speed=("wind_speed", "mean"),
            precipitation=("precipitation", "mean"),
            visibility=("visibility", "mean"),
            _u=("_u", "mean"),
            _v=("_v", "mean"),
            nb_stations=("station_id", "nunique"),
        )
    )
    agg["wind_direction"] = (np.rad2deg(np.arctan2(agg["_u"], agg["_v"])) % 360).round(0)
    agg = agg.drop(columns=["_u", "_v"])
    for c in ("temperature", "dew_point", "relative_humidity", "pressure", "wind_speed", "precipitation", "visibility"):
        agg[c] = agg[c].round(2)
    return agg.sort_values(["ville_id", "heure_utc"]).reset_index(drop=True)


def clean_all(storage: Any = None, use_local: bool = False) -> pd.DataFrame:
    """Lit tous les JSON bronze Open-Meteo (`bronze/meteo/`), nettoie et écrit les Parquet silver.

    La couche silver (`silver/meteo/`) ne contient uniquement que les fichiers Parquet issus du
    nettoyage des données brutes de la couche bronze. Si aucun fichier bronze n'est présent,
    rien n'est écrit dans la couche silver.
    """
    st = storage if storage is not None else (get_storage(use_local=True) if use_local else Storage())
    keys = sorted(
        {
            k
            for prefix in (f"{BRONZE_PREFIX}/", f"{LEGACY_BRONZE_PREFIX}/")
            for k in st.list(prefix)
            if k.endswith(".json")
        }
    )
    frames: list[pd.DataFrame] = []

    for key in keys:
        parts = key.split("/")
        ville_id = parts[-2] if len(parts) >= 2 else "france"
        m_dates = _BRONZE_DATE_RANGE_RE.search(parts[-1])
        key_start = m_dates.group("start") if m_dates else None
        key_end = m_dates.group("end") if m_dates else None
        try:
            payload = json.loads(st.get_bytes(key).decode("utf-8"))
            frame = clean_openmeteo_json(
                payload,
                ville_id,
                start_date=key_start,
                end_date=key_end,
            )
            if not frame.empty:
                frames.append(frame)
        except Exception as exc:  # noqa: BLE001
            log.warning("Nettoyage impossible pour %s : %s", key, exc)

    if not frames:
        log.warning("Aucun fichier bronze Open-Meteo nettoyé : aucun Parquet silver écrit.")
        return pd.DataFrame(columns=SILVER_COLS)

    df = (
        pd.concat(frames, ignore_index=True)
        .sort_values(["ville_id", "station_id", "heure_utc"])
        .drop_duplicates(subset=["station_id", "heure_utc"], keep="last")
        .reset_index(drop=True)
    )

    # Écriture stricte des Parquet silver générés depuis la couche bronze
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    st.put_bytes(SILVER_KEY, buf.getvalue(), "application/octet-stream")

    present_sids = set(df["station_id"].unique())
    stations_df = build_station_table(obs=df)
    stations_df = stations_df[stations_df["station_id"].isin(present_sids)].reset_index(drop=True)
    st.put_parquet(STATION_SILVER_KEY, stations_df)

    log.info(
        "Silver P1 écrit depuis Bronze (%d fichiers) : %s (%d lignes, %d stations)",
        len(keys),
        SILVER_KEY,
        len(df),
        len(stations_df),
    )
    return df


def load_silver(storage: Any = None, use_local: bool = False) -> pd.DataFrame:
    """Charge les observations météo silver depuis `silver/meteo/meteo_obs.parquet`."""
    st = storage if storage is not None else get_storage(use_local=use_local)
    if st.exists(SILVER_KEY):
        return st.get_parquet(SILVER_KEY)
    return pd.DataFrame(columns=SILVER_COLS)


def main() -> None:
    parser = argparse.ArgumentParser(description="P1 — Nettoyage Open-Meteo bronze -> silver Parquet")
    parser.add_argument("--local", action="store_true", help="Utiliser le stockage disque local (data/)")
    args = parser.parse_args()
    df = clean_all(use_local=args.local)
    print(f"Lignes silver P1 : {len(df)}")


if __name__ == "__main__":
    main()
