"""P1 — Nettoyage Open-Meteo (JSON horaire) → Parquet silver.

Responsable : P1.
"""

from __future__ import annotations

import json

import pandas as pd

from common.log import setup_logging
from common.storage import Storage

log = setup_logging(__name__)

SILVER_KEY = "p1_meteo/silver/meteo_obs.parquet"

RENAMES = {
    "time": "heure_utc",
    "temperature_2m": "temperature",
    "precipitation": "precipitation",
    "pressure_msl": "pressure",
    "wind_speed_10m": "wind_speed",
    "wind_direction_10m": "wind_direction",
    "visibility": "visibility",
}


def clean_openmeteo_json(payload: dict, ville_id: str) -> pd.DataFrame:
    """Transforme la réponse Open-Meteo en DataFrame horaire propre."""
    hourly = payload.get("hourly", {})
    if not hourly or not hourly.get("time"):
        return pd.DataFrame()

    df = pd.DataFrame(hourly).rename(columns=RENAMES)
    df["ville_id"] = ville_id
    df["heure_utc"] = pd.to_datetime(df["heure_utc"], errors="coerce", utc=True)
    return df.dropna(subset=["heure_utc"])


def clean_all(storage: Storage | None = None) -> pd.DataFrame:
    """Lit tous les JSON bronze Open-Meteo, concatène et écrit le Parquet silver."""
    storage = storage or Storage()
    keys = list(storage.list("p1_meteo/raw/openmeteo/"))
    frames = []

    for key in keys:
        ville_id = key.split("/")[-2]
        try:
            payload = json.loads(storage.read_bytes(key).decode("utf-8"))
            frame = clean_openmeteo_json(payload, ville_id)
            if not frame.empty:
                frames.append(frame)
        except Exception as exc:  # noqa: BLE001
            log.warning("Nettoyage impossible pour %s : %s", key, exc)

    if not frames:
        log.warning("Aucun fichier nettoyé.")
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    storage.put_bytes(SILVER_KEY, df.to_parquet(index=False), "application/octet-stream")
    log.info("Silver écrit : %s (%s lignes)", SILVER_KEY, len(df))
    return df


if __name__ == "__main__":
    clean_all()
