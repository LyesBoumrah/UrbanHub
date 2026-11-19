"""P1 — Téléchargement Open-Meteo (archive horaire, 5 ans) vers le bronze MinIO.

Responsable : P1.
"""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

import requests

from common.config import load_villes, settings
from common.log import setup_logging
from common.storage import Storage

log = setup_logging(__name__)

OPENMETEO_URL = "https://archive-api.open-meteo.com/v1/archive"
BRONZE_PREFIX = "p1_meteo/raw/openmeteo"

HOURLY_VARS = [
    "temperature_2m",
    "precipitation",
    "pressure_msl",
    "wind_speed_10m",
    "wind_direction_10m",
    "visibility",
]


def default_period(nb_years: int = 5) -> tuple[str, str]:
    """Période des n dernières années complètes (ex. 2021-01-01 -> 2025-12-31)."""
    today = date.today()
    start = date(today.year - nb_years, 1, 1)
    end = date(today.year - 1, 12, 31)
    return start.isoformat(), end.isoformat()


def bronze_key(ville_id: str, start_date: str, end_date: str) -> str:
    return f"{BRONZE_PREFIX}/{ville_id}/{start_date}_{end_date}.json"


def fetch_ville_meteo(
    ville: dict,
    start_date: str,
    end_date: str,
    storage: Storage,
    timeout: int = 180,
    retries: int = 3,
) -> str | None:
    """Récupère 5 ans de données horaires pour une ville et les écrit dans bronze."""
    params = {
        "latitude": ville["lat"],
        "longitude": ville["lon"],
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "UTC",
    }
    key = bronze_key(ville["ville_id"], start_date, end_date)

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(
                OPENMETEO_URL,
                params=params,
                headers={"User-Agent": settings.http_user_agent},
                timeout=timeout,
            )
            if response.status_code == 429:
                wait = int(response.headers.get("Retry-After", 15))
                log.warning(
                    "429 Open-Meteo %s, attente %ss (tentative %s/%s)",
                    ville["ville_id"], wait, attempt, retries,
                )
                time.sleep(wait)
                continue

            response.raise_for_status()
            payload = response.json()
            if "hourly" not in payload or not payload["hourly"].get("time"):
                log.warning("Open-Meteo %s : réponse vide", ville["ville_id"])
                return None

            storage.put_bytes(
                key,
                json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                "application/json",
            )
            log.info(
                "Bronze écrit : %s (%s heures)",
                key,
                len(payload["hourly"]["time"]),
            )
            return key
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "Échec Open-Meteo %s (tentative %s/%s) : %s",
                ville["ville_id"], attempt, retries, exc,
            )
            time.sleep(5)

    return None


def ingest(
    nb_years: int = 5,
    max_workers: int = 2,
    limit_villes: int | None = None,
    skip_existing: bool = True,
    delay_seconds: float = 1.5,
) -> list[str]:
    """Télécharge en parallèle les villes sur les n dernières années.

    Reprend là où l'ingestion s'est arrêtée (skip_existing=True).
    """
    storage = Storage()
    villes = load_villes()
    if limit_villes:
        villes = villes[:limit_villes]

    start_date, end_date = default_period(nb_years)
    log.info("Villes : %s | période : %s -> %s", len(villes), start_date, end_date)

    written: list[str] = []
    tasks = []
    for ville in villes:
        key = bronze_key(ville["ville_id"], start_date, end_date)
        if skip_existing and storage.exists(key):
            log.info("Déjà présent : %s", key)
            written.append(key)
            continue
        tasks.append((ville, start_date, end_date))

    if not tasks:
        log.info("Tous les fichiers sont déjà présents.")
        return written

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for ville, start, end in tasks:
            futures[executor.submit(fetch_ville_meteo, ville, start, end, storage)] = ville
            time.sleep(delay_seconds)

        for future in as_completed(futures):
            key = future.result()
            if key:
                written.append(key)

    log.info("Ingestion terminée : %s fichiers / %s villes", len(written), len(villes))
    return written


if __name__ == "__main__":
    ingest()
