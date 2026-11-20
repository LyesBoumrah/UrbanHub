"""P1 — Extract : archive horaire Open-Meteo des 5 dernières années → couche bronze (Silo).

Source : https://archive-api.open-meteo.com/v1/archive (réanalyse ERA5, sans clé)
Une requête par ville couvre toute la période ; la réponse JSON est copiée TELLE QUELLE :
    bronze/meteo/ville=<ville_id>/<début>_<fin>.json
+ un manifeste par exécution : bronze/meteo/_manifests/<horodatage>_<batch>.json

Variables horaires retenues (la visibilité est absente de l'archive à 100 % : retirée) :
    temperature_2m, relative_humidity_2m, precipitation, pressure_msl,
    cloud_cover, wind_speed_10m, wind_direction_10m

Idempotent : une ville déjà présente pour la même période n'est pas retéléchargée.
Open-Meteo pondère le quota selon le volume demandé : on reste à 2 requêtes simultanées,
avec une pause entre deux départs, et on respecte l'en-tête Retry-After en cas de 429.

Responsable : P1 (version extraction seule, ajoutée pour l'ETL local).
"""
from __future__ import annotations

import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone

import requests

from common.log import get_logger

OPENMETEO_URL = "https://archive-api.open-meteo.com/v1/archive"
BRONZE_PREFIX = "bronze/meteo"
HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "pressure_msl",
    "cloud_cover",
    "wind_speed_10m",
    "wind_direction_10m",
]

log = get_logger("p1.extract")


# ---------------------------------------------------------------- fonctions pures (testées)

def bronze_key(ville_id: str, start: date, end: date) -> str:
    return f"{BRONZE_PREFIX}/ville={ville_id}/{start.isoformat()}_{end.isoformat()}.json"


def request_params(ville: dict, start: date, end: date) -> dict:
    return {
        "latitude": ville["lat"],
        "longitude": ville["lon"],
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "UTC",  # tout est stocké et traité en UTC
    }


def validate_payload(raw: bytes) -> int:
    """Vérifie qu'une réponse est exploitable ; renvoie le nombre d'heures. Lève ValueError sinon."""
    payload = json.loads(raw)
    times = (payload.get("hourly") or {}).get("time") or []
    if not times:
        raise ValueError(f"réponse sans données horaires : {str(payload)[:200]}")
    return len(times)


# ---------------------------------------------------------------- appels HTTP

def fetch_city(ville: dict, start: date, end: date, user_agent: str, timeout: int = 180, retries: int = 4) -> bytes:
    """Télécharge l'archive d'une ville ; renvoie les octets bruts de la réponse."""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(OPENMETEO_URL, params=request_params(ville, start, end),
                                headers={"User-Agent": user_agent}, timeout=timeout)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 60))
                log.warning("429 Open-Meteo (%s), pause de %ss", ville["ville_id"], wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            validate_payload(resp.content)
            return resp.content
        except (requests.RequestException, ValueError) as exc:
            if attempt == retries:
                raise
            wait = 5 * attempt
            log.warning("Échec Open-Meteo %s (%s), nouvel essai dans %ss", ville["ville_id"], exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"Open-Meteo : abandon pour {ville['ville_id']}")


# ---------------------------------------------------------------- extraction

def extract(storage, villes, start: date, end: date, user_agent: str, fetch=fetch_city,
            workers: int = 2, delay_s: float = 2.0) -> dict:
    """Copie dans le bronze l'archive horaire de chaque ville. Renvoie un bilan (dict)."""
    batch_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    t0 = time.monotonic()
    already = set(storage.list(f"{BRONZE_PREFIX}/"))
    todo = [v for v in villes if bronze_key(v["ville_id"], start, end) not in already]
    log.info("Extraction Open-Meteo du %s au %s : %d villes, %d déjà présentes, %d à télécharger",
             start, end, len(villes), len(villes) - len(todo), len(todo))

    per_city, errors, n_bytes = {}, [], 0
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="meteo") as pool:
        futures = {}
        for v in todo:
            futures[pool.submit(fetch, v, start, end, user_agent)] = v
            time.sleep(delay_s)  # étale les départs (quota Open-Meteo pondéré)
        for fut in as_completed(futures):
            v = futures[fut]
            try:
                raw = fut.result()
                key = bronze_key(v["ville_id"], start, end)
                storage.put_bytes(key, raw, content_type="application/json")
                n_bytes += len(raw)
                per_city[v["ville_id"]] = {"key": key, "hours": validate_payload(raw), "bytes": len(raw)}
                log.info("Bronze écrit : %s (%d heures)", key, per_city[v["ville_id"]]["hours"])
            except Exception as exc:  # une ville en échec n'arrête pas les autres
                errors.append(v["ville_id"])
                log.error("Ville %s en échec : %s", v["ville_id"], exc)

    stats = {
        "source": "Open-Meteo Archive API",
        "batch_id": batch_id,
        "started_at": started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "period": {"start": start.isoformat(), "end": end.isoformat()},
        "hourly_variables": HOURLY_VARS,
        "files_downloaded": len(per_city),
        "files_skipped": len(villes) - len(todo),
        "bytes_downloaded": n_bytes,
        "errors": errors,
        "duration_s": round(time.monotonic() - t0, 1),
        "villes": per_city,
    }
    storage.put_json(f"{BRONZE_PREFIX}/_manifests/{started_at:%Y%m%dT%H%M%SZ}_{batch_id[:8]}.json", stats)
    log.info("Extraction Open-Meteo terminée : %d fichiers (%.1f Mo), %d déjà présents, %d erreurs, %ss",
             stats["files_downloaded"], n_bytes / 1e6, stats["files_skipped"], len(errors), stats["duration_s"])
    return stats
