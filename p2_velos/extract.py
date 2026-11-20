"""P2 — Extract : archive mensuelle CityBikes des 5 dernières années → couche bronze (Silo).

Source : https://data.citybik.es/dumps/by-network/<année>/<AAAAMM>-<réseau>-stats.parquet
(fichiers Parquet mensuels publiés par CityBikes, licence ODbL ; disponibles depuis 11/2024).
Chaque fichier est copié TEL QUEL, avec la convention de clés du module P2 d'origine :
    bronze/velos_historique/<ville_id>/<année>/<AAAAMM>-<réseau>-stats.parquet
+ un manifeste par exécution : bronze/velos_historique/_manifests/<horodatage>_<batch>.json

Contrairement à p2_velos/backfill.py (qui télécharge, nettoie et charge dans la foulée),
ce module ne fait QUE l'extraction : le nettoyage relève de l'étape Transform.

Idempotent : un fichier déjà présent dans le bronze n'est pas retéléchargé.
Attribution : « Bike-share data by CityBikes contributors, https://data.citybik.es (ODbL v1.0) ».

Responsable : P2 (version extraction seule, ajoutée pour l'ETL local).
"""
from __future__ import annotations

import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone

import requests

from common.log import get_logger
from common.period import year_month

BRONZE_PREFIX = "bronze/velos_historique"
DEFAULT_WORKERS = 6

log = get_logger("p2.extract")


# ---------------------------------------------------------------- fonctions pures (testées)

def bronze_key(dump) -> str:
    return f"{BRONZE_PREFIX}/{dump.ville_id}/{dump.year}/{dump.filename}"


def in_period(dumps, start: date, end: date):
    """Garde les dumps dont le mois est dans [start, end] (mois complets)."""
    lo, hi = year_month(start), year_month(end)
    return sorted((d for d in dumps if lo <= d.ym <= hi), key=lambda d: (d.ym, d.ville_id, d.filename))


def months_by_city(dumps) -> dict:
    out = defaultdict(set)
    for d in dumps:
        out[d.ville_id].add(d.ym)
    return {k: sorted(v) for k, v in out.items()}


# ---------------------------------------------------------------- appels HTTP

def list_dumps(nb_years: int):
    """Inventaire des dumps disponibles (réutilise le listage du module P2 d'origine)."""
    from p2_velos.backfill import list_available_dumps

    return list_available_dumps(years=nb_years)


def download(dump, user_agent: str, timeout: int = 300, retries: int = 4) -> bytes:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(dump.url, headers={"User-Agent": user_agent}, timeout=timeout)
            resp.raise_for_status()
            data = resp.content
            if dump.size_bytes and len(data) != dump.size_bytes:
                raise ValueError(f"taille reçue {len(data)} ≠ taille annoncée {dump.size_bytes}")
            return data
        except (requests.RequestException, ValueError) as exc:
            if attempt == retries:
                raise
            wait = 2 ** attempt
            log.warning("Échec %s (%s), nouvel essai dans %ss", dump.filename, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"abandon : {dump.url}")


# ---------------------------------------------------------------- extraction

def extract(storage, dumps, start: date, end: date, user_agent: str, fetch=download,
            workers: int = DEFAULT_WORKERS) -> dict:
    """Copie dans le bronze tous les dumps de la période. Renvoie un bilan (dict)."""
    batch_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    t0 = time.monotonic()
    selected = in_period(dumps, start, end)
    already = set(storage.list(f"{BRONZE_PREFIX}/"))
    todo = [d for d in selected if bronze_key(d) not in already]
    log.info("Extraction CityBikes %s → %s : %d fichiers dans la période (%.0f Mo), %d déjà présents, %d à télécharger",
             year_month(start), year_month(end), len(selected), sum(d.size_bytes for d in selected) / 1e6,
             len(selected) - len(todo), len(todo))

    n_done, n_bytes, errors = 0, 0, []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="velos") as pool:
        futures = {pool.submit(fetch, d, user_agent): d for d in todo}
        for fut in as_completed(futures):
            d = futures[fut]
            try:
                data = fut.result()
                storage.put_bytes(bronze_key(d), data, content_type="application/vnd.apache.parquet")
                n_done += 1
                n_bytes += len(data)
                if n_done % 25 == 0:
                    log.info("… %d / %d fichiers (%.0f Mo)", n_done, len(todo), n_bytes / 1e6)
            except Exception as exc:
                errors.append(d.filename)
                log.error("Échec %s : %s", d.filename, exc)

    coverage = months_by_city(selected)
    stats = {
        "source": "CityBikes dumps (data.citybik.es, ODbL v1.0)",
        "batch_id": batch_id,
        "started_at": started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "period": {"start": start.isoformat(), "end": end.isoformat()},
        "files_available": len(selected),
        "files_downloaded": n_done,
        "files_skipped": len(selected) - len(todo),
        "bytes_downloaded": n_bytes,
        "errors": errors,
        "duration_s": round(time.monotonic() - t0, 1),
        "villes": {v: {"months": len(m), "first": m[0], "last": m[-1]} for v, m in coverage.items()},
    }
    storage.put_json(f"{BRONZE_PREFIX}/_manifests/{started_at:%Y%m%dT%H%M%SZ}_{batch_id[:8]}.json", stats)
    log.info("Extraction CityBikes terminée : %d fichiers (%.0f Mo), %d déjà présents, %d erreurs, %ss",
             n_done, n_bytes / 1e6, stats["files_skipped"], len(errors), stats["duration_s"])
    return stats
