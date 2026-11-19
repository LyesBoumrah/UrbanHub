"""P2 — Collecte CityBikes toutes les 2-3 min (réseaux de ref_ville) vers le bronze MinIO.

Responsable : P2.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common.config import get_settings, load_ref_ville
from common.log import get_logger
from common.storage import LocalBronzeStorage, MinIOStorage, get_storage

logger = get_logger(__name__)

CITYBIKES_API_BASE = "https://api.citybik.es/v2/networks"
DEFAULT_TIMEOUT_SECONDS = 15


@dataclass(frozen=True)
class IngestionResult:
    """Résultat de la collecte bronze pour une ville."""

    ville_id: str
    network_id: str
    bronze_key: str | None
    station_count: int
    collected_at_utc: datetime
    payload: dict[str, Any] | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.payload is not None


def build_http_session(user_agent: str | None = None) -> requests.Session:
    """Crée une session HTTP avec retry exponentiel sur les erreurs transitoires."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": user_agent or get_settings().http_user_agent})
    return session


def build_bronze_key(ville_id: str, network_id: str, collected_at_utc: datetime) -> str:
    """Construit la clé d'objet immuable en couche bronze partitionnée par date/heure UTC."""
    ts_utc = collected_at_utc.astimezone(timezone.utc)
    date_part = ts_utc.strftime("%Y-%m-%d")
    hour_part = ts_utc.strftime("%H")
    stamp = ts_utc.strftime("%Y%m%dT%H%M%SZ")
    return f"bronze/velos/{ville_id}/dt={date_part}/hr={hour_part}/{network_id}_{stamp}.json"


def fetch_network_snapshot(
    network_id: str,
    session: requests.Session | None = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Récupère le snapshot temps réel complet d'un réseau CityBikes v2."""
    http = session or build_http_session()
    url = f"{CITYBIKES_API_BASE}/{network_id}"
    response = http.get(url, timeout=timeout)
    response.raise_for_status()
    data: dict[str, Any] = response.json()
    if "network" not in data or "stations" not in data["network"]:
        raise ValueError(f"Réponse CityBikes invalide pour {network_id}: clé network.stations absente")
    return data


def ingest_city_snapshot(
    ville_entry: dict[str, Any],
    storage: MinIOStorage | LocalBronzeStorage | None = None,
    session: requests.Session | None = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> IngestionResult:
    """Collecte un réseau CityBikes et dépose le JSON brut immuable en couche bronze."""
    ville_id = ville_entry["ville_id"]
    cb_cfg = ville_entry.get("citybikes") or {}
    network_id = cb_cfg.get("network_id", "")
    collected_at = datetime.now(timezone.utc)

    if not network_id:
        return IngestionResult(
            ville_id=ville_id,
            network_id="",
            bronze_key=None,
            station_count=0,
            collected_at_utc=collected_at,
            error="Aucun network_id CityBikes configuré",
        )

    try:
        raw_payload = fetch_network_snapshot(network_id, session=session, timeout=timeout)
        stations = raw_payload.get("network", {}).get("stations", [])
        envelope = {
            "meta": {
                "ville_id": ville_id,
                "ville_nom": ville_entry.get("nom", ville_id),
                "network_id": network_id,
                "source": "citybikes_v2",
                "collected_at_utc": collected_at.isoformat(),
                "station_count": len(stations),
            },
            "payload": raw_payload,
        }

        key = build_bronze_key(ville_id, network_id, collected_at)
        if storage is not None:
            storage.put_json(key, envelope)

        logger.info(
            "[%s] Collecte bronze OK (%s) : %d stations -> %s",
            ville_id,
            network_id,
            len(stations),
            key,
        )
        return IngestionResult(
            ville_id=ville_id,
            network_id=network_id,
            bronze_key=key,
            station_count=len(stations),
            collected_at_utc=collected_at,
            payload=envelope,
        )
    except Exception as exc:
        logger.error("[%s] Échec collecte CityBikes (%s) : %s", ville_id, network_id, exc)
        return IngestionResult(
            ville_id=ville_id,
            network_id=network_id,
            bronze_key=None,
            station_count=0,
            collected_at_utc=collected_at,
            error=str(exc),
        )


def ingest_all_cities(
    cities: list[str] | None = None,
    core_only: bool = False,
    use_local_storage: bool = False,
    storage: MinIOStorage | LocalBronzeStorage | None = None,
    pause_seconds: float = 0.0,
    max_workers: int = 4,
) -> list[IngestionResult]:
    """Collecte en parallèle les snapshots CityBikes v2 pour toutes les villes sélectionnées."""
    del pause_seconds  # conservé pour rétrocompatibilité de signature
    ref_villes = load_ref_ville(core_only=core_only)
    if cities:
        wanted = {c.strip().lower() for c in cities}
        ref_villes = [v for v in ref_villes if v["ville_id"].lower() in wanted]

    if not ref_villes:
        return []

    def _collect_one(ville: dict[str, Any]) -> IngestionResult:
        worker_storage = storage if storage is not None else get_storage(use_local=use_local_storage)
        worker_session = build_http_session()
        return ingest_city_snapshot(ville, storage=worker_storage, session=worker_session)

    workers = max(1, min(int(max_workers), len(ref_villes)))
    results_by_idx: dict[int, IngestionResult] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_collect_one, v): idx for idx, v in enumerate(ref_villes)}
        for fut in as_completed(futures):
            idx = futures[fut]
            results_by_idx[idx] = fut.result()

    results = [results_by_idx[i] for i in range(len(ref_villes))]
    ok_count = sum(1 for r in results if r.ok)
    total_stations = sum(r.station_count for r in results if r.ok)
    logger.info(
        "Cycle d'ingestion parallèle (%d workers) terminé : %d/%d villes OK (%d stations collectées)",
        workers,
        ok_count,
        len(results),
        total_stations,
    )
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="P2 — Collecte CityBikes vers la couche bronze")
    parser.add_argument("--cities", nargs="*", help="Liste de ville_id à collecter (défaut: toutes)")
    parser.add_argument("--core-only", action="store_true", help="Limiter aux 5 villes cœur UrbanHub")
    parser.add_argument("--local", action="store_true", help="Écrire en bronze local (data/) au lieu de MinIO")
    parser.add_argument("--max-workers", type=int, default=4, help="Requêtes HTTP parallèles (défaut: 4)")
    args = parser.parse_args()

    ingest_all_cities(
        cities=args.cities,
        core_only=args.core_only,
        use_local_storage=args.local,
        max_workers=args.max_workers,
    )


if __name__ == "__main__":
    main()
