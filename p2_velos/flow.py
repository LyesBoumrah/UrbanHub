"""P2 — Orchestrateur streaming (collecte automatique toutes les 2-3 minutes).

Orchestration complète en Python pur (sans dépendance Prefect) :
1. Collecte CityBikes parallélisée -> Bronze MinIO (partitionnée par jour dt=YYYY-MM-DD/)
2. Nettoyage + règles qualité pures -> séparation velo_station / velo_releve
3. Écriture Silver Parquet dans MinIO (`silver/velos/dt=YYYY-MM-DD/...`)
4. Alerting automatique (taux de rejet, réseau indisponible, stations critiques)

Utilisation :
- Streaming continu (toutes les 3 min = 180 s) : python -m p2_velos.flow --interval 180
- Exécution unique (test / cron) :               python -m p2_velos.flow --once [--local] [--cities paris lyon]
- Backfill historique 5 ans :                    python -m p2_velos.flow --backfill --years 5

Responsable : P2.
"""

from __future__ import annotations

import argparse
import os
import signal
import time
from datetime import datetime, timezone
from typing import Any

from common.alerts import emit_alert
from common.log import get_logger
from common.period import DEFAULT_FETCH_DURATION, add_duration_arguments
from common.storage import get_storage
from p2_velos.backfill import DEFAULT_HISTORY_YEARS, run_backfill
from p2_velos.cleaning import (
    build_silver_streaming_key,
    clean_bronze_payload,
    get_bbox_map,
    save_silver_local,
    save_silver_parquet_to_storage,
)
from p2_velos.ingestion import IngestionResult, ingest_all_cities

logger = get_logger(__name__)

HIGH_REJECTION_RATE_THRESHOLD = 0.10
CRITICAL_CITY_RATIO_THRESHOLD = 0.25


def collect_bronze_task(
    cities: list[str] | None = None,
    core_only: bool = False,
    use_local: bool = False,
) -> list[IngestionResult]:
    """Collecte les snapshots temps réel CityBikes vers la couche bronze."""
    return ingest_all_cities(
        cities=cities,
        core_only=core_only,
        use_local_storage=use_local,
    )


def clean_and_validate_task(
    ingestion_results: list[IngestionResult],
    max_age_minutes: int | None = 30,
) -> dict[str, Any]:
    """Normalise et audite les relevés de chaque ville collectée."""
    bbox_map = get_bbox_map()
    now_utc = datetime.now(timezone.utc)

    all_stations: list[dict[str, Any]] = []
    all_releves: list[dict[str, Any]] = []
    city_summaries: list[dict[str, Any]] = []

    for res in ingestion_results:
        if not res.ok or res.payload is None:
            city_summaries.append(
                {
                    "ville_id": res.ville_id,
                    "network_id": res.network_id,
                    "ok": False,
                    "error": res.error,
                    "valid_count": 0,
                    "rejected_count": 0,
                    "rejection_rate": 1.0,
                    "critical_ratio": 0.0,
                }
            )
            continue

        stations, releves, report = clean_bronze_payload(
            res.payload,
            bbox=bbox_map.get(res.ville_id),
            now_utc=now_utc,
            max_age_minutes=max_age_minutes,
            ville_id=res.ville_id,
            network_id=res.network_id,
        )
        all_stations.extend(stations)
        all_releves.extend(releves)

        critical_count = sum(1 for r in releves if r["est_critique"] == 1)
        critical_ratio = round(critical_count / len(releves), 4) if releves else 0.0

        city_summaries.append(
            {
                "ville_id": res.ville_id,
                "network_id": res.network_id,
                "ok": True,
                "valid_count": report.valid_count,
                "rejected_count": report.rejected_count,
                "rejection_rate": report.rejection_rate,
                "reasons_summary": report.reasons_summary,
                "critical_count": critical_count,
                "critical_ratio": critical_ratio,
            }
        )

    return {
        "stations": all_stations,
        "releves": all_releves,
        "city_summaries": city_summaries,
        "processed_at_utc": now_utc.isoformat(),
    }


def write_silver_parquet_task(
    cleaned_batch: dict[str, Any],
    use_local: bool = False,
) -> dict[str, Any]:
    """Écrit les données nettoyées en fichiers Parquet dans la couche silver (MinIO ou local)."""
    stations = cleaned_batch["stations"]
    releves = cleaned_batch["releves"]

    if use_local:
        out_path = save_silver_local(stations, releves)
        return {
            "backend": "local",
            "releve_key": str(out_path),
            "station_key": None,
            "stations_count": len(stations),
            "releves_count": len(releves),
        }

    storage = get_storage(use_local=False)
    station_key = "silver/velos/velo_station_latest.parquet"
    releve_key = save_silver_parquet_to_storage(
        storage=storage,
        stations=stations,
        releves=releves,
        releve_key=build_silver_streaming_key(),
        station_key=station_key,
    )
    return {
        "backend": "minio_silver_parquet",
        "releve_key": releve_key,
        "station_key": station_key,
        "stations_count": len(stations),
        "releves_count": len(releves),
    }


def emit_alerts_task(
    city_summaries: list[dict[str, Any]],
    use_local: bool = False,
) -> int:
    """Émet les alertes pipeline en cas de panne API, fort taux de rejet ou tension réseau."""
    storage = get_storage(use_local=use_local)

    alerts_count = 0
    for summary in city_summaries:
        ville_id = summary["ville_id"]
        if not summary["ok"]:
            emit_alert(
                pipeline="p2_velos",
                niveau="CRITICAL",
                message=f"Collecte CityBikes en échec pour {ville_id}",
                ville_id=ville_id,
                details={"network_id": summary["network_id"], "error": summary.get("error")},
                storage=storage,
            )
            alerts_count += 1
            continue

        if summary["rejection_rate"] > HIGH_REJECTION_RATE_THRESHOLD:
            emit_alert(
                pipeline="p2_velos",
                niveau="WARNING",
                message=(
                    f"Taux de rejet qualité élevé ({summary['rejection_rate'] * 100:.1f} %) "
                    f"sur {ville_id}"
                ),
                ville_id=ville_id,
                details={
                    "rejected_count": summary["rejected_count"],
                    "reasons": summary.get("reasons_summary", {}),
                },
                storage=storage,
            )
            alerts_count += 1

        if summary["critical_ratio"] >= CRITICAL_CITY_RATIO_THRESHOLD:
            emit_alert(
                pipeline="p2_velos",
                niveau="WARNING",
                message=(
                    f"Tension forte sur le réseau {ville_id} : "
                    f"{summary['critical_ratio'] * 100:.1f} % des stations critiques"
                ),
                ville_id=ville_id,
                details={
                    "critical_count": summary.get("critical_count", 0),
                    "valid_count": summary["valid_count"],
                },
                storage=storage,
            )
            alerts_count += 1

    return alerts_count


def velos_streaming_flow(
    cities: list[str] | None = None,
    core_only: bool = False,
    use_local: bool = False,
    max_age_minutes: int | None = 30,
) -> dict[str, Any]:
    """Pipeline principal P2 : Bronze -> Nettoyage -> Silver Parquet -> Alertes."""
    ingestion_results = collect_bronze_task(cities=cities, core_only=core_only, use_local=use_local)
    cleaned_batch = clean_and_validate_task(ingestion_results, max_age_minutes=max_age_minutes)
    silver_ref = write_silver_parquet_task(cleaned_batch, use_local=use_local)
    alerts_count = emit_alerts_task(cleaned_batch["city_summaries"], use_local=use_local)

    summary = {
        "processed_at_utc": cleaned_batch["processed_at_utc"],
        "cities_total": len(ingestion_results),
        "cities_ok": sum(1 for r in ingestion_results if r.ok),
        "silver": silver_ref,
        "alerts_emitted": alerts_count,
    }
    logger.info("Cycle P2 terminé : %s", summary)
    return summary


def velos_backfill_flow(
    years: int = DEFAULT_HISTORY_YEARS,
    cities: list[str] | None = None,
    core_only: bool = False,
    freq: str | None = "1h",
    use_local: bool = False,
    max_months_per_city: int | None = None,
    max_workers: int = 5,
    *,
    days: int | None = None,
    weeks: int | None = None,
    months: int | None = None,
    duration: str | None = None,
) -> dict[str, Any]:
    """Pipeline de backfill historique parallélisé par année (durée configurable)."""
    return run_backfill(
        years=years,
        cities=cities,
        core_only=core_only,
        freq=freq,
        use_local=use_local,
        max_months_per_city=max_months_per_city,
        max_workers=max_workers,
        days=days,
        weeks=weeks,
        months=months,
        duration=duration,
    )


def run_loop(
    interval_seconds: int = 180,
    cities: list[str] | None = None,
    core_only: bool = False,
    use_local: bool = False,
    max_cycles: int | None = None,
) -> None:
    """Boucle streaming Python exécutant la collecte toutes les 2-3 minutes."""
    stop_requested = False

    def _handle_signal(signum: int, frame: Any) -> None:
        del signum, frame
        nonlocal stop_requested
        stop_requested = True
        logger.info("Arrêt demandé, fin après le cycle en cours...")

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle_signal)
        except (ValueError, OSError):
            pass

    cycle = 0
    logger.info("Démarrage worker streaming P2 (intervalle = %d s)", interval_seconds)
    while not stop_requested and (max_cycles is None or cycle < max_cycles):
        cycle += 1
        started = time.monotonic()
        try:
            velos_streaming_flow(cities=cities, core_only=core_only, use_local=use_local)
        except Exception as exc:
            logger.exception("Erreur non interceptée sur le cycle %d : %s", cycle, exc)

        if stop_requested or (max_cycles is not None and cycle >= max_cycles):
            break
        elapsed = time.monotonic() - started
        sleep_for = max(1.0, interval_seconds - elapsed)
        logger.info("Attente %.1f s avant le prochain cycle...", sleep_for)
        deadline = time.monotonic() + sleep_for
        while not stop_requested and time.monotonic() < deadline:
            time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))


def main() -> None:
    parser = argparse.ArgumentParser(description="P2 — Pipeline streaming mobilité urbaine (CityBikes)")
    parser.add_argument("--cities", nargs="*", help="Filtrer sur certains ville_id")
    parser.add_argument("--core-only", action="store_true", help="Limiter aux 5 villes cœur UrbanHub")
    parser.add_argument("--local", action="store_true", help="Mode local (data/ bronze + silver Parquet)")
    parser.add_argument(
        "--interval",
        type=int,
        default=180,
        help="Intervalle de collecte en secondes (défaut: 180 s = 3 min, <= 240 req/h sur 12 villes)",
    )
    parser.add_argument("--once", action="store_true", help="Exécuter un seul cycle puis quitter")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Lancer le backfill historique depuis data.citybik.es (durée configurable)",
    )
    add_duration_arguments(parser, default_help_suffix=f"backfill, défaut : {DEFAULT_HISTORY_YEARS} ans")
    parser.add_argument(
        "--max-months-per-city",
        type=int,
        default=None,
        help="Limiter le backfill aux N derniers mois par ville",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=5,
        help="Nombre de threads parallèles par année pour --backfill (défaut: 5)",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=None,
        help="Nombre maximal de cycles en mode boucle (défaut: infini)",
    )
    args = parser.parse_args()

    if args.backfill:
        has_cli = any(v is not None for v in (args.duration, args.days, args.weeks, args.months, args.years))
        default_dur = os.getenv("FETCH_DURATION", "").strip() or DEFAULT_FETCH_DURATION
        velos_backfill_flow(
            years=args.years if args.years is not None else DEFAULT_HISTORY_YEARS,
            cities=args.cities,
            core_only=args.core_only,
            use_local=args.local,
            max_months_per_city=args.max_months_per_city,
            max_workers=args.max_workers,
            days=args.days,
            weeks=args.weeks,
            months=args.months,
            duration=args.duration if has_cli else default_dur,
        )
    elif args.once:
        velos_streaming_flow(cities=args.cities, core_only=args.core_only, use_local=args.local)
    else:
        run_loop(
            interval_seconds=args.interval,
            cities=args.cities,
            core_only=args.core_only,
            use_local=args.local,
            max_cycles=args.max_cycles,
        )


if __name__ == "__main__":
    main()
