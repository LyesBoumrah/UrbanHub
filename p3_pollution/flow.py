"""P3 — Orchestrateur du flux IoT pollution OpenAQ v3 (100 % Data Lake MinIO / local).

Étapes du pipeline :
  1. Ingestion IoT (`collect_latest` horaire ou `backfill` historique) -> Bronze (`bronze/openaq/...`)
  2. Nettoyage & règles qualité -> Silver Parquet (`silver/pollution/air_mesure.parquet`,
     `silver/pollution/air_capteur.parquet`)
  3. Alerting automatique dans le Data Lake (`logs/alertes/`) : sites KO, capteurs muets,
     taux de rejet qualité élevé.

Usage :
  python -m p3_pollution.flow --once [--local]
  python -m p3_pollution.flow --loop --interval 3600
  python -m p3_pollution.flow --backfill --days 7
  python -m p3_pollution.flow --backfill --years 5
  python -m p3_pollution.flow --skip-ingest [--local]

Responsable : P3.
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv

from common.alerts import emit_alert
from common.log import get_logger
from common.period import DEFAULT_FETCH_DURATION, add_duration_arguments, compute_window
from common.storage import get_storage
from p3_pollution.cleaning import clean_all
from p3_pollution.ingestion import (
    DEFAULT_WORKERS,
    ROOT,
    backfill,
    collect_latest,
    load_locations,
)
from p3_pollution.openaq_client import OpenAQClient
from p3_pollution.storage import storage_from_env

log = get_logger("p3.flow")

HIGH_REJECTION_PCT_THRESHOLD = 15.0


def emit_alerts_task(
    ingest_stats: dict[str, Any] | None,
    clean_stats: dict[str, Any],
    use_local: bool = False,
) -> int:
    """Émet les alertes pipeline P3 (sites KO, capteurs muets, taux de rejet qualité)."""
    lake_storage = get_storage(use_local=use_local)
    alerts = 0

    if ingest_stats:
        sites_ko = ingest_stats.get("sites_ko") or []
        if sites_ko:
            emit_alert(
                pipeline="p3_pollution",
                niveau="WARNING",
                message=f"{len(sites_ko)} sites OpenAQ en échec lors de la collecte IoT",
                details={"sites_ko": sites_ko[:30]},
                storage=lake_storage,
            )
            alerts += 1
        silent = ingest_stats.get("silent_sensors") or []
        if len(silent) > 10:
            emit_alert(
                pipeline="p3_pollution",
                niveau="INFO",
                message=f"{len(silent)} capteurs OpenAQ muets (> 30 h sans mesure)",
                details={"silent_count": len(silent), "sample_sensors": silent[:25]},
                storage=lake_storage,
            )
            alerts += 1

    q = clean_stats.get("quality") or {}
    if q.get("total", 0) > 0 and (100.0 - q.get("taux_validite", 100.0)) > HIGH_REJECTION_PCT_THRESHOLD:
        emit_alert(
            pipeline="p3_pollution",
            niveau="WARNING",
            message=f"Taux de rejet qualité élevé en P3 : {100.0 - q['taux_validite']:.1f} %",
            details=q,
            storage=lake_storage,
        )
        alerts += 1

    return alerts


def p3_pollution_flow(
    mode: str = "latest",
    days: int | None = None,
    weeks: int | None = None,
    months: int | None = None,
    years: int | None = None,
    duration: str | None = None,
    workers: int = DEFAULT_WORKERS,
    use_local: bool = False,
    skip_ingest: bool = False,
) -> dict[str, Any]:
    """Exécute un cycle complet du pipeline IoT pollution P3 (Bronze -> Silver Parquet -> Alertes)."""
    t0 = time.monotonic()
    load_dotenv(ROOT / ".env")
    storage = storage_from_env(use_local=True if use_local else None)
    ingest_stats: dict[str, Any] | None = None

    if not skip_ingest:
        api_key = os.environ.get("OPENAQ_API_KEY", "").strip()
        if not api_key:
            log.warning("OPENAQ_API_KEY absente : étape d'ingestion API sautée, nettoyage du bronze existant.")
        else:
            client = OpenAQClient(
                api_key,
                user_agent=os.environ.get("HTTP_USER_AGENT", "UrbanHub-school-project/0.1"),
                workers=workers,
            )
            locations = load_locations()
            if mode == "backfill":
                now = datetime.now(timezone.utc)
                has_dur = any(v is not None for v in (duration, days, weeks, months, years))
                eff_dur = duration if has_dur else (os.getenv("FETCH_DURATION", "").strip() or DEFAULT_FETCH_DURATION)
                start, now = compute_window(
                    days=days,
                    weeks=weeks,
                    months=months,
                    years=years,
                    duration=eff_dur if not any(v is not None for v in (days, weeks, months, years)) else None,
                    now=now,
                )
                ingest_stats = backfill(client, storage, locations, start, now, workers=workers)
            else:
                ingest_stats = collect_latest(client, storage, locations, workers=workers)

    clean_stats = clean_all(storage=storage, use_local=True if use_local else None)
    alerts = emit_alerts_task(ingest_stats, clean_stats, use_local=use_local)

    summary = {
        "mode": mode,
        "duration_s": round(time.monotonic() - t0, 1),
        "ingest": ingest_stats,
        "clean": clean_stats,
        "alerts": alerts,
    }
    log.info("Cycle P3 terminé : %s", summary)
    return summary


def run_iot_loop(interval_s: int = 3600, workers: int = DEFAULT_WORKERS, use_local: bool = False) -> None:
    """Boucle d'ingestion régulière simulant un flux IoT continu."""
    log.info("Démarrage du worker IoT P3 (intervalle = %d s)", interval_s)
    while True:
        started = time.monotonic()
        try:
            p3_pollution_flow(mode="latest", workers=workers, use_local=use_local)
        except Exception as exc:  # noqa: BLE001
            log.exception("Erreur sur le cycle IoT P3 : %s", exc)
        elapsed = time.monotonic() - started
        wait = max(5.0, interval_s - elapsed)
        log.info("Prochain cycle IoT P3 dans %.0f s", wait)
        time.sleep(wait)


def main() -> None:
    parser = argparse.ArgumentParser(description="P3 — Pipeline IoT pollution urbaine (OpenAQ v3)")
    parser.add_argument("--once", action="store_true", help="Exécuter un cycle unique puis quitter")
    parser.add_argument("--loop", action="store_true", help="Exécuter en continu (flux IoT régulier)")
    parser.add_argument(
        "--interval", type=int, default=3600, help="Intervalle en secondes en mode --loop (défaut 3600)"
    )
    parser.add_argument("--backfill", action="store_true", help="Mode historique (/sensors/{id}/hours)")
    add_duration_arguments(parser, default_help_suffix=f"backfill, défaut : FETCH_DURATION ou {DEFAULT_FETCH_DURATION}")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--local", action="store_true", help="Stockage local (data/) au lieu de MinIO")
    parser.add_argument("--skip-ingest", action="store_true", help="Nettoyer le bronze existant sans appel API")
    args = parser.parse_args()

    mode = "backfill" if args.backfill else "latest"
    if args.loop:
        run_iot_loop(interval_s=args.interval, workers=args.workers, use_local=args.local)
    else:
        p3_pollution_flow(
            mode=mode,
            days=args.days,
            weeks=args.weeks,
            months=args.months,
            years=args.years,
            duration=args.duration,
            workers=args.workers,
            use_local=args.local,
            skip_ingest=args.skip_ingest,
        )


if __name__ == "__main__":
    main()
