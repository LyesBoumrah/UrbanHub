"""P1 — Pipeline batch météo Open-Meteo (5 ans + données récentes) 100 % Data Lake.

Étapes :
  1. `ingest_task`  : téléchargement parallèle des villes françaises sur 5 ans jusqu'à aujourd'hui
                      (`archive-api.open-meteo.com` + `api.open-meteo.com/v1/forecast?past_days=14`) -> Bronze
  2. `clean_task`   : nettoyage, conversion d'unités (`km/h` -> `m/s`), timestamps UTC -> Silver Parquet
  3. `quality_task` : vérification des bornes physiques et résumé qualité
  4. `alerts_task`  : journalisation des anomalies dans le Data Lake (`logs/alertes/`)

Usage :
  python -m p1_meteo.flow                    # pipeline complet (MinIO)
  python -m p1_meteo.flow --local            # mode local (data/)
  python -m p1_meteo.flow --skip-ingest      # nettoyer le bronze déjà présent -> Silver Parquet
  python -m p1_meteo.flow --daily            # exécution quotidienne continue

Responsable : P1.
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from common.alerts import emit_alert
from common.log import setup_logging
from common.period import DEFAULT_FETCH_DURATION, add_duration_arguments
from common.storage import get_storage
from p1_meteo.cleaning import clean_all
from p1_meteo.ingestion import DEFAULT_WORKERS, DEFAULT_YEARS, ingest
from p1_meteo.quality import check_quality, quality_summary

log = setup_logging(__name__)

MIN_VALIDITY_PCT = 90.0


def ingest_task(
    nb_years: int = DEFAULT_YEARS,
    max_workers: int = DEFAULT_WORKERS,
    limit_villes: int | None = None,
    skip_existing: bool = True,
    use_local: bool = False,
    *,
    days: int | None = None,
    weeks: int | None = None,
    months: int | None = None,
    duration: str | None = None,
) -> list[str]:
    return ingest(
        nb_years=nb_years,
        max_workers=max_workers,
        limit_villes=limit_villes,
        skip_existing=skip_existing,
        use_local=use_local,
        days=days,
        weeks=weeks,
        months=months,
        duration=duration,
    )


def clean_task(use_local: bool = False) -> pd.DataFrame:
    return clean_all(use_local=use_local)


def quality_task(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    checked = check_quality(df)
    summary = quality_summary(checked)
    log.info("Qualité P1 : %s", summary)
    return checked, summary


def alerts_task(summary: dict[str, Any], use_local: bool = False) -> int:
    storage = get_storage(use_local=use_local)
    alerts = 0
    if summary.get("total", 0) == 0:
        emit_alert("p1_meteo", "WARNING", "Aucune observation météo nettoyée en P1", storage=storage)
        alerts += 1
    elif summary.get("taux_validite", 100.0) < MIN_VALIDITY_PCT:
        emit_alert(
            "p1_meteo",
            "WARNING",
            f"Taux de validité P1 faible : {summary['taux_validite']} %",
            details=summary,
            storage=storage,
        )
        alerts += 1
    return alerts


def p1_meteo_flow(
    nb_years: int = DEFAULT_YEARS,
    max_workers: int = DEFAULT_WORKERS,
    limit_villes: int | None = None,
    use_local: bool = False,
    skip_ingest: bool = False,
    force_refresh: bool = False,
    *,
    days: int | None = None,
    weeks: int | None = None,
    months: int | None = None,
    duration: str | None = None,
) -> dict[str, Any]:
    """Pipeline complet : téléchargement parallèle (Bronze) → nettoyage (Silver Parquet) → qualité → alertes."""
    t0 = time.monotonic()
    written = []
    if not skip_ingest:
        written = ingest_task(
            nb_years=nb_years,
            max_workers=max_workers,
            limit_villes=limit_villes,
            skip_existing=not force_refresh,
            use_local=use_local,
            days=days,
            weeks=weeks,
            months=months,
            duration=duration,
        )
    df = clean_task(use_local=use_local)
    _, summary = quality_task(df)
    n_alerts = alerts_task(summary, use_local=use_local)

    result = {
        "duration_s": round(time.monotonic() - t0, 1),
        "bronze_files": len(written),
        "silver_rows": len(df),
        "quality": summary,
        "alerts": n_alerts,
    }
    log.info("Pipeline P1 terminé : %s", result)
    return result


def run_daily(**kwargs: Any) -> None:
    """Relance quotidienne du pipeline batch P1 (rafraîchit les données jusqu'au jour courant)."""
    while True:
        try:
            p1_meteo_flow(force_refresh=True, **kwargs)
        except Exception as exc:  # noqa: BLE001
            log.exception("Erreur sur le cycle quotidien P1 : %s", exc)
        now = datetime.now(timezone.utc)
        nxt = (now + timedelta(days=1)).replace(hour=3, minute=0, second=0, microsecond=0)
        log.info("Prochaine exécution P1 : %s", nxt.isoformat())
        time.sleep(max(60.0, (nxt - now).total_seconds()))


def main() -> None:
    parser = argparse.ArgumentParser(description="P1 — Pipeline batch météo Open-Meteo (durée configurable)")
    add_duration_arguments(parser, default_help_suffix=f"défaut : FETCH_DURATION ou {DEFAULT_FETCH_DURATION}")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--limit-villes", type=int, default=None)
    parser.add_argument("--local", action="store_true", help="Mode local (data/) au lieu de MinIO")
    parser.add_argument("--skip-ingest", action="store_true", help="Nettoyer le bronze existant sans appel API")
    parser.add_argument("--force", action="store_true", help="Forcer le re-téléchargement des fichiers")
    parser.add_argument("--daily", action="store_true", help="Boucle quotidienne")
    args = parser.parse_args()

    has_cli_duration = any(v is not None for v in (args.duration, args.days, args.weeks, args.months, args.years))
    env_duration = os.getenv("FETCH_DURATION", "").strip() or DEFAULT_FETCH_DURATION
    kwargs = dict(
        nb_years=args.years if args.years is not None else DEFAULT_YEARS,
        max_workers=args.workers,
        limit_villes=args.limit_villes,
        use_local=args.local,
        days=args.days,
        weeks=args.weeks,
        months=args.months,
        duration=args.duration if has_cli_duration else env_duration,
    )
    if args.daily:
        run_daily(**kwargs)
    else:
        p1_meteo_flow(skip_ingest=args.skip_ingest, force_refresh=args.force, **kwargs)


if __name__ == "__main__":
    main()
