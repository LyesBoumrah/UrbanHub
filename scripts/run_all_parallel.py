"""Orchestrateur parallèle global UrbanHub : exécute P1 (Météo), P2 (Vélos) et P3 (Pollution)
SIMULTANÉMENT en parallèle sur une durée libre (1, 2, 3... jours, semaines, mois ou années),
puis construit la couche Gold JSON (P4).

Exemples :
  python scripts/run_all_parallel.py --duration "2 semaines"
  python scripts/run_all_parallel.py --months 3 --with-history
  python scripts/run_all_parallel.py --years 1 --local
  python scripts/run_all_parallel.py --days 7

Responsable : commun.
"""

from __future__ import annotations

import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from common.log import get_logger
from common.period import DEFAULT_FETCH_DURATION, add_duration_arguments
from p1_meteo.flow import p1_meteo_flow
from p2_velos.flow import velos_backfill_flow, velos_streaming_flow
from p3_pollution.flow import p3_pollution_flow
from p4_croise.flow import p4_gold_flow
from scripts.init_datalake import init_datalake

log = get_logger("urbanhub.parallel")


def run_all_parallel(
    use_local: bool = False,
    *,
    days: int | None = None,
    weeks: int | None = None,
    months: int | None = None,
    years: int | None = None,
    duration: str | None = None,
    with_history: bool = False,
) -> dict[str, Any]:
    """Lance en parallèle les 3 pipelines d'ingestion+nettoyage (P1, P2, P3), puis P4 Gold JSON."""
    t0 = time.monotonic()
    init_datalake(use_local=use_local)

    dur_kwargs = dict(days=days, weeks=weeks, months=months, duration=duration)
    p1_years = years if years is not None else 5

    def _run_p2() -> dict[str, Any]:
        out: dict[str, Any] = {"streaming": velos_streaming_flow(use_local=use_local)}
        if with_history:
            out["backfill"] = velos_backfill_flow(years=p1_years, use_local=use_local, **dur_kwargs)
        return out

    def _run_p3() -> dict[str, Any]:
        mode = "backfill" if with_history else "latest"
        return p3_pollution_flow(mode=mode, years=years, workers=4, use_local=use_local, **dur_kwargs)

    tasks = {
        "p1_meteo": lambda: p1_meteo_flow(nb_years=p1_years, max_workers=6, use_local=use_local, **dur_kwargs),
        "p2_velos": _run_p2,
        "p3_pollution": _run_p3,
    }

    results: dict[str, Any] = {}
    log.info("Lancement PARALLÈLE des 3 flux (P1 Météo, P2 Vélos, P3 Pollution)...")
    with ThreadPoolExecutor(max_workers=3, thread_name_prefix="urbanhub-flux") as pool:
        futures = {pool.submit(fn): name for name, fn in tasks.items()}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                results[name] = fut.result()
                log.info("[%s] Flux terminé avec succès", name)
            except Exception as exc:  # noqa: BLE001 — un flux en échec n'interrompt pas les autres
                log.error("[%s] Échec du flux : %s", name, exc)
                results[name] = {"error": str(exc)}

    log.info("Génération de la couche Gold JSON (P4) depuis les Parquet Silver...")
    results["p4_gold"] = p4_gold_flow(use_local=use_local)
    results["total_duration_s"] = round(time.monotonic() - t0, 1)
    log.info("Cycle complet UrbanHub terminé en %ss", results["total_duration_s"])
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Exécution parallèle des 3 flux UrbanHub + Gold JSON")
    add_duration_arguments(parser, default_help_suffix=f"défaut : FETCH_DURATION ou {DEFAULT_FETCH_DURATION}")
    parser.add_argument("--local", action="store_true", help="Utiliser le stockage disque local (data/)")
    parser.add_argument(
        "--with-history",
        action="store_true",
        help="Activer aussi le backfill historique sur P2 (CityBikes) et P3 (OpenAQ) sur la durée choisie",
    )
    args = parser.parse_args()
    has_cli = any(v is not None for v in (args.duration, args.days, args.weeks, args.months, args.years))
    env_dur = os.getenv("FETCH_DURATION", "").strip() or DEFAULT_FETCH_DURATION
    run_all_parallel(
        use_local=args.local,
        days=args.days,
        weeks=args.weeks,
        months=args.months,
        years=args.years,
        duration=args.duration if has_cli else env_dur,
        with_history=args.with_history,
    )


if __name__ == "__main__":
    main()
