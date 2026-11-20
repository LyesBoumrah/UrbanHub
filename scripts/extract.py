"""ÉTAPE 1 — EXTRACT : les 3 archives (5 ans glissants) → couche bronze dans Silo, telles quelles.

    P1 météo     Open-Meteo Archive   → bronze/meteo/
    P2 vélos     CityBikes dumps      → bronze/velos_historique/
    P3 pollution OpenAQ archive S3    → bronze/openaq_archive/

Les trois extractions tournent en parallèle (sources indépendantes) ; chacune est idempotente
et reprenable, et écrit son propre manifeste dans bronze/<source>/_manifests/.

Usage :
    python -m scripts.extract                 # les 3 sources
    python -m scripts.extract --only p1 p3    # une partie seulement
    python -m scripts.extract --local         # vers data/ au lieu de Silo (tests)

Responsable : commun.
"""
from __future__ import annotations

import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor

from common.config import get_settings, load_villes
from common.log import get_logger
from common.period import DEFAULT_YEARS, study_period
from common.storage import get_storage

log = get_logger("extract")


def run_p1(storage, start, end, user_agent, nb_years):
    from p1_meteo.extract import extract

    return extract(storage, load_villes(), start, end, user_agent)


def run_p2(storage, start, end, user_agent, nb_years):
    from p2_velos.extract import extract, list_dumps

    return extract(storage, list_dumps(nb_years), start, end, user_agent)


def run_p3(storage, start, end, user_agent, nb_years):
    from p3_pollution.ingestion import ArchiveClient, extract, load_locations

    return extract(ArchiveClient(user_agent), storage, load_locations(), start, end)


RUNNERS = {"p1": run_p1, "p2": run_p2, "p3": run_p3}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--only", nargs="+", choices=sorted(RUNNERS), default=sorted(RUNNERS))
    parser.add_argument("--years", type=int, default=DEFAULT_YEARS)
    parser.add_argument("--local", action="store_true", help="écrire dans data/ au lieu de Silo")
    args = parser.parse_args()

    settings = get_settings()
    use_local = args.local or os.environ.get("STORAGE", "minio").lower() == "local"
    start, end = study_period(args.years)
    log.info("EXTRACT %s | période %s → %s | destination : %s", " + ".join(args.only), start, end,
             "data/ (local)" if use_local else f"Silo s3://{settings.minio_bucket} ({settings.minio_endpoint})")

    t0 = time.monotonic()
    results = {}
    with ThreadPoolExecutor(max_workers=len(args.only), thread_name_prefix="source") as pool:
        # un client de stockage par source : pas de partage d'état entre threads
        futures = {name: pool.submit(RUNNERS[name], get_storage(use_local=use_local), start, end,
                                     settings.http_user_agent, args.years) for name in args.only}
        for name, fut in futures.items():
            try:
                results[name] = fut.result()
            except Exception as exc:
                log.error("%s : extraction interrompue : %s", name.upper(), exc)
                results[name] = None

    log.info("=" * 70)
    for name in args.only:
        r = results[name]
        if r is None:
            log.info("%s : ÉCHEC (voir les erreurs ci-dessus)", name.upper())
            continue
        log.info("%s : %s fichiers téléchargés, %s déjà présents, %s erreurs, %.1f Mo, %ss", name.upper(),
                 r["files_downloaded"], r["files_skipped"], len(r["errors"]), r["bytes_downloaded"] / 1e6,
                 r["duration_s"])
    log.info("EXTRACT terminé en %.0f s", time.monotonic() - t0)


if __name__ == "__main__":
    main()
