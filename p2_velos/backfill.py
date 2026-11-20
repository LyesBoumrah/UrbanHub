"""P2 — Backfill historique sur 5 ans parallélisé par année (data.citybik.es).

L'API temps réel api.citybik.es/v2 ne retourne que l'état instantané.
Ce module complète le flux streaming en récupérant en parallèle par année tous les dumps
mensuels Parquet disponibles sur https://data.citybik.es/dumps/by-network/ sur une
fenêtre glissante de 5 ans (actuellement disponible depuis novembre 2024 sur CityBikes),
les archive en bronze puis délègue tout le nettoyage/validation/rééchantillonnage à
p2_velos.cleaning.clean_historical_parquet (clé de jointure (ville_id, heure_utc)).

Parallélisation :
- Listage concurrent des répertoires annuels (`/{year}/`) via ThreadPoolExecutor.
- Téléchargement, nettoyage et chargement silver exécutés en parallèle par année
  (un thread, une session HTTP et un client stockage dédiés par année -> thread-safe).

Attribution requise (ODbL v1.0) :
"Bike-share data by CityBikes contributors and available from https://data.citybik.es."

Responsable : P2.
"""

from __future__ import annotations

import argparse
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from common.config import get_settings, load_ref_ville
from common.log import get_logger
from common.period import DEFAULT_FETCH_DURATION, add_duration_arguments, compute_window, months_span
from common.storage import LocalBronzeStorage, MinIOStorage, get_storage
from p2_velos.cleaning import (
    build_silver_historical_key,
    clean_historical_parquet,
    save_silver_parquet_to_storage,
)

logger = get_logger(__name__)

CITYBIKES_DUMPS_BASE = "https://data.citybik.es/dumps/by-network"
DEFAULT_HISTORY_YEARS = 5
DEFAULT_MAX_WORKERS = 5

# Alias rétrocompatible vers la fonction unifiée dans p2_velos.cleaning
transform_dump_parquet = clean_historical_parquet

_DUMP_FILENAME_RE = re.compile(r"^(?P<ym>\d{6})-(?P<network>.+)-stats\.parquet$")


@dataclass(frozen=True)
class HistoricalDump:
    """Métadonnées d'un fichier Parquet mensuel disponible sur data.citybik.es."""

    ville_id: str
    network_id: str
    year: int
    month: int
    ym: str
    filename: str
    url: str
    size_bytes: int


def compute_backfill_window(
    years: int = DEFAULT_HISTORY_YEARS,
    now_utc: datetime | None = None,
) -> tuple[str, str]:
    """Retourne les bornes inclusives (YYYYMM_debut, YYYYMM_fin) sur `years` années."""
    if years < 1:
        raise ValueError("Le nombre d'années doit être >= 1")
    ref = now_utc or datetime.now(timezone.utc)
    if ref.tzinfo is None:
        ref = ref.replace(tzinfo=timezone.utc)
    start_year = ref.year - years
    start_ym = f"{start_year:04d}{ref.month:02d}"
    end_ym = f"{ref.year:04d}{ref.month:02d}"
    return start_ym, end_ym


def parse_dump_filename(filename: str) -> tuple[str, int, int, str] | None:
    """Parse un nom de dump CityBikes (`YYYYMM-<network>-stats.parquet`)."""
    match = _DUMP_FILENAME_RE.match(filename)
    if not match:
        return None
    ym = match.group("ym")
    year = int(ym[:4])
    month = int(ym[4:6])
    if not (1 <= month <= 12):
        return None
    return ym, year, month, match.group("network")


def filter_dumps_for_window(
    entries_by_year: Mapping[int, Sequence[Mapping[str, Any]]],
    network_to_ville: Mapping[str, str],
    years: int = DEFAULT_HISTORY_YEARS,
    now_utc: datetime | None = None,
    base_url: str = CITYBIKES_DUMPS_BASE,
) -> list[HistoricalDump]:
    """Filtre les dumps disponibles pour les réseaux cibles sur la fenêtre de N années."""
    start_ym, end_ym = compute_backfill_window(years=years, now_utc=now_utc)
    dumps: list[HistoricalDump] = []

    for year in sorted(entries_by_year.keys()):
        for item in entries_by_year[year]:
            fname = str(item.get("name", ""))
            parsed = parse_dump_filename(fname)
            if parsed is None:
                continue
            ym, parsed_year, parsed_month, network_id = parsed
            if network_id not in network_to_ville:
                continue
            if not (start_ym <= ym <= end_ym):
                continue
            dumps.append(
                HistoricalDump(
                    ville_id=network_to_ville[network_id],
                    network_id=network_id,
                    year=parsed_year,
                    month=parsed_month,
                    ym=ym,
                    filename=fname,
                    url=f"{base_url.rstrip('/')}/{year}/{fname}",
                    size_bytes=int(item.get("size", 0)),
                )
            )

    return sorted(dumps, key=lambda d: (d.ym, d.ville_id))


def group_dumps_by_year(dumps: Sequence[HistoricalDump]) -> dict[int, list[HistoricalDump]]:
    """Regroupe les dumps par année civile (triés chronologiquement) pour exécution parallèle."""
    grouped: dict[int, list[HistoricalDump]] = {}
    for dump in sorted(dumps, key=lambda d: (d.year, d.ym, d.ville_id)):
        grouped.setdefault(dump.year, []).append(dump)
    return dict(sorted(grouped.items()))


def list_available_dumps(
    cities: Sequence[str] | None = None,
    core_only: bool = False,
    years: int = DEFAULT_HISTORY_YEARS,
    now_utc: datetime | None = None,
    timeout: int = 20,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> list[HistoricalDump]:
    """Interroge data.citybik.es/dumps/by-network/ en parallèle par année."""
    from p2_velos.ingestion import build_http_session

    ref_villes = load_ref_ville(core_only=core_only)
    if cities:
        wanted = {c.strip().lower() for c in cities}
        ref_villes = [v for v in ref_villes if v["ville_id"].lower() in wanted]

    network_to_ville = {
        v["citybikes"]["network_id"]: v["ville_id"]
        for v in ref_villes
        if (v.get("citybikes") or {}).get("network_id")
    }

    start_ym, end_ym = compute_backfill_window(years=years, now_utc=now_utc)
    start_year, end_year = int(start_ym[:4]), int(end_ym[:4])

    root_session = build_http_session()
    root_resp = root_session.get(f"{CITYBIKES_DUMPS_BASE}/", timeout=timeout)
    root_resp.raise_for_status()
    root_entries = root_resp.json()

    available_years = sorted(
        int(e["name"])
        for e in root_entries
        if e.get("type") == "directory" and str(e.get("name", "")).isdigit()
    )
    target_years = [y for y in available_years if start_year <= y <= end_year]

    def _fetch_year_listing(year: int) -> tuple[int, list[dict[str, Any]]]:
        sess = build_http_session()
        resp = sess.get(f"{CITYBIKES_DUMPS_BASE}/{year}/", timeout=timeout)
        resp.raise_for_status()
        return year, resp.json()

    entries_by_year: dict[int, list[dict[str, Any]]] = {}
    if target_years:
        workers = max(1, min(int(max_workers), len(target_years)))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_fetch_year_listing, y): y for y in target_years}
            for fut in as_completed(futures):
                y, items = fut.result()
                entries_by_year[y] = items

    dumps = filter_dumps_for_window(
        entries_by_year=entries_by_year,
        network_to_ville=network_to_ville,
        years=years,
        now_utc=now_utc,
    )

    if dumps:
        logger.info(
            "Fenêtre demandée : %s -> %s (%d ans) | Archive CityBikes disponible : %s -> %s "
            "(%d fichiers sur %d années, %.1f Mo)",
            start_ym,
            end_ym,
            years,
            dumps[0].ym,
            dumps[-1].ym,
            len(dumps),
            len(group_dumps_by_year(dumps)),
            sum(d.size_bytes for d in dumps) / (1024 * 1024),
        )
        if dumps[0].ym > start_ym:
            logger.info(
                "Note : l'archive publique data.citybik.es débute en %s ; "
                "aucune donnée antérieure n'est publiée par CityBikes.",
                dumps[0].ym,
            )
    else:
        logger.warning("Aucun dump trouvé sur data.citybik.es pour la fenêtre %s -> %s", start_ym, end_ym)

    return dumps


def backfill_single_dump(
    dump: HistoricalDump,
    storage: MinIOStorage | LocalBronzeStorage,
    bbox: Mapping[str, float] | None = None,
    freq: str | None = "1h",
    use_local: bool = False,
    skip_existing_bronze: bool = True,
    session: Any = None,
    min_timestamp: datetime | None = None,
    max_timestamp: datetime | None = None,
) -> dict[str, Any]:
    """Télécharge, archive en bronze, nettoie (via cleaning.py) et écrit en silver Parquet un dump mensuel."""
    from p2_velos.ingestion import build_http_session

    bronze_key = f"bronze/velos_historique/{dump.ville_id}/{dump.year}/{dump.filename}"

    if skip_existing_bronze and storage.exists(bronze_key):
        logger.info("[%d][%s][%s] Réutilisation bronze : %s", dump.year, dump.ville_id, dump.ym, bronze_key)
        raw_bytes = storage.get_bytes(bronze_key)
    else:
        http = session or build_http_session()
        logger.info(
            "[%d][%s][%s] Téléchargement %s (%.2f Mo)...",
            dump.year,
            dump.ville_id,
            dump.ym,
            dump.filename,
            dump.size_bytes / (1024 * 1024),
        )
        resp = http.get(dump.url, timeout=120)
        resp.raise_for_status()
        raw_bytes = resp.content
        storage.put_bytes(bronze_key, raw_bytes, content_type="application/octet-stream")

    df_stations, df_releves, stats = clean_historical_parquet(
        parquet_bytes=raw_bytes,
        ville_id=dump.ville_id,
        network_id=dump.network_id,
        bbox=bbox,
        freq=freq,
    )

    if not df_releves.empty and (min_timestamp is not None or max_timestamp is not None):
        import pandas as pd

        ts_col = pd.to_datetime(df_releves["timestamp"], utc=True)
        mask = pd.Series(True, index=df_releves.index)
        if min_timestamp is not None:
            mask &= ts_col >= pd.Timestamp(min_timestamp)
        if max_timestamp is not None:
            mask &= ts_col <= pd.Timestamp(max_timestamp)
        df_releves = df_releves.loc[mask].reset_index(drop=True)
        stats["resampled_count"] = len(df_releves)

    silver_key: str | None = None
    if use_local:
        out_dir = Path(get_settings().local_data_dir) / "silver" / "velos_historique" / dump.ville_id
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"velo_releve_{dump.ym}.parquet"
        df_releves.to_parquet(out_path, index=False)
        silver_key = str(out_path)
        logger.info(
            "[%d][%s][%s] Silver local : %d -> %d relevés (%s, rejets: %.1f %%)",
            dump.year,
            dump.ville_id,
            dump.ym,
            stats["raw_count"],
            stats["resampled_count"],
            freq or "brut",
            stats["rejection_rate"] * 100.0,
        )
    else:
        hist_station_key = f"silver/velos_historique/{dump.ville_id}/velo_station_latest.parquet"
        silver_key = save_silver_parquet_to_storage(
            storage=storage,
            stations=df_stations,
            releves=df_releves,
            releve_key=build_silver_historical_key(dump.ville_id, dump.year, dump.ym),
            station_key=hist_station_key,
        )
        try:
            from common.warehouse import try_get_warehouse
            from p2_velos.cleaning import RELEVE_COLUMNS, STATION_COLUMNS

            wh = try_get_warehouse()
            if wh is not None:
                if not df_stations.empty:
                    wh.insert_df("urbanhub.velo_station", df_stations[STATION_COLUMNS])
                if not df_releves.empty:
                    wh.insert_df("urbanhub.velo_releve", df_releves[RELEVE_COLUMNS])
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[%d][%s][%s] Synchronisation ClickHouse ignorée : %s",
                dump.year,
                dump.ville_id,
                dump.ym,
                exc,
            )
        logger.info(
            "[%d][%s][%s] Bronze -> Silver Parquet : %d stations, %d relevés (%s, rejets: %.1f %%)",
            dump.year,
            dump.ville_id,
            dump.ym,
            len(df_stations),
            len(df_releves),
            freq or "brut",
            stats["rejection_rate"] * 100.0,
        )

    return {
        "year": dump.year,
        "ville_id": dump.ville_id,
        "ym": dump.ym,
        "bronze_key": bronze_key,
        "silver_key": silver_key,
        **stats,
    }


def _process_year_batch(
    year: int,
    year_dumps: Sequence[HistoricalDump],
    bbox_map: Mapping[str, Mapping[str, float]],
    freq: str | None,
    use_local: bool,
    skip_existing_bronze: bool,
    min_timestamp: datetime | None = None,
    max_timestamp: datetime | None = None,
) -> dict[str, Any]:
    """Worker dédié à une année (session HTTP et stockage isolés par thread)."""
    from p2_velos.ingestion import build_http_session

    storage = get_storage(use_local=use_local)
    session = build_http_session()

    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    logger.info("[année %d] Démarrage du worker (%d fichiers)", year, len(year_dumps))
    for dump in year_dumps:
        try:
            res = backfill_single_dump(
                dump=dump,
                storage=storage,
                bbox=bbox_map.get(dump.ville_id),
                freq=freq,
                use_local=use_local,
                skip_existing_bronze=skip_existing_bronze,
                session=session,
                min_timestamp=min_timestamp,
                max_timestamp=max_timestamp,
            )
            results.append(res)
        except Exception as exc:
            logger.error("[%d][%s][%s] Échec backfill : %s", year, dump.ville_id, dump.ym, exc)
            errors.append({"year": year, "ville_id": dump.ville_id, "ym": dump.ym, "error": str(exc)})

    logger.info(
        "[année %d] Terminée : %d/%d fichiers OK (%d relevés silver)",
        year,
        len(results),
        len(year_dumps),
        sum(r["resampled_count"] for r in results),
    )
    return {
        "year": year,
        "results": results,
        "errors": errors,
    }


def run_backfill(
    years: int = DEFAULT_HISTORY_YEARS,
    cities: Sequence[str] | None = None,
    core_only: bool = False,
    freq: str | None = "1h",
    use_local: bool = False,
    skip_existing_bronze: bool = True,
    max_months_per_city: int | None = None,
    max_workers: int = DEFAULT_MAX_WORKERS,
    *,
    days: int | None = None,
    weeks: int | None = None,
    months: int | None = None,
    duration: str | None = None,
) -> dict[str, Any]:
    """Lance le backfill historique en parallèle sur la durée demandée (jours, semaines, mois, années)."""
    has_custom = any(v is not None for v in (days, weeks, months, duration))
    effective_years = years
    req_start_utc: datetime | None = None
    req_end_utc: datetime | None = None
    req_start_ym: str | None = None
    req_end_ym: str | None = None
    if has_custom:
        req_start_utc, req_end_utc = compute_window(
            days=days,
            weeks=weeks,
            months=months,
            years=None,
            duration=duration,
            default_years=years,
        )
        span_m = months_span(req_start_utc, req_end_utc)
        effective_years = max(1, (span_m + 11) // 12)
        req_start_ym = f"{req_start_utc.year:04d}{req_start_utc.month:02d}"
        req_end_ym = f"{req_end_utc.year:04d}{req_end_utc.month:02d}"

    all_available_dumps = list_available_dumps(
        cities=cities,
        core_only=core_only,
        years=effective_years,
        max_workers=max_workers,
    )
    latest_available_ym = max((d.ym for d in all_available_dumps), default=None)
    dumps = all_available_dumps

    if req_start_ym and req_end_ym:
        dumps = [d for d in dumps if req_start_ym <= d.ym <= req_end_ym]

    if max_months_per_city is not None and max_months_per_city > 0:
        grouped_city: dict[str, list[HistoricalDump]] = {}
        for d in dumps:
            grouped_city.setdefault(d.ville_id, []).append(d)
        selected: list[HistoricalDump] = []
        for city_dumps in grouped_city.values():
            selected.extend(sorted(city_dumps, key=lambda x: x.ym)[-max_months_per_city:])
        dumps = sorted(selected, key=lambda d: (d.ym, d.ville_id))

    dumps_by_year = group_dumps_by_year(dumps)
    bbox_map = {v["ville_id"]: v["bbox"] for v in load_ref_ville() if "bbox" in v}

    all_results: list[dict[str, Any]] = []
    all_errors: list[dict[str, Any]] = []
    by_year_stats: dict[int, dict[str, int]] = {}

    if dumps_by_year:
        workers = max(1, min(int(max_workers), len(dumps_by_year)))
        logger.info(
            "Exécution parallèle par année : %d workers pour les années %s",
            workers,
            list(dumps_by_year.keys()),
        )
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    _process_year_batch,
                    year,
                    year_dumps,
                    bbox_map,
                    freq,
                    use_local,
                    skip_existing_bronze,
                    req_start_utc,
                    req_end_utc,
                ): year
                for year, year_dumps in dumps_by_year.items()
            }
            for fut in as_completed(futures):
                year_out = fut.result()
                y = year_out["year"]
                all_results.extend(year_out["results"])
                all_errors.extend(year_out["errors"])
                by_year_stats[y] = {
                    "dumps_ok": len(year_out["results"]),
                    "dumps_failed": len(year_out["errors"]),
                    "silver_rows": sum(r["resampled_count"] for r in year_out["results"]),
                }

    summary = {
        "years_requested": years,
        "requested_start_utc": req_start_utc.isoformat() if req_start_utc else None,
        "requested_end_utc": req_end_utc.isoformat() if req_end_utc else None,
        "latest_available_ym": latest_available_ym,
        "parallel_years": sorted(dumps_by_year.keys()),
        "dumps_total": len(dumps),
        "dumps_ok": len(all_results),
        "dumps_failed": len(all_errors),
        "total_raw_rows": sum(r["raw_count"] for r in all_results),
        "total_silver_rows": sum(r["resampled_count"] for r in all_results),
        "earliest_ym": dumps[0].ym if dumps else None,
        "latest_ym": dumps[-1].ym if dumps else None,
        "by_year": dict(sorted(by_year_stats.items())),
        "errors": all_errors,
    }
    logger.info("Backfill historique parallèle terminé : %s", summary)
    return summary


def main() -> None:
    import os

    parser = argparse.ArgumentParser(
        description="P2 — Backfill historique parallélisé par année (durée configurable : jours/semaines/mois/années)"
    )
    add_duration_arguments(parser, default_help_suffix=f"défaut : FETCH_DURATION ou {DEFAULT_FETCH_DURATION}")
    parser.add_argument("--cities", nargs="*", help="Liste de ville_id à récupérer (défaut: toutes)")
    parser.add_argument("--core-only", action="store_true", help="Limiter aux 5 villes cœur UrbanHub")
    parser.add_argument(
        "--freq",
        default="1h",
        help="Granularité de rééchantillonnage pandas (défaut: 1h ; 'raw' pour conserver tous les événements)",
    )
    parser.add_argument("--local", action="store_true", help="Écrire en bronze/silver local (data/) au lieu de MinIO")
    parser.add_argument("--force", action="store_true", help="Re-télécharger même si déjà présent en bronze")
    parser.add_argument(
        "--max-months-per-city",
        type=int,
        default=None,
        help="Optionnel : limiter aux N derniers mois par ville",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help="Nombre maximal de threads parallèles (1 par année, défaut: 5)",
    )
    args = parser.parse_args()

    has_cli = any(v is not None for v in (args.duration, args.days, args.weeks, args.months, args.years))
    default_dur = os.getenv("FETCH_DURATION", "").strip() or DEFAULT_FETCH_DURATION
    freq_val = None if str(args.freq).lower() == "raw" else args.freq
    run_backfill(
        years=args.years if args.years is not None else DEFAULT_HISTORY_YEARS,
        cities=args.cities,
        core_only=args.core_only,
        freq=freq_val,
        use_local=args.local,
        skip_existing_bronze=not args.force,
        max_months_per_city=args.max_months_per_city,
        max_workers=args.max_workers,
        days=args.days,
        weeks=args.weeks,
        months=args.months,
        duration=args.duration if has_cli else default_dur,
    )


if __name__ == "__main__":
    main()
