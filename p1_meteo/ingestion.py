"""P1 — Téléchargement parallèle Open-Meteo (archive horaire 5 ans + données récentes) vers le bronze.

Pourquoi Open-Meteo plutôt que NOAA ISD :
  L'archive NOAA Global Hourly (`/data/global-hourly/access/`) a cessé d'être alimentée en
  données récentes (arrêt en août 2025 lors de la migration NCEI), ce qui empêche le croisement
  temporel récent avec les flux temps réel CityBikes (P2) et IoT OpenAQ (P3).
  L'API Open-Meteo combine la réanalyse historique ERA5/IFS (`archive-api.open-meteo.com`) et le
  modèle haute résolution temps réel (`api.open-meteo.com/v1/forecast` avec `past_days`), fournissant
  les observations horaires jusqu'au jour même pour toutes les stations/villes françaises.

Responsable : P1.
"""

from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests

from common.config import load_ref_ville, load_villes, settings
from common.log import setup_logging
from common.period import DEFAULT_FETCH_DURATION, add_duration_arguments, compute_date_period
from common.storage import Storage, get_storage

log = setup_logging(__name__)

OPENMETEO_URL = "https://archive-api.open-meteo.com/v1/archive"
OPENMETEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
BRONZE_PREFIX = "bronze/meteo"

DEFAULT_YEARS = 5
DEFAULT_WORKERS = 6

HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "precipitation",
    "pressure_msl",
    "wind_speed_10m",
    "wind_direction_10m",
    "visibility",
]


def default_period(
    nb_years: int = DEFAULT_YEARS,
    today: date | None = None,
    *,
    days: int | None = None,
    weeks: int | None = None,
    months: int | None = None,
    duration: str | None = None,
) -> tuple[str, str]:
    """Fenêtre de récupération jusqu'à aujourd'hui (ex. 3 jours, 2 semaines, 1 mois, 5 ans)."""
    return compute_date_period(
        days=days,
        weeks=weeks,
        months=months,
        years=None if any(v is not None for v in (days, weeks, months, duration)) else nb_years,
        duration=duration,
        default_years=DEFAULT_YEARS,
        today=today,
    )


def bronze_key(ville_id: str, start_date: str, end_date: str) -> str:
    """Clé de stockage bronze pour une ville et une période."""
    return f"{BRONZE_PREFIX}/{ville_id}/{start_date}_{end_date}.json"


def station_id_for_ville(ville: dict[str, Any], ref_map: dict[str, dict[str, Any]] | None = None) -> str:
    """Retourne l'identifiant de station météo associé à la ville (code station ou FR-<VILLE>)."""
    vid = ville["ville_id"]
    if ref_map and vid in ref_map and ref_map[vid].get("noaa_station_id"):
        return str(ref_map[vid]["noaa_station_id"])
    return str(ville.get("station_id") or f"FR-{vid.upper()}")


def merge_archive_and_recent(archive_payload: dict[str, Any], recent_payload: dict[str, Any] | None) -> dict[str, Any]:
    """Fusionne la série historique (`archive-api`) avec les relevés récents (`forecast?past_days=14`).

    Les relevés récents complètent les dernières heures non encore consolidées dans ERA5 et
    apportent la visibilité mesurée en temps réel.
    """
    if not recent_payload or "hourly" not in recent_payload or not recent_payload["hourly"].get("time"):
        return archive_payload
    if "hourly" not in archive_payload or not archive_payload["hourly"].get("time"):
        return recent_payload

    arch_h = archive_payload["hourly"]
    rec_h = recent_payload["hourly"]
    arch_times = list(arch_h.get("time", []))
    rec_times = list(rec_h.get("time", []))

    index_by_time = {t: i for i, t in enumerate(arch_times)}
    all_vars = [k for k in HOURLY_VARS if k in arch_h or k in rec_h]

    # S'assurer que chaque variable existe dans arch_h avec la bonne longueur
    for v in all_vars:
        if v not in arch_h or not isinstance(arch_h[v], list):
            arch_h[v] = [None] * len(arch_times)

    for j, t in enumerate(rec_times):
        if t in index_by_time:
            idx = index_by_time[t]
            for v in all_vars:
                rec_vals = rec_h.get(v)
                rec_val = rec_vals[j] if isinstance(rec_vals, list) and j < len(rec_vals) else None
                if rec_val is not None:
                    arch_h[v][idx] = rec_val
        else:
            index_by_time[t] = len(arch_times)
            arch_times.append(t)
            for v in all_vars:
                rec_vals = rec_h.get(v)
                rec_val = rec_vals[j] if isinstance(rec_vals, list) and j < len(rec_vals) else None
                arch_h[v].append(rec_val)

    arch_h["time"] = arch_times
    return archive_payload


def clip_hourly_payload(
    payload: dict[str, Any],
    start_date: str,
    end_date: str,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    """Filtre les pas horaires sur `[start_date 00:00 UTC, min(end_date 23:59 UTC, heure courante UTC)]`.

    Évite à la fois le débordement antérieur de `forecast?past_days=14` (ex. 14 jours quand 1 semaine
    est demandée) et l'inclusion d'heures futures de prévision au-delà de l'heure courante.
    """
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict) or not hourly.get("time"):
        return payload

    times = list(hourly.get("time", []))
    start_dt = datetime.fromisoformat(f"{start_date}T00:00:00+00:00")
    end_day_dt = datetime.fromisoformat(f"{end_date}T23:59:59+00:00")
    ref_now = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )
    upper_dt = min(end_day_dt, ref_now)

    keep_pairs: list[tuple[datetime, int]] = []
    for idx, raw_t in enumerate(times):
        try:
            ts = datetime.fromisoformat(str(raw_t).replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            else:
                ts = ts.astimezone(timezone.utc)
        except ValueError:
            continue
        if start_dt <= ts <= upper_dt:
            keep_pairs.append((ts, idx))

    keep_pairs.sort(key=lambda pair: pair[0])
    keep_indices = [idx for _, idx in keep_pairs]

    out = dict(payload)
    new_hourly: dict[str, Any] = {}
    for key, vals in hourly.items():
        if isinstance(vals, list):
            new_hourly[key] = [vals[i] if i < len(vals) else None for i in keep_indices]
        else:
            new_hourly[key] = vals
    out["hourly"] = new_hourly
    return out


def _request_openmeteo(
    url: str,
    params: dict[str, Any],
    timeout: int = 180,
    retries: int = 3,
    ville_id: str = "",
) -> dict[str, Any] | None:
    """Exécute un appel GET vers Open-Meteo avec gestion du rate-limit (429) et retries."""
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(
                url,
                params=params,
                headers={"User-Agent": settings.http_user_agent},
                timeout=timeout,
            )
            if response.status_code == 429:
                wait = int(response.headers.get("Retry-After", 15))
                log.warning(
                    "429 Open-Meteo %s, attente %ss (tentative %s/%s)",
                    ville_id,
                    wait,
                    attempt,
                    retries,
                )
                time.sleep(wait)
                continue

            if response.status_code == 400:
                log.warning("400 Open-Meteo %s sur %s : %s", ville_id, url, response.text[:200])
                return None

            response.raise_for_status()
            payload = response.json()
            if "hourly" in payload and payload["hourly"].get("time"):
                return payload
            return None
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "Échec Open-Meteo %s sur %s (tentative %s/%s) : %s",
                ville_id,
                url,
                attempt,
                retries,
                exc,
            )
            if attempt < retries:
                time.sleep(3 * attempt)
    return None


def fetch_ville_meteo(
    ville: dict[str, Any],
    start_date: str,
    end_date: str,
    storage: Any,
    timeout: int = 180,
    retries: int = 3,
    include_recent_forecast: bool = True,
    station_id: str | None = None,
) -> str | None:
    """Récupère 5 ans de données horaires + les données récentes pour une ville et écrit dans bronze."""
    vid = ville["ville_id"]
    # L'archive ERA5 a ~2 à 5 jours de latence ; forecast?past_days=14 comble jusqu'à l'heure courante
    archive_end = (date.fromisoformat(end_date) - timedelta(days=2)).isoformat()
    archive_params = {
        "latitude": ville["lat"],
        "longitude": ville["lon"],
        "start_date": start_date,
        "end_date": archive_end,
        "hourly": ",".join(HOURLY_VARS),
        "wind_speed_unit": "kmh",
        "timezone": "UTC",
    }
    payload = _request_openmeteo(
        OPENMETEO_URL,
        params=archive_params,
        timeout=timeout,
        retries=retries,
        ville_id=vid,
    )

    # Repli à J-7 si la réanalyse ERA5 n'est pas encore publiée jusqu'à J-2
    if payload is None:
        archive_params["end_date"] = (date.fromisoformat(end_date) - timedelta(days=7)).isoformat()
        payload = _request_openmeteo(
            OPENMETEO_URL,
            params=archive_params,
            timeout=timeout,
            retries=retries,
            ville_id=vid,
        )

    # Complément temps réel / récent via l'endpoint forecast (borné à la durée demandée, max 14 jours)
    recent_payload = None
    if include_recent_forecast:
        span_days = max(1, (date.fromisoformat(end_date) - date.fromisoformat(start_date)).days)
        forecast_params = {
            "latitude": ville["lat"],
            "longitude": ville["lon"],
            "hourly": ",".join(HOURLY_VARS),
            "past_days": min(14, span_days),
            "forecast_days": 1,
            "wind_speed_unit": "kmh",
            "timezone": "UTC",
        }
        recent_payload = _request_openmeteo(
            OPENMETEO_FORECAST_URL,
            params=forecast_params,
            timeout=timeout,
            retries=2,
            ville_id=vid,
        )

    if payload is None and recent_payload is None:
        log.warning("Open-Meteo %s : aucune donnée récupérée", vid)
        return None

    merged = merge_archive_and_recent(payload or recent_payload, recent_payload if payload else None)
    merged = clip_hourly_payload(merged, start_date=start_date, end_date=end_date)
    merged["requested_start_date"] = start_date
    merged["requested_end_date"] = end_date
    merged["ville_id"] = vid
    merged["ville_nom"] = ville.get("nom", vid)
    merged["station_id"] = station_id or station_id_for_ville(ville)

    key = bronze_key(vid, start_date, end_date)
    storage.put_bytes(
        key,
        json.dumps(merged, ensure_ascii=False).encode("utf-8"),
        "application/json",
    )
    log.info(
        "Bronze écrit : %s (%s heures, station=%s)",
        key,
        len(merged["hourly"]["time"]),
        merged["station_id"],
    )
    return key


def ingest(
    nb_years: int = DEFAULT_YEARS,
    max_workers: int = DEFAULT_WORKERS,
    limit_villes: int | None = None,
    skip_existing: bool = True,
    delay_seconds: float = 0.0,
    use_local: bool = False,
    storage: Any = None,
    *,
    days: int | None = None,
    weeks: int | None = None,
    months: int | None = None,
    duration: str | None = None,
) -> list[str]:
    """Télécharge en parallèle les données météo des villes françaises sur la durée choisie."""
    st = storage if storage is not None else (get_storage(use_local=True) if use_local else Storage())
    villes = load_villes()
    ref_map = {r["ville_id"]: r for r in load_ref_ville()}
    if limit_villes:
        villes = villes[:limit_villes]

    start_date, end_date = default_period(
        nb_years=nb_years,
        days=days,
        weeks=weeks,
        months=months,
        duration=duration,
    )
    log.info("Villes : %s | période : %s -> %s (parallélisme=%s)", len(villes), start_date, end_date, max_workers)

    written: list[str] = []
    tasks = []
    for ville in villes:
        key = bronze_key(ville["ville_id"], start_date, end_date)
        if skip_existing and st.exists(key):
            log.info("Déjà présent : %s", key)
            written.append(key)
            continue
        tasks.append((ville, start_date, end_date, station_id_for_ville(ville, ref_map)))

    if not tasks:
        log.info("Tous les fichiers sont déjà présents.")
        return written

    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="openmeteo") as executor:
        futures = {}
        for ville, start, end, sid in tasks:
            futures[
                executor.submit(
                    fetch_ville_meteo,
                    ville,
                    start,
                    end,
                    st,
                    station_id=sid,
                )
            ] = ville
            if delay_seconds > 0:
                time.sleep(delay_seconds)

        for future in as_completed(futures):
            key = future.result()
            if key:
                written.append(key)

    log.info("Ingestion terminée : %s fichiers / %s villes", len(written), len(villes))
    return written


def main() -> None:
    import os

    parser = argparse.ArgumentParser(description="P1 — Téléchargement parallèle Open-Meteo (durée configurable)")
    add_duration_arguments(parser, default_help_suffix=f"défaut : FETCH_DURATION ou {DEFAULT_FETCH_DURATION}")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Nombre de threads parallèles")
    parser.add_argument("--limit-villes", type=int, default=None, help="Limiter aux N premières villes")
    parser.add_argument("--local", action="store_true", help="Écrire dans data/ au lieu de MinIO")
    parser.add_argument("--force", action="store_true", help="Re-télécharger même si déjà présent")
    args = parser.parse_args()
    has_cli = any(v is not None for v in (args.duration, args.days, args.weeks, args.months, args.years))
    default_dur = os.getenv("FETCH_DURATION", "").strip() or DEFAULT_FETCH_DURATION
    ingest(
        nb_years=args.years if args.years is not None else DEFAULT_YEARS,
        max_workers=args.workers,
        limit_villes=args.limit_villes,
        skip_existing=not args.force,
        use_local=args.local,
        days=args.days,
        weeks=args.weeks,
        months=args.months,
        duration=args.duration if has_cli else default_dur,
    )


if __name__ == "__main__":
    main()
