"""P3 — Extraction : archives OpenAQ des 5 dernières années → couche bronze (MinIO ou disque local).

Source : l'archive publique OpenAQ sur AWS S3 (sans clé API, sans quota)
    https://openaq-data-archive.s3.amazonaws.com/records/csv.gz/
        locationid=<id>/year=<YYYY>/month=<MM>/location-<id>-<YYYYMMDD>.csv.gz
Un fichier = une station × un jour ; CSV horaire contenant tous les capteurs de la station :
    location_id, sensors_id, location, datetime (heure locale avec décalage), lat, lon, parameter, units, value

Choix (validés avec le professeur) : pas de temps réel, uniquement l'historique des 5 dernières
années glissantes en mois complets (ex. le 20/11/2026 : 2021-11-01 → 2026-10-31), même période
pour les 3 sources (jointure P4).

Renumérotation EEA 2024 : 14 stations ont changé d'identifiant OpenAQ (ancien arrêté début 2024).
Leurs anciens identifiants (`predecessor_location_ids` dans la config) sont téléchargés aussi,
tels quels sous leur propre locationid ; le rattachement à la station actuelle se fait au nettoyage.

Fonctionnement :
  1. pour chaque station de config/openaq_locations.json, liste les fichiers disponibles par année ;
  2. télécharge en parallèle (ThreadPoolExecutor) les fichiers absents du bronze ;
  3. copie chaque fichier TEL QUEL (bronze = brut et immuable) sous
        bronze/openaq_archive/locationid=<id>/year=<YYYY>/month=<MM>/location-<id>-<YYYYMMDD>.csv.gz
  4. écrit un manifeste de l'exécution (source, horodatage, couverture par station)
        bronze/openaq_archive/_manifests/<YYYYMMDDTHHMMSSZ>_<batch_id>.json

Idempotent et reprenable : un fichier déjà présent dans le bronze n'est jamais retéléchargé.
Le parsing (UTC, filtrage des capteurs, dédoublonnage) se fait dans cleaning.py.

Usage :
  python -m p3_pollution.ingestion                    # 5 dernières années, toutes les stations
  python -m p3_pollution.ingestion --years 1          # test rapide sur 1 an
  python -m p3_pollution.ingestion --locations 2696   # une seule station (test)

Stockage : variable STORAGE du .env — `local` (data/, défaut) ou `minio`.

Responsable : P3.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests

from common.config import get_settings
from common.log import get_logger
from common.period import study_period
from common.storage import get_storage

ROOT = Path(__file__).resolve().parents[1]
LOCATIONS_FILE = ROOT / "config" / "openaq_locations.json"

ARCHIVE_URL = "https://openaq-data-archive.s3.amazonaws.com"
ARCHIVE_PREFIX = "records/csv.gz"
BRONZE_PREFIX = "bronze/openaq_archive"
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

# Pas de quota sur l'archive S3 : le parallélisme accélère réellement (fichiers de ~1 Ko).
DEFAULT_WORKERS = 16

KEY_RE = re.compile(r"location-(?P<loc>\d+)-(?P<day>\d{8})\.csv\.gz$")

log = get_logger("p3.ingestion")


# ---------------------------------------------------------------- fonctions pures (testées)

def default_period(nb_years=5, today=None):
    """Période d'étude commune aux 3 sources (voir common/period.py)."""
    return study_period(nb_years, today)


def archive_ids(loc):
    """Identifiants d'archive d'une station : l'actuel + ses prédécesseurs (renumérotation EEA 2024)."""
    return [loc["location_id"], *loc.get("predecessor_location_ids", [])]


def unique_days(keys):
    """Nombre de jours distincts couverts par une liste de fichiers (tous identifiants confondus)."""
    return len({parsed[1] for k in keys for parsed in [parse_archive_key(k)] if parsed})


def archive_year_prefix(location_id, year):
    return f"{ARCHIVE_PREFIX}/locationid={location_id}/year={year}/"


def parse_archive_key(key):
    """'…/location-2696-20230101.csv.gz' -> (2696, date(2023, 1, 1)) ; None si le nom est inattendu."""
    m = KEY_RE.search(key)
    if not m:
        return None
    return int(m["loc"]), datetime.strptime(m["day"], "%Y%m%d").date()


def to_bronze_key(archive_key):
    """Même arborescence que l'archive, sous bronze/openaq_archive/."""
    return f"{BRONZE_PREFIX}/{archive_key[len(ARCHIVE_PREFIX) + 1:]}"


def select_keys(keys, start, end):
    """Garde les fichiers dont la date est dans [start, end] (bornes incluses)."""
    out = []
    for k in keys:
        parsed = parse_archive_key(k)
        if parsed and start <= parsed[1] <= end:
            out.append(k)
    return sorted(out)


def coverage_pct(n_days, start, end):
    """Part des jours de la période pour lesquels un fichier existe."""
    total = (end - start).days + 1
    return round(100 * n_days / total, 1) if total > 0 else 0.0


def parse_list_response(xml_text):
    """Extrait (clés, jeton de continuation) d'une réponse S3 ListObjectsV2."""
    root = ET.fromstring(xml_text)
    keys = [el.text for el in root.findall("s3:Contents/s3:Key", S3_NS)]
    token = root.findtext("s3:NextContinuationToken", default=None, namespaces=S3_NS)
    truncated = root.findtext("s3:IsTruncated", default="false", namespaces=S3_NS) == "true"
    return keys, (token if truncated else None)


# ---------------------------------------------------------------- accès HTTP à l'archive

class ArchiveClient:
    """Accès HTTP anonyme à l'archive S3, une session par thread, avec retries."""

    def __init__(self, user_agent, timeout=60, max_retries=4):
        self.headers = {"User-Agent": user_agent}
        self.timeout = timeout
        self.max_retries = max_retries
        self._local = threading.local()

    @property
    def session(self):
        if not hasattr(self._local, "session"):
            self._local.session = requests.Session()
            self._local.session.headers.update(self.headers)
        return self._local.session

    def _get(self, url, params=None):
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                if resp.status_code == 404:
                    return None
                if resp.status_code >= 500:
                    raise requests.HTTPError(f"HTTP {resp.status_code}")
                resp.raise_for_status()
                return resp
            except requests.RequestException as exc:
                if attempt == self.max_retries:
                    raise
                wait = 2 ** attempt
                log.warning("Erreur sur %s (%s), nouvel essai dans %ss", url, exc, wait)
                time.sleep(wait)
        return None

    def list_keys(self, prefix):
        """Toutes les clés sous un préfixe (pagination S3 gérée)."""
        keys, token = [], None
        while True:
            params = {"list-type": "2", "prefix": prefix}
            if token:
                params["continuation-token"] = token
            resp = self._get(f"{ARCHIVE_URL}/", params)
            if resp is None:
                return keys
            page, token = parse_list_response(resp.text)
            keys += page
            if not token:
                return keys

    def download(self, key):
        resp = self._get(f"{ARCHIVE_URL}/{key}")
        return resp.content if resp is not None else None


# ---------------------------------------------------------------- extraction

def load_locations(path=LOCATIONS_FILE):
    if not path.exists():
        raise FileNotFoundError(f"{path} introuvable : lancez d'abord "
                                "`python -m p3_pollution.explore --write-config`")
    return json.loads(path.read_text(encoding="utf-8"))["locations"]


def _list_location(client, loc, start, end):
    """Fichiers d'archive d'une station sur la période, prédécesseurs compris
    (une requête de listing par identifiant et par année)."""
    keys = []
    for archive_id in archive_ids(loc):
        for year in range(start.year, end.year + 1):
            keys += client.list_keys(archive_year_prefix(archive_id, year))
    return select_keys(keys, start, end)


def _copy_file(client, storage, archive_key):
    """Télécharge un fichier d'archive et le copie tel quel dans le bronze. Renvoie sa taille."""
    data = client.download(archive_key)
    if data is None:
        return 0
    storage.put_bytes(to_bronze_key(archive_key), data, content_type="application/gzip")
    return len(data)


def extract(client, storage, locations, start, end, workers=DEFAULT_WORKERS):
    """Copie dans le bronze toutes les archives de la période. Renvoie un bilan (dict)."""
    batch_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    t0 = time.monotonic()
    log.info("Extraction archives OpenAQ du %s au %s : %d stations, %d threads",
             start, end, len(locations), workers)

    # 1) inventaire : fichiers disponibles dans l'archive et déjà présents dans le bronze
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="liste") as pool:
        futures = {pool.submit(_list_location, client, loc, start, end): loc for loc in locations}
        available = {futures[f]["location_id"]: f.result() for f in as_completed(futures)}

    todo, per_location = [], {}
    for loc in locations:
        lid = loc["location_id"]
        already = {k for aid in archive_ids(loc) for k in storage.list(f"{BRONZE_PREFIX}/locationid={aid}/")}
        missing = [k for k in available[lid] if to_bronze_key(k) not in already]
        todo += missing
        per_location[lid] = {
            "ville_id": loc["ville_id"], "name": loc["name"],
            "archive_ids": archive_ids(loc),
            "files_available": len(available[lid]),
            "files_already_present": len(available[lid]) - len(missing),
            # jours couverts par au moins un des identifiants (actuel ou prédécesseur)
            "coverage_pct": coverage_pct(unique_days(available[lid]), start, end),
        }
    n_available = sum(len(v) for v in available.values())
    log.info("Inventaire : %d fichiers disponibles, %d déjà dans le bronze, %d à télécharger",
             n_available, n_available - len(todo), len(todo))

    # 2) téléchargement parallèle
    n_done, n_bytes, errors = 0, 0, []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dl") as pool:
        futures = {pool.submit(_copy_file, client, storage, k): k for k in todo}
        for fut in as_completed(futures):
            try:
                n_bytes += fut.result()
                n_done += 1
            except Exception as exc:  # un fichier en échec n'arrête pas l'extraction
                errors.append(futures[fut])
                log.error("Échec sur %s : %s", futures[fut], exc)
            if n_done and n_done % 2000 == 0:
                log.info("… %d / %d fichiers téléchargés", n_done, len(todo))

    duration = round(time.monotonic() - t0, 1)
    stats = {
        "source": "openaq-data-archive (AWS S3)",
        "batch_id": batch_id,
        "started_at": started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "period": {"start": start.isoformat(), "end": end.isoformat()},
        "files_available": n_available,
        "files_downloaded": n_done,
        "files_skipped": n_available - len(todo),
        "bytes_downloaded": n_bytes,
        "errors": errors,
        "duration_s": duration,
        "locations": per_location,
    }
    manifest_key = f"{BRONZE_PREFIX}/_manifests/{started_at:%Y%m%dT%H%M%SZ}_{batch_id[:8]}.json"
    storage.put_json(manifest_key, stats)

    low = [f"{v['ville_id']}/{lid} ({v['coverage_pct']} %)" for lid, v in per_location.items()
           if v["coverage_pct"] < 50]
    log.info("Extraction terminée : %d fichiers téléchargés (%.1f Mo), %d déjà présents, %d erreurs, %ss",
             n_done, n_bytes / 1e6, stats["files_skipped"], len(errors), duration)
    log.info("Manifeste : %s", manifest_key)
    if low:
        log.warning("Stations couvrant moins de 50 %% de la période : %s", ", ".join(low))
    return stats


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--years", type=int, default=5, help="nb d'années glissantes, en mois complets (défaut 5)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="téléchargements en parallèle")
    parser.add_argument("--locations", type=int, nargs="*", help="limiter à ces location_id (tests)")
    args = parser.parse_args()

    settings = get_settings()
    storage = get_storage(use_local=os.environ.get("STORAGE", "local").lower() != "minio")
    locations = load_locations()
    if args.locations:
        locations = [loc for loc in locations if loc["location_id"] in set(args.locations)]
    log.info("%d stations ; stockage : %s", len(locations), type(storage).__name__)

    start, end = default_period(args.years)
    client = ArchiveClient(settings.http_user_agent)
    try:
        extract(client, storage, locations, start, end, workers=args.workers)
    except KeyboardInterrupt:
        log.info("Arrêt demandé : relancez la commande pour reprendre (les fichiers présents sont sautés)")


if __name__ == "__main__":
    main()
