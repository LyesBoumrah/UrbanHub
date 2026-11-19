"""P3 — Ingestion OpenAQ v3 vers la couche bronze (flux IoT simulé).

Deux modes :
  * latest (défaut)  : /locations/{id}/latest pour chaque site de config/openaq_locations.json.
                       C'est le flux IoT « temps réel », à lancer chaque heure (Prefect, ou --loop).
  * backfill         : /sensors/{id}/hours pour récupérer l'historique horaire.
                       Découpé par capteur × année et REPRENABLE : les années passées déjà
                       téléchargées sont sautées ; l'année en cours est toujours mise à jour.

Chaque mesure devient un message capteur individuel :
  {source, mode, fetched_at, batch_id, ville_id, location_id, sensor_id, pollutant, unit, payload}
où `payload` est la mesure brute, intacte, telle que renvoyée par l'API.

Clés bronze :
  bronze/openaq/<location_id>/date=YYYY-MM-DD/<HHMMSS>.json        (latest, 1 fichier par site et par run)
  bronze/openaq_backfill/<sensor_id>/year=YYYY/full.json.gz        (backfill, année civile complète)
  bronze/openaq_backfill/<sensor_id>/year=YYYY/from=..._to=....json.gz  (backfill, tranche partielle)

Certains réseaux publient par lots journaliers (retard ~27 h) : /latest n'en voit que la dernière
valeur. Lancer chaque jour un petit rattrapage `--backfill --days 2` comble ces trous
(les doublons sont éliminés au nettoyage, clé (sensor_id, ts)).

Usage :
  python -m p3_pollution.ingestion                       # une collecte latest
  python -m p3_pollution.ingestion --loop                # collecte latest toutes les heures
  python -m p3_pollution.ingestion --backfill --days 7   # historique des 7 derniers jours
  python -m p3_pollution.ingestion --backfill --years 5  # historique des 5 dernières années

Responsable : P3.
"""
import argparse
import json
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

from p3_pollution.openaq_client import OpenAQClient
from p3_pollution.storage import storage_from_env

ROOT = Path(__file__).resolve().parents[1]
LOCATIONS_FILE = ROOT / "config" / "openaq_locations.json"
# Délais de publication observés (19/11/2026) : ~2 h pour la plupart des capteurs, mais ~12 h et ~27 h
# pour certains réseaux qui livrent par lots journaliers. Au-delà de 30 h, le capteur est considéré muet.
SILENT_AFTER = timedelta(hours=30)

# Parallélisme : la vitesse est plafonnée par le quota OpenAQ (60 req/min, 2 000 req/h), pas par le code.
# 4 threads suffisent pour l'atteindre sur le backfill (pages de 1 000 h, ~3 s par requête).
DEFAULT_WORKERS = 4

log = logging.getLogger("p3.ingestion")


# ---------------------------------------------------------------- fonctions pures (testées)

def utc_iso(dt):
    """datetime UTC -> '2026-11-19T13:00:05Z'."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def make_message(mode, fetched_at, batch_id, location, sensor, payload):
    """Enveloppe d'un message capteur (le payload reste brut)."""
    return {
        "source": "openaq",
        "mode": mode,
        "fetched_at": utc_iso(fetched_at),
        "batch_id": batch_id,
        "ville_id": location["ville_id"],
        "location_id": location["location_id"],
        "sensor_id": sensor["sensor_id"],
        "pollutant": sensor["pollutant"],
        "unit": sensor["unit"],
        "payload": payload,
    }


def build_latest_messages(location, latest_results, fetched_at, batch_id):
    """Transforme la réponse /latest d'un site en messages capteurs.

    Les capteurs absents de la configuration (autres polluants : so2, no...) sont ignorés.
    """
    sensors = {s["sensor_id"]: s for s in location["sensors"]}
    return [make_message("latest", fetched_at, batch_id, location, sensors[m["sensorsId"]], m)
            for m in latest_results if m.get("sensorsId") in sensors]


def silent_sensors(location, latest_results, now, max_silence=SILENT_AFTER):
    """Capteurs configurés sans mesure récente (ou absents de /latest)."""
    last = {m.get("sensorsId"): parse_utc((m.get("datetime") or {}).get("utc")) for m in latest_results}
    return [s["sensor_id"] for s in location["sensors"]
            if last.get(s["sensor_id"]) is None or now - last[s["sensor_id"]] > max_silence]


def latest_key(location_id, fetched_at):
    return f"bronze/openaq/{location_id}/date={fetched_at:%Y-%m-%d}/{fetched_at:%H%M%S}.json"


def backfill_key(sensor_id, year, lo, hi):
    """Année complète → nom fixe (sert de marqueur de reprise) ; tranche partielle → nom daté,
    pour qu'un petit rattrapage (--days 2) n'écrase jamais une année complète déjà téléchargée."""
    prefix = f"bronze/openaq_backfill/{sensor_id}/year={year}"
    if is_complete_year(year, lo, hi):
        return f"{prefix}/full.json.gz"
    return f"{prefix}/from={lo:%Y%m%dT%H%M}_to={hi:%Y%m%dT%H%M}.json.gz"


def year_chunks(start, end):
    """Découpe [start, end) en tranches par année civile : [(année, début, fin), ...]."""
    chunks = []
    for year in range(start.year, end.year + 1):
        lo = max(start, datetime(year, 1, 1, tzinfo=timezone.utc))
        hi = min(end, datetime(year + 1, 1, 1, tzinfo=timezone.utc))
        if lo < hi:
            chunks.append((year, lo, hi))
    return chunks


def is_complete_year(year, lo, hi):
    """La tranche couvre-t-elle l'année civile entière ? (seules celles-ci sont sautées à la reprise)."""
    return (lo == datetime(year, 1, 1, tzinfo=timezone.utc)
            and hi == datetime(year + 1, 1, 1, tzinfo=timezone.utc))


# ---------------------------------------------------------------- collecte

def load_locations(path=LOCATIONS_FILE):
    if not path.exists():
        raise FileNotFoundError(f"{path} introuvable : lancez d'abord "
                                "`python -m p3_pollution.explore --write-config`")
    return json.loads(path.read_text(encoding="utf-8"))["locations"]


def _latest_one_site(client, storage, loc, now, batch_id):
    """Collecte /latest d'UN site (exécutée dans un thread). Renvoie (nb messages, capteurs muets)."""
    results = client.get(f"/locations/{loc['location_id']}/latest").get("results", [])
    messages = build_latest_messages(loc, results, now, batch_id)
    if messages:
        storage.write_json(latest_key(loc["location_id"], now), messages)
    return len(messages), silent_sensors(loc, results, now)


def collect_latest(client, storage, locations, now=None, workers=DEFAULT_WORKERS):
    """Une collecte /latest de tous les sites, en parallèle. Renvoie un bilan (dict)."""
    now = now or datetime.now(timezone.utc)
    batch_id = str(uuid.uuid4())
    stats = {"batch_id": batch_id, "sites_ok": 0, "sites_ko": [], "messages": 0, "silent_sensors": []}
    t0 = time.monotonic()

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="latest") as pool:
        futures = {pool.submit(_latest_one_site, client, storage, loc, now, batch_id): loc for loc in locations}
        for fut in as_completed(futures):  # les bilans sont agrégés dans le thread principal
            loc = futures[fut]
            try:
                n_messages, silent = fut.result()
            except Exception as exc:  # un site en échec ne doit pas arrêter la collecte
                log.error("Site %s (%s) en échec : %s", loc["location_id"], loc["name"], exc)
                stats["sites_ko"].append(loc["location_id"])
                continue
            stats["messages"] += n_messages
            stats["sites_ok"] += 1
            stats["silent_sensors"] += silent

    stats["duration_s"] = round(time.monotonic() - t0, 1)
    log.info("Collecte latest terminée : %d sites OK, %d en échec, %d messages, %d capteurs muets, %ss",
             stats["sites_ok"], len(stats["sites_ko"]), stats["messages"],
             len(stats["silent_sensors"]), stats["duration_s"])
    if stats["silent_sensors"]:
        log.warning("Capteurs muets (> %s sans mesure) : %s", SILENT_AFTER, stats["silent_sensors"])
    return stats


def _backfill_one_sensor(client, storage, loc, sensor, start, end, batch_id):
    """Backfill d'UN capteur, année par année (exécuté dans un thread).

    Un même capteur n'est traité que par un seul thread : la reprise par année reste simple.
    Renvoie un bilan partiel (dict).
    """
    sid = sensor["sensor_id"]
    res = {"files_written": 0, "files_skipped": 0, "messages": 0, "errors": []}
    for year, lo, hi in year_chunks(start, end):
        key = backfill_key(sid, year, lo, hi)
        if is_complete_year(year, lo, hi) and storage.exists(key):
            res["files_skipped"] += 1
            continue
        try:
            params = {"datetime_from": utc_iso(lo), "datetime_to": utc_iso(hi)}
            results = list(client.paginate(f"/sensors/{sid}/hours", params, limit=1000))
        except Exception as exc:
            log.error("Capteur %s, année %s en échec : %s", sid, year, exc)
            res["errors"].append((sid, year))
            continue

        fetched_at = datetime.now(timezone.utc)
        messages = [make_message("backfill", fetched_at, batch_id, loc, sensor, r) for r in results]
        # fichier écrit même vide : marque l'année comme traitée pour la reprise
        storage.write_json(key, messages, compress=True)
        res["files_written"] += 1
        res["messages"] += len(messages)
    return res


def backfill(client, storage, locations, start, end=None, workers=DEFAULT_WORKERS):
    """Historique horaire /sensors/{id}/hours, par capteur × année, reprenable, en parallèle."""
    end = end or datetime.now(timezone.utc)
    batch_id = str(uuid.uuid4())
    sensors = [(loc, s) for loc in locations for s in loc["sensors"]]
    stats = {"batch_id": batch_id, "files_written": 0, "files_skipped": 0, "messages": 0,
             "errors": [], "empty_sensors": []}
    t0 = time.monotonic()
    log.info("Backfill du %s au %s : %d capteurs, %d threads", utc_iso(start), utc_iso(end),
             len(sensors), workers)

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="backfill") as pool:
        futures = {pool.submit(_backfill_one_sensor, client, storage, loc, s, start, end, batch_id): (loc, s)
                   for loc, s in sensors}
        for i, fut in enumerate(as_completed(futures), 1):
            loc, sensor = futures[fut]
            sid = sensor["sensor_id"]
            try:
                res = fut.result()
            except Exception as exc:  # erreur inattendue (ex. écriture disque) : on continue
                log.error("Capteur %s en échec : %s", sid, exc)
                stats["errors"].append((sid, None))
                continue
            for k in ("files_written", "files_skipped", "messages"):
                stats[k] += res[k]
            stats["errors"] += res["errors"]
            if res["messages"] == 0 and res["files_skipped"] == 0:
                stats["empty_sensors"].append(sid)
            log.info("[%d/%d] capteur %s (%s, %s) : %d mesures, %d années sautées", i, len(sensors), sid,
                     loc["ville_id"], sensor["pollutant"], res["messages"], res["files_skipped"])

    stats["duration_s"] = round(time.monotonic() - t0, 1)
    log.info("Backfill terminé : %d fichiers écrits, %d sautés (déjà présents), %d mesures, "
             "%d erreurs, %d requêtes API, %ss",
             stats["files_written"], stats["files_skipped"], stats["messages"],
             len(stats["errors"]), client.n_requests, stats["duration_s"])
    return stats


def run_loop(client, storage, locations, interval_s=3600, offset_s=300, workers=DEFAULT_WORKERS):
    """Collecte latest en boucle, calée sur l'heure pleine + offset (en attendant Prefect)."""
    log.info("Mode boucle : collecte toutes les %ss (Ctrl+C pour arrêter)", interval_s)
    while True:
        collect_latest(client, storage, locations, workers=workers)
        wait = interval_s - (time.time() % interval_s) + offset_s
        log.info("Prochaine collecte dans %d min", wait // 60)
        time.sleep(wait)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--loop", action="store_true", help="collecte latest toutes les heures")
    parser.add_argument("--backfill", action="store_true", help="récupérer l'historique horaire")
    period = parser.add_mutually_exclusive_group()
    period.add_argument("--days", type=int, help="backfill : nb de jours d'historique")
    period.add_argument("--years", type=int, help="backfill : nb d'années d'historique (ex. 5)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"nb de threads en parallèle (défaut {DEFAULT_WORKERS} ; 1 = séquentiel)")
    args = parser.parse_args()

    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(threadName)s %(name)s : %(message)s")
    load_dotenv(ROOT / ".env")
    client = OpenAQClient(os.environ.get("OPENAQ_API_KEY"),
                          user_agent=os.environ.get("HTTP_USER_AGENT", "UrbanHub-school-project/0.1"),
                          workers=args.workers)
    storage = storage_from_env()
    locations = load_locations()
    log.info("%d sites, %d capteurs chargés ; stockage : %s", len(locations),
             sum(len(loc["sensors"]) for loc in locations), type(storage).__name__)

    try:
        if args.backfill:
            now = datetime.now(timezone.utc)
            if args.years:
                start = datetime(now.year - args.years, 1, 1, tzinfo=timezone.utc)  # années civiles entières
            else:
                start = now - timedelta(days=args.days or 7)
            backfill(client, storage, locations, start, now, workers=args.workers)
        elif args.loop:
            run_loop(client, storage, locations, workers=args.workers)
        else:
            collect_latest(client, storage, locations, workers=args.workers)
    except KeyboardInterrupt:
        log.info("Arrêt demandé par l'utilisateur")


if __name__ == "__main__":
    main()
