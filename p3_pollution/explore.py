"""P3 — Exploration de la couverture OpenAQ autour des villes de config/villes.json.

Pour chaque ville :
  1. liste les sites de mesure (locations) dans un rayon donné ;
  2. garde les sites fixes et actifs (dernière mesure récente) ;
  3. vérifie, capteur par capteur, la fraîcheur des 5 polluants cibles via /latest ;
  4. propose une sélection de sites couvrant un maximum de polluants.

Sorties :
  - un rapport dans la console (couverture ville × polluant, unités, sélection proposée) ;
  - data/explore/openaq_couverture.json (détail complet, non versionné).

Usage : python -m p3_pollution.explore [--radius 15000] [--max-age-hours 48] [--max-sites 5]

Responsable : P3.
"""
import argparse
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

from p3_pollution.openaq_client import OpenAQClient

ROOT = Path(__file__).resolve().parents[1]
POLLUTANTS = ("pm25", "pm10", "no2", "o3", "co")

log = logging.getLogger("p3.explore")


# ---------------------------------------------------------------- fonctions pures (testées)

def parse_utc(value):
    """'2024-01-01T00:00:00Z' -> datetime UTC timezone-aware (None si absent)."""
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def is_active(location, now, max_age):
    """Site fixe dont la dernière mesure date de moins de max_age."""
    if location.get("isMobile"):
        return False
    last = parse_utc((location.get("datetimeLast") or {}).get("utc"))
    return last is not None and now - last <= max_age


def target_sensors(location):
    """Capteurs du site mesurant un des 5 polluants cibles."""
    out = []
    for s in location.get("sensors", []):
        param = s.get("parameter", {})
        if param.get("name") in POLLUTANTS:
            out.append({"sensor_id": s["id"], "pollutant": param["name"], "unit": param.get("units")})
    return out


def propose_selection(sites, max_sites):
    """Sélection gloutonne : d'abord les sites qui apportent de nouveaux polluants,
    puis on complète avec les sites les plus riches (utile pour comparer des zones).

    `sites` : liste de dicts avec au moins 'location_id', 'distance_m', 'fresh_pollutants' (set).
    """
    candidates = [s for s in sites if s["fresh_pollutants"]]
    candidates.sort(key=lambda s: (-len(s["fresh_pollutants"]), s["distance_m"]))
    selected, covered = [], set()

    # 1) couverture maximale des polluants
    while len(selected) < max_sites:
        best = max(
            (s for s in candidates if s not in selected),
            key=lambda s: (len(s["fresh_pollutants"] - covered), len(s["fresh_pollutants"]), -s["distance_m"]),
            default=None,
        )
        if best is None or not (best["fresh_pollutants"] - covered):
            break
        selected.append(best)
        covered |= best["fresh_pollutants"]

    # 2) compléter avec les sites restants les plus riches
    for s in candidates:
        if len(selected) >= max_sites:
            break
        if s not in selected:
            selected.append(s)
    return selected


# ---------------------------------------------------------------- appels API

def explore_city(client, ville, radius_m, now, max_age, monitors_only=True):
    """Renvoie la liste des sites actifs de la ville avec la fraîcheur de chaque capteur cible.

    monitors_only : ne garder que les stations de référence (réseaux officiels, isMonitor=true)
    et écarter les capteurs low-cost (AirGradient, Clarity...), moins fiables.
    """
    params = {"coordinates": f"{ville['lat']},{ville['lon']}", "radius": radius_m}  # ordre lat,lon vérifié
    if monitors_only:
        params["monitor"] = "true"
    locations = list(client.paginate("/locations", params))
    active = [loc for loc in locations
              if is_active(loc, now, max_age) and target_sensors(loc)
              and (loc.get("isMonitor") or not monitors_only)]
    log.info("%s : %d sites trouvés, %d actifs avec polluants cibles", ville["nom"], len(locations), len(active))

    sites = []
    for loc in active:
        latest = client.get(f"/locations/{loc['id']}/latest").get("results", [])
        last_by_sensor = {m["sensorsId"]: parse_utc(m["datetime"]["utc"]) for m in latest}
        sensors = []
        for s in target_sensors(loc):
            last = last_by_sensor.get(s["sensor_id"])
            s["last_utc"] = last.isoformat() if last else None
            s["fresh"] = last is not None and now - last <= max_age
            sensors.append(s)
        sites.append({
            "location_id": loc["id"],
            "name": loc["name"],
            "provider": (loc.get("provider") or {}).get("name"),
            "latitude": loc["coordinates"]["latitude"],
            "longitude": loc["coordinates"]["longitude"],
            "distance_m": round(loc.get("distance") or 0),
            "sensors": sensors,
            "fresh_pollutants": {s["pollutant"] for s in sensors if s["fresh"]},
        })
    return len(locations), sites


# ---------------------------------------------------------------- rapport

def print_report(report):
    header = f"{'Ville':<11}{'trouvés':>8}{'actifs':>7}  " + "".join(f"{p:>6}" for p in POLLUTANTS)
    print("\n=== Couverture OpenAQ (nb de capteurs frais par polluant) ===")
    print(header)
    print("-" * len(header))
    for v in report:
        counts = {p: sum(1 for s in v["sites"] for x in s["sensors"] if x["pollutant"] == p and x["fresh"])
                  for p in POLLUTANTS}
        print(f"{v['nom']:<11}{v['n_found']:>8}{len(v['sites']):>7}  " + "".join(f"{counts[p]:>6}" for p in POLLUTANTS))

    units = {}
    for v in report:
        for s in v["sites"]:
            for x in s["sensors"]:
                units.setdefault(x["pollutant"], set()).add(x["unit"])
    print("\n=== Unités rencontrées ===")
    for p in POLLUTANTS:
        print(f"  {p:<5}: {', '.join(sorted(u or '?' for u in units.get(p, []))) or '(aucun capteur)'}")

    print("\n=== Sélection proposée ===")
    for v in report:
        print(f"\n{v['nom']} :")
        if not v["selection"]:
            print("  (aucun site exploitable)")
        for s in v["selection"]:
            pol = ", ".join(p for p in POLLUTANTS if p in s["fresh_pollutants"])
            print(f"  - {s['location_id']:>8}  {s['name'][:35]:<35} {s['distance_m'] / 1000:5.1f} km  [{pol}]")


def build_locations_config(report, now, criteria):
    """Construit le contenu de config/openaq_locations.json à partir de la sélection.

    Seuls les capteurs frais des polluants cibles sont retenus.
    """
    locations = []
    for v in report:
        for s in v["selection"]:
            locations.append({
                "ville_id": v["ville_id"],
                "location_id": s["location_id"],
                "name": s["name"],
                "provider": s["provider"],
                "latitude": s["latitude"],
                "longitude": s["longitude"],
                "distance_m": s["distance_m"],
                "sensors": [{"sensor_id": x["sensor_id"], "pollutant": x["pollutant"], "unit": x["unit"]}
                            for x in s["sensors"] if x["fresh"]],
            })
    return {"generated_at": now.isoformat(), "criteria": criteria, "locations": locations}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--radius", type=int, default=15000, help="rayon de recherche en mètres (max 25000)")
    parser.add_argument("--max-age-hours", type=int, default=48, help="âge max de la dernière mesure")
    parser.add_argument("--max-sites", type=int, default=5, help="nb max de sites proposés par ville")
    parser.add_argument("--include-low-cost", action="store_true",
                        help="inclure les capteurs low-cost (par défaut : stations de référence uniquement)")
    parser.add_argument("--write-config", action="store_true",
                        help="figer la sélection dans config/openaq_locations.json (lu par ingestion.py)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s : %(message)s")
    load_dotenv(ROOT / ".env")
    client = OpenAQClient(os.environ.get("OPENAQ_API_KEY"))

    villes = json.loads((ROOT / "config" / "villes.json").read_text(encoding="utf-8"))
    now = datetime.now(timezone.utc)
    max_age = timedelta(hours=args.max_age_hours)

    report = []
    for ville in villes:
        n_found, sites = explore_city(client, ville, args.radius, now, max_age,
                                      monitors_only=not args.include_low_cost)
        report.append({**ville, "n_found": n_found, "sites": sites,
                       "selection": propose_selection(sites, args.max_sites)})

    print_report(report)

    out = ROOT / "data" / "explore" / "openaq_couverture.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    serializable = json.loads(json.dumps(report, default=lambda o: sorted(o) if isinstance(o, set) else str(o)))
    serializable = {"generated_at": now.isoformat(), "radius_m": args.radius, "villes": serializable}
    out.write_text(json.dumps(serializable, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("Détail écrit dans %s (%d requêtes API)", out.relative_to(ROOT), client.n_requests)

    if args.write_config:
        criteria = {"radius_m": args.radius, "max_age_hours": args.max_age_hours,
                    "max_sites": args.max_sites, "monitors_only": not args.include_low_cost}
        config = build_locations_config(report, now, criteria)
        cfg = ROOT / "config" / "openaq_locations.json"
        cfg.write_text(json.dumps(config, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        n_sensors = sum(len(loc["sensors"]) for loc in config["locations"])
        log.info("Sélection figée dans %s : %d sites, %d capteurs",
                 cfg.relative_to(ROOT), len(config["locations"]), n_sensors)


if __name__ == "__main__":
    main()
