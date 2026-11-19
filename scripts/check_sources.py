"""Vérifie l'accessibilité des sources CityBikes pour les villes de config/ref_ville.json.

Responsable : commun.
"""

from __future__ import annotations

import argparse
import sys

import requests

from common.config import get_settings, load_ref_ville
from common.log import get_logger

logger = get_logger(__name__)
CITYBIKES_BASE_URL = "https://api.citybik.es/v2/networks"


def check_citybikes_networks(core_only: bool = False, timeout: int = 15) -> bool:
    """Vérifie chaque réseau CityBikes configuré dans config/ref_ville.json."""
    settings = get_settings()
    headers = {"User-Agent": settings.http_user_agent}
    villes = load_ref_ville(core_only=core_only)

    all_ok = True
    for ville in villes:
        cb = ville.get("citybikes") or {}
        network_id = cb.get("network_id")
        if not network_id:
            logger.warning("[%s] Aucun réseau CityBikes configuré", ville["ville_id"])
            all_ok = False
            continue

        url = f"{CITYBIKES_BASE_URL}/{network_id}"
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            stations = resp.json().get("network", {}).get("stations", [])
            if not stations:
                logger.warning("[%s] Réseau %s accessible mais 0 station", ville["ville_id"], network_id)
                all_ok = False
            else:
                logger.info(
                    "[%s] OK — réseau %s (%s) : %d stations",
                    ville["ville_id"],
                    network_id,
                    cb.get("network_name", ""),
                    len(stations),
                )
        except Exception as exc:
            logger.error("[%s] Erreur sur %s : %s", ville["ville_id"], url, exc)
            all_ok = False

    return all_ok


def main() -> int:
    parser = argparse.ArgumentParser(description="Vérification des sources CityBikes UrbanHub")
    parser.add_argument("--core-only", action="store_true", help="Vérifier uniquement les 5 villes cœur")
    args = parser.parse_args()
    ok = check_citybikes_networks(core_only=args.core_only)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
