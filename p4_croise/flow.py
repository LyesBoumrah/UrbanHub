"""P4 — Orchestrateur de rafraîchissement de la couche Gold JSON (météo × vélos × pollution).

Exécute :
  1. Lecture des Parquet Silver (`silver/meteo/`, `silver/velos/`, `silver/pollution/`)
  2. Génération des fichiers JSON métier dans `gold/` (P1 à P4 + synthèse)
  3. Détection des épisodes d'interaction critique et journalisation des alertes multi-facteurs

Usage :
  python -m p4_croise.flow [--local] [--loop --interval 3600]

Responsable : commun / P4.
"""

from __future__ import annotations

import argparse
import time
from typing import Any

from common.alerts import emit_alert
from common.log import get_logger
from common.storage import get_storage
from common.warehouse import try_get_warehouse
from p4_croise.analytics import strong_tripartite_interactions
from p4_croise.build_gold import build_gold, load_gold_df

log = get_logger("p4.flow")


def p4_gold_flow(use_local: bool = False) -> dict[str, Any]:
    """Construit la couche Gold JSON et émet les alertes d'interaction urbaine tripartite."""
    t0 = time.monotonic()
    stats = build_gold(use_local=use_local)
    gold_df = load_gold_df(use_local=use_local)

    alerts = 0
    if not gold_df.empty:
        inter = strong_tripartite_interactions(gold_df, min_score=65.0)
        episodes = inter["episodes"]
        if not episodes.empty:
            storage = get_storage(use_local=use_local)
            warehouse = None if use_local else try_get_warehouse()
            top_ep = episodes.iloc[0]
            emit_alert(
                pipeline="p4_croise",
                niveau="WARNING",
                ville_id=str(top_ep["ville_id"]),
                message=(
                    f"{len(episodes)} épisodes d'interaction forte météo×mobilité×pollution détectés "
                    f"(max {top_ep['score_interaction_max']}/100 sur {top_ep['ville_id']})"
                ),
                details={
                    "nb_episodes": len(episodes),
                    "regime_dominant": str(top_ep["regime_dominant"]),
                    "score_max": float(top_ep["score_interaction_max"]),
                },
                storage=storage,
                warehouse=warehouse,
            )
            alerts += 1

    result = {
        **stats,
        "alerts_emitted": alerts,
        "duration_s": round(time.monotonic() - t0, 2),
    }
    log.info("Pipeline P4 Gold terminé : %s", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="P4 — Pipeline de rafraîchissement de la couche Gold JSON")
    parser.add_argument("--local", action="store_true", help="Exécuter en mode local (data/) au lieu de MinIO")
    parser.add_argument("--loop", action="store_true", help="Rafraîchir la couche Gold toutes les heures")
    parser.add_argument("--interval", type=int, default=3600)
    args = parser.parse_args()

    if args.loop:
        while True:
            try:
                p4_gold_flow(use_local=args.local)
            except Exception as exc:  # noqa: BLE001
                log.exception("Erreur sur le cycle Gold P4 : %s", exc)
            time.sleep(max(10, args.interval))
    else:
        p4_gold_flow(use_local=args.local)


if __name__ == "__main__":
    main()
