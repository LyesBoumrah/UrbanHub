"""Écriture des anomalies de pipeline dans la table pipeline_alertes (INFO / WARNING / CRITICAL).

Responsable : commun.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from common.log import get_logger
from common.warehouse import ClickHouseWarehouse

logger = get_logger(__name__)

VALID_LEVELS = frozenset({"INFO", "WARNING", "CRITICAL"})


def emit_alert(
    pipeline: str,
    niveau: str,
    message: str,
    ville_id: str = "",
    details: dict[str, Any] | None = None,
    warehouse: ClickHouseWarehouse | None = None,
) -> dict[str, Any]:
    """Journalise une alerte et l'enregistre dans urbanhub.pipeline_alertes si ClickHouse est fourni."""
    level = niveau.upper()
    if level not in VALID_LEVELS:
        raise ValueError(f"Niveau d'alerte invalide: {niveau!r} (attendu: {sorted(VALID_LEVELS)})")

    now_utc = datetime.now(timezone.utc)
    details_json = json.dumps(details or {}, ensure_ascii=False)
    alert_record = {
        "ts_utc": now_utc,
        "pipeline": pipeline,
        "ville_id": ville_id,
        "niveau": level,
        "message": message,
        "details_json": details_json,
    }

    log_fn = {
        "INFO": logger.info,
        "WARNING": logger.warning,
        "CRITICAL": logger.error,
    }[level]
    log_fn("[%s][%s] %s | %s", pipeline, ville_id or "global", message, details_json)

    if warehouse is not None:
        try:
            warehouse.insert_rows(
                table="urbanhub.pipeline_alertes",
                rows=[
                    (
                        now_utc,
                        pipeline,
                        ville_id,
                        level,
                        message,
                        details_json,
                    )
                ],
                column_names=["ts_utc", "pipeline", "ville_id", "niveau", "message", "details_json"],
            )
        except Exception as exc:
            logger.warning("Impossible d'écrire l'alerte dans ClickHouse : %s", exc)

    return alert_record
