"""Journalisation structurée des anomalies de pipeline (INFO / WARNING / CRITICAL).

Les alertes sont journalisées dans les logs Python et peuvent être archivées en JSON dans
la couche `logs/` du Data Lake si un client `storage` est fourni.

Responsable : commun.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from common.log import get_logger

logger = get_logger(__name__)

VALID_LEVELS = frozenset({"INFO", "WARNING", "CRITICAL"})


def emit_alert(
    pipeline: str,
    niveau: str,
    message: str,
    ville_id: str = "",
    details: dict[str, Any] | None = None,
    storage: Any | None = None,
    warehouse: Any | None = None,
) -> dict[str, Any]:
    """Journalise une alerte structurée, l'archive en JSON (Data Lake) et/ou dans ClickHouse."""
    level = niveau.upper()
    if level not in VALID_LEVELS:
        raise ValueError(f"Niveau d'alerte invalide: {niveau!r} (attendu: {sorted(VALID_LEVELS)})")

    now_utc = datetime.now(timezone.utc)
    details_json = json.dumps(details or {}, ensure_ascii=False)
    alert_record = {
        "ts_utc": now_utc.isoformat(),
        "pipeline": pipeline,
        "ville_id": ville_id,
        "niveau": level,
        "message": message,
        "details": details or {},
    }

    log_fn = {
        "INFO": logger.info,
        "WARNING": logger.warning,
        "CRITICAL": logger.error,
    }[level]
    log_fn("[%s][%s] %s | %s", pipeline, ville_id or "global", message, details_json)

    if storage is not None:
        try:
            date_part = now_utc.strftime("%Y-%m-%d")
            stamp = now_utc.strftime("%H%M%S%f")
            key = f"gold/alertes/{pipeline}/dt={date_part}/{stamp}.json"
            storage.put_json(key, alert_record)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Impossible d'archiver l'alerte dans le Data Lake : %s", exc)

    if warehouse is not None:
        try:
            warehouse.insert_rows(
                table="urbanhub.pipeline_alertes",
                rows=[(now_utc, pipeline, ville_id, level, message, details_json)],
                column_names=["ts_utc", "pipeline", "ville_id", "niveau", "message", "details_json"],
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Impossible d'écrire l'alerte dans ClickHouse : %s", exc)

    return alert_record


def write_alert(
    level: str,
    source: str,
    message: str,
    storage: Any | None = None,
    warehouse: Any | None = None,
) -> dict[str, Any]:
    """Alias rétrocompatible P1 autour de `emit_alert`."""
    return emit_alert(pipeline=source, niveau=level, message=message, storage=storage, warehouse=warehouse)
