"""Écriture des anomalies de pipeline dans pipeline_alertes (INFO / WARNING / CRITICAL).

Responsable : commun.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from .warehouse import Warehouse


def write_alert(
    level: str,
    source: str,
    message: str,
    warehouse: Warehouse | None = None,
) -> None:
    """Insère une alerte dans ClickHouse.

    level : INFO, WARNING ou CRITICAL.
    """
    wh = warehouse or Warehouse()
    df = pd.DataFrame(
        [
            {
                "ts": datetime.now(timezone.utc),
                "level": level.upper(),
                "source": source,
                "message": message,
            }
        ]
    )
    wh.insert_df("pipeline_alertes", df)
