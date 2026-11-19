"""P1 — Pipeline batch météo (quotidien, sans dépendance Prefect).

Responsable : P1.
"""

from __future__ import annotations

from common.log import setup_logging
from common.warehouse import Warehouse
from p1_meteo.cleaning import clean_all
from p1_meteo.ingestion import ingest
from p1_meteo.quality import check_quality, quality_summary

log = setup_logging(__name__)


def ingest_task() -> list[str]:
    return ingest()


def clean_task():
    return clean_all()


def quality_task(df):
    checked = check_quality(df)
    summary = quality_summary(checked)
    log.info("Qualité P1 : %s", summary)
    return checked


def load_task(df) -> None:
    if df.empty:
        log.warning("Aucune donnée à charger dans ClickHouse.")
        return
    wh = Warehouse()
    wh.insert_df("meteo_obs", df)


def p1_meteo_flow() -> None:
    """Pipeline complet : téléchargement → nettoyage → qualité → ClickHouse."""
    ingest_task()
    df = clean_task()
    df = quality_task(df)
    load_task(df)


if __name__ == "__main__":
    p1_meteo_flow()
