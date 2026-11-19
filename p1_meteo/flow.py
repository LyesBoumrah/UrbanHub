"""P1 — Flow Prefect batch (quotidien).

Responsable : P1.
"""

from __future__ import annotations

from prefect import flow, task

from common.log import setup_logging
from common.warehouse import Warehouse
from p1_meteo.cleaning import clean_all
from p1_meteo.ingestion import ingest
from p1_meteo.quality import check_quality, quality_summary

log = setup_logging(__name__)


@task(name="ingestion-noaa")
def ingest_task() -> list[str]:
    return ingest()


@task(name="nettoyage-silver")
def clean_task():
    return clean_all()


@task(name="qualite-p1")
def quality_task(df):
    checked = check_quality(df)
    summary = quality_summary(checked)
    log.info("Qualité P1 : %s", summary)
    return checked


@task(name="chargement-clickhouse")
def load_task(df) -> None:
    if df.empty:
        log.warning("Aucune donnée à charger dans ClickHouse.")
        return
    wh = Warehouse()
    wh.insert_df("meteo_obs", df)


@flow(name="p1-meteo-batch")
def p1_meteo_flow() -> None:
    """Pipeline complet : téléchargement → nettoyage → qualité → ClickHouse."""
    ingest_task()
    df = clean_task()
    df = quality_task(df)
    load_task(df)


if __name__ == "__main__":
    p1_meteo_flow()
