"""Initialise le Data Lake MinIO (création des 3 buckets bronze, silver et gold) et vérifie le référentiel villes.

Responsable : commun.
"""

from __future__ import annotations

from common.config import load_ref_ville
from common.log import get_logger
from common.storage import get_storage

logger = get_logger(__name__)


def init_datalake(use_local: bool = False) -> None:
    """Crée les 3 buckets MinIO (`bronze`, `silver`, `gold`) ou les dossiers locaux correspondants."""
    villes = load_ref_ville()
    logger.info("Référentiel chargé : %d villes françaises configurées", len(villes))

    storage = get_storage(use_local=use_local)
    storage.ensure_bucket()
    if use_local:
        target = str(storage.base_dir)
    else:
        buckets = getattr(storage, "buckets", {"bronze": "bronze", "silver": "silver", "gold": "gold"})
        target = ", ".join(f"s3://{b}" for b in buckets.values())
    logger.info("Data Lake prêt (3 buckets Medallion : bronze brut / silver Parquet / gold JSON) : %s", target)


if __name__ == "__main__":
    init_datalake()
