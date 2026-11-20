"""Initialise le Data Lakehouse UrbanHub : 3 buckets MinIO (`bronze`, `silver`, `gold`) + schéma ClickHouse (`sql/`).

Exécute de manière idempotente :
  1. Création des 3 buckets MinIO Medallion (`bronze`, `silver`, `gold`)
  2. Exécution des scripts DDL ClickHouse dans l'ordre (`sql/00_commun.sql` -> `sql/40_gold.sql`)
  3. Alimentation / mise à jour de `urbanhub.ref_ville` depuis `config/ref_ville.json`

Usage :
  python scripts/init_warehouse.py

Responsable : commun.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from common.config import PROJECT_ROOT, load_ref_ville
from common.log import get_logger
from common.storage import get_storage
from common.warehouse import ClickHouseWarehouse, get_warehouse

logger = get_logger(__name__)

SQL_DIR = PROJECT_ROOT / "sql"


def split_sql_statements(sql_text: str) -> list[str]:
    """Découpe un fichier SQL en instructions individuelles (ignore commentaires `--` et blocs vides)."""
    cleaned_lines = [line for line in sql_text.splitlines() if not line.strip().startswith("--")]
    raw = "\n".join(cleaned_lines)
    return [stmt.strip() for stmt in raw.split(";") if stmt.strip()]


def apply_sql_files(warehouse: ClickHouseWarehouse, sql_dir: Path = SQL_DIR) -> list[str]:
    """Exécute tous les fichiers `sql/*.sql` par ordre alphabétique dans ClickHouse."""
    executed: list[str] = []
    for sql_file in sorted(sql_dir.glob("*.sql")):
        statements = split_sql_statements(sql_file.read_text(encoding="utf-8"))
        for stmt in statements:
            warehouse.execute(stmt)
        executed.append(sql_file.name)
        logger.info("DDL ClickHouse appliqué : %s (%d instructions)", sql_file.name, len(statements))
    return executed


def seed_ref_ville(warehouse: ClickHouseWarehouse, villes: list[dict[str, Any]] | None = None) -> int:
    """Insère ou met à jour le référentiel des 12 métropoles françaises dans `urbanhub.ref_ville`."""
    entries = villes if villes is not None else load_ref_ville()
    now_utc = datetime.now(timezone.utc)
    rows = []
    for v in entries:
        bbox = v.get("bbox") or {}
        cb = v.get("citybikes") or {}
        rows.append((
            str(v["ville_id"]),
            str(v.get("nom", v["ville_id"])),
            1 if v.get("core_urbanhub") else 0,
            float(v.get("lat", 0.0)),
            float(v.get("lon", 0.0)),
            float(bbox.get("lat_min", 0.0)),
            float(bbox.get("lat_max", 0.0)),
            float(bbox.get("lon_min", 0.0)),
            float(bbox.get("lon_max", 0.0)),
            str(cb.get("network_id", "")),
            str(cb.get("network_name", "")),
            str(v.get("noaa_station_id", "")),
            now_utc,
        ))
    cols = [
        "ville_id", "nom", "core_urbanhub", "latitude", "longitude",
        "bbox_lat_min", "bbox_lat_max", "bbox_lon_min", "bbox_lon_max",
        "citybikes_network", "citybikes_name", "noaa_station_id", "updated_at_utc",
    ]
    count = warehouse.insert_rows("urbanhub.ref_ville", rows, cols)
    logger.info("Référentiel ClickHouse urbanhub.ref_ville synchronisé (%d villes)", count)
    return count


def init_lakehouse(
    use_local: bool = False,
    storage: Any = None,
    warehouse: ClickHouseWarehouse | None = None,
) -> dict[str, Any]:
    """Initialise le Data Lake MinIO (3 buckets) puis l'entrepôt ClickHouse (DDL + ref_ville)."""
    villes = load_ref_ville()
    logger.info("Référentiel chargé : %d villes françaises configurées", len(villes))

    st = storage if storage is not None else get_storage(use_local=use_local)
    st.ensure_bucket()

    sql_files: list[str] = []
    ch_ready = False
    if not use_local:
        wh = warehouse if warehouse is not None else get_warehouse(database="")
        if wh.ping():
            sql_files = apply_sql_files(wh)
            seed_ref_ville(wh, villes=villes)
            ch_ready = True
        else:
            logger.warning("ClickHouse injoignable : seul le Data Lake MinIO a été initialisé.")

    return {
        "villes": len(villes),
        "clickhouse_ready": ch_ready,
        "sql_files": sql_files,
    }


if __name__ == "__main__":
    init_lakehouse()
