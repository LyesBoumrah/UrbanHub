"""Applique les fichiers sql/*.sql dans l'ordre et peuple urbanhub.ref_ville depuis config/ref_ville.json.

Responsable : commun.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from common.config import PROJECT_ROOT, load_ref_ville
from common.log import get_logger
from common.storage import get_storage
from common.warehouse import ClickHouseWarehouse, get_warehouse

logger = get_logger(__name__)


def split_sql_statements(sql_text: str) -> list[str]:
    """Découpe un fichier SQL en instructions individuelles (ignore les lignes purement commentaires)."""
    cleaned_lines: list[str] = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or not stripped:
            continue
        cleaned_lines.append(line)
    joined = "\n".join(cleaned_lines)
    return [stmt.strip() for stmt in joined.split(";") if stmt.strip()]


def seed_ref_ville(warehouse: ClickHouseWarehouse) -> int:
    """Insère / met à jour urbanhub.ref_ville à partir de config/ref_ville.json."""
    entries = load_ref_ville()
    now_utc = datetime.now(timezone.utc)
    rows = []
    for entry in entries:
        bbox = entry.get("bbox", {})
        cb = entry.get("citybikes") or {}
        rows.append(
            (
                entry["ville_id"],
                entry["nom"],
                1 if entry.get("core_urbanhub") else 0,
                float(entry["lat"]),
                float(entry["lon"]),
                float(bbox.get("lat_min", entry["lat"] - 0.15)),
                float(bbox.get("lat_max", entry["lat"] + 0.15)),
                float(bbox.get("lon_min", entry["lon"] - 0.20)),
                float(bbox.get("lon_max", entry["lon"] + 0.20)),
                cb.get("network_id", ""),
                cb.get("network_name", ""),
                entry.get("noaa_station_id") or "",
                now_utc,
            )
        )
    columns = [
        "ville_id",
        "nom",
        "core_urbanhub",
        "latitude",
        "longitude",
        "bbox_lat_min",
        "bbox_lat_max",
        "bbox_lon_min",
        "bbox_lon_max",
        "citybikes_network",
        "citybikes_name",
        "noaa_station_id",
        "updated_at_utc",
    ]
    return warehouse.insert_rows("urbanhub.ref_ville", rows, columns)


def init_warehouse(sql_dir: Path | None = None) -> None:
    """Exécute les scripts SQL par ordre alphabétique puis charge ref_ville."""
    target_dir = sql_dir or (PROJECT_ROOT / "sql")
    warehouse = get_warehouse(database="")

    for sql_file in sorted(target_dir.glob("*.sql")):
        statements = split_sql_statements(sql_file.read_text(encoding="utf-8"))
        if not statements:
            logger.info("Fichier SQL vide ignoré : %s", sql_file.name)
            continue
        logger.info("Application de %s (%d instructions)", sql_file.name, len(statements))
        for stmt in statements:
            warehouse.execute(stmt)

    db_warehouse = get_warehouse()
    inserted = seed_ref_ville(db_warehouse)
    logger.info("urbanhub.ref_ville initialisée avec %d villes", inserted)

    try:
        storage = get_storage(use_local=False)
        storage.ensure_bucket()
        logger.info("Bucket bronze prêt : %s", storage.bucket)
    except Exception as exc:
        logger.warning("Initialisation du bucket bronze ignorée : %s", exc)


if __name__ == "__main__":
    init_warehouse()
