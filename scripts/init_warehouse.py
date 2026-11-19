"""Applique les fichiers sql/*.sql dans l'ordre (si l'initialisation automatique Docker n'est pas utilisée).

Responsable : commun.
"""

from __future__ import annotations

from pathlib import Path

from common.log import setup_logging
from common.warehouse import Warehouse

log = setup_logging(__name__)
SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


def split_statements(sql: str) -> list[str]:
    """Découpe un fichier SQL en instructions (séparateur ';')."""
    return [s.strip() for s in sql.split(";") if s.strip()]


def main() -> None:
    wh = Warehouse()
    for sql_file in sorted(SQL_DIR.glob("*.sql")):
        log.info("Application de %s", sql_file.name)
        for statement in split_statements(sql_file.read_text(encoding="utf-8")):
            wh.command(statement)
    log.info("Initialisation du warehouse terminée.")


if __name__ == "__main__":
    main()
