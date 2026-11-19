"""Configuration du logging structuré partagée par tous les pipelines.

Responsable : commun.
"""

from __future__ import annotations

import logging
import sys

from .config import settings


def setup_logging(name: str | None = None) -> logging.Logger:
    """Configure et retourne un logger nommé."""
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger(name or "urbanhub")
