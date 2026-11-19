"""Configuration du logging structuré partagée par tous les pipelines.

Responsable : commun.
"""

from __future__ import annotations

import logging
import sys

from common.config import get_settings


def get_logger(name: str | None = None, level: str | None = None) -> logging.Logger:
    """Retourne un logger configuré avec un format uniforme UTC."""
    logger = logging.getLogger(name or "urbanhub")
    resolved_level = (level or get_settings().log_level).upper()
    logger.setLevel(getattr(logging, resolved_level, logging.INFO))

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False

    return logger


# Alias rétrocompatible utilisé par P1
setup_logging = get_logger
