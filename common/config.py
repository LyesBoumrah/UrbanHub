"""Configuration centrale : dataclass Settings chargée une seule fois depuis .env.

Responsable : commun (ne modifier que par une PR dédiée).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"


def _load_dotenv_if_available() -> None:
    """Charge le fichier .env à la racine du projet si python-dotenv est installé."""
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(dotenv_path=env_path, override=False)
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
VILLES_FILE = ROOT / "config" / "villes.json"


@dataclass(frozen=True)
class Settings:
    """Paramètres globaux de la plateforme UrbanHub."""

    # OpenAQ (P3)
    openaq_api_key: str = ""

    # MinIO (data lake bronze)
    minio_endpoint: str = "localhost:9000"
    minio_root_user: str = "urbanhub"
    minio_root_password: str = "change-me"
    minio_bucket: str = "urbanhub"
    minio_secure: bool = False

    # ClickHouse (entrepôt silver / gold)
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "urbanhub"
    clickhouse_password: str = "change-me"
    clickhouse_db: str = "urbanhub"

    # Divers
    http_user_agent: str = "UrbanHub-school-project/0.1"
    log_level: str = "INFO"
    local_data_dir: Path = PROJECT_ROOT / "data"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Charge et met en cache la configuration depuis l'environnement / .env."""
    _load_dotenv_if_available()
    return Settings(
        openaq_api_key=os.getenv("OPENAQ_API_KEY", ""),
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        minio_root_user=os.getenv("MINIO_ROOT_USER", "urbanhub"),
        minio_root_password=os.getenv("MINIO_ROOT_PASSWORD", "change-me"),
        minio_bucket=os.getenv("MINIO_BUCKET", "urbanhub"),
        minio_secure=os.getenv("MINIO_SECURE", "false").lower() in {"1", "true", "yes"},
        clickhouse_host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        clickhouse_port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        clickhouse_user=os.getenv("CLICKHOUSE_USER", "urbanhub"),
        clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", "change-me"),
        clickhouse_db=os.getenv("CLICKHOUSE_DB", "urbanhub"),
        http_user_agent=os.getenv("HTTP_USER_AGENT", "UrbanHub-school-project/0.1"),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        local_data_dir=Path(os.getenv("LOCAL_DATA_DIR", str(PROJECT_ROOT / "data"))),
    )


def load_villes(path: Path | None = None) -> list[dict[str, Any]]:
    """Charge config/villes.json."""
    target = path or (CONFIG_DIR / "villes.json")
    with target.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_ref_ville(path: Path | None = None, core_only: bool = False) -> list[dict[str, Any]]:
    """Charge config/ref_ville.json (avec filtre optionnel sur les 5 villes cœur UrbanHub)."""
    target = path or (CONFIG_DIR / "ref_ville.json")
    with target.open("r", encoding="utf-8") as f:
        entries: list[dict[str, Any]] = json.load(f)
    if core_only:
        return [e for e in entries if e.get("core_urbanhub", False)]
    return entries
    """Paramètres partagés par tous les pipelines."""

    # MinIO
    minio_endpoint: str
    minio_root_user: str
    minio_root_password: str
    minio_bucket: str

    # ClickHouse
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_db: str

    # APIs
    openaq_api_key: str
    http_user_agent: str

    # Divers
    log_level: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            minio_root_user=os.getenv("MINIO_ROOT_USER", "urbanhub"),
            minio_root_password=os.getenv("MINIO_ROOT_PASSWORD", "change-me"),
            minio_bucket=os.getenv("MINIO_BUCKET", "urbanhub"),
            clickhouse_host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            clickhouse_port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            clickhouse_user=os.getenv("CLICKHOUSE_USER", "urbanhub"),
            clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", "change-me"),
            clickhouse_db=os.getenv("CLICKHOUSE_DB", "urbanhub"),
            openaq_api_key=os.getenv("OPENAQ_API_KEY", ""),
            http_user_agent=os.getenv("HTTP_USER_AGENT", "UrbanHub-school-project/0.1"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )


settings = Settings.from_env()


def load_villes() -> list[dict]:
    """Charge la liste des villes depuis config/villes.json."""
    return json.loads(VILLES_FILE.read_text(encoding="utf-8"))
