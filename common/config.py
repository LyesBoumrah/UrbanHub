"""Configuration centrale : dataclass Settings chargée une seule fois depuis .env.

Responsable : commun (ne modifier que par une PR dédiée).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
VILLES_FILE = ROOT / "config" / "villes.json"


@dataclass(frozen=True)
class Settings:
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
