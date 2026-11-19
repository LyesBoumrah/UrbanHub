"""Client MinIO (data lake, couche bronze) : put_json, put_csv, exists, list.

Inclut un backend fichier local (LocalBronzeStorage) respectant la même interface
pour les tests ou l'exécution hors Docker.

Responsable : commun.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

from common.config import Settings, get_settings
from common.log import get_logger

logger = get_logger(__name__)


class LocalBronzeStorage:
    """Stockage bronze sur disque local (data/) avec la même API que MinIOStorage."""

    def __init__(self, base_dir: Path | None = None) -> None:
        self.base_dir = Path(base_dir or get_settings().local_data_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _resolve(self, key: str) -> Path:
        clean_key = key.lstrip("/")
        return self.base_dir / clean_key

    def ensure_bucket(self) -> None:
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def put_json(self, key: str, payload: Any) -> str:
        target = self._resolve(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        target.write_text(raw, encoding="utf-8")
        return key

    def get_json(self, key: str) -> Any:
        target = self._resolve(key)
        return json.loads(target.read_text(encoding="utf-8"))

    def put_csv(self, key: str, content: str) -> str:
        target = self._resolve(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return key

    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        del content_type
        target = self._resolve(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        return key

    def get_bytes(self, key: str) -> bytes:
        return self._resolve(key).read_bytes()

    def put_parquet(self, key: str, df: Any) -> str:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        return self.put_bytes(key, buf.getvalue(), content_type="application/octet-stream")

    def get_parquet(self, key: str) -> Any:
        import pandas as pd

        return pd.read_parquet(io.BytesIO(self.get_bytes(key)))

    def exists(self, key: str) -> bool:
        return self._resolve(key).is_file()

    def list(self, prefix: str = "") -> list[str]:
        clean_prefix = prefix.lstrip("/")
        root = self.base_dir / clean_prefix
        if root.is_file():
            return [clean_prefix]
        search_root = root if root.is_dir() else self.base_dir
        results: list[str] = []
        for path in sorted(search_root.rglob("*")):
            if path.is_file():
                rel = path.relative_to(self.base_dir).as_posix()
                if rel.startswith(clean_prefix):
                    results.append(rel)
        return results


class MinIOStorage:
    """Client MinIO pour la couche bronze immuable du data lake."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.bucket = self.settings.minio_bucket
        self._client: Any = None

    @property
    def client(self) -> Any:
        if self._client is None:
            from minio import Minio

            self._client = Minio(
                endpoint=self.settings.minio_endpoint,
                access_key=self.settings.minio_root_user,
                secret_key=self.settings.minio_root_password,
                secure=self.settings.minio_secure,
            )
        return self._client

    def ensure_bucket(self) -> None:
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)
            logger.info("Bucket MinIO créé : %s", self.bucket)

    def put_json(self, key: str, payload: Any) -> str:
        self.ensure_bucket()
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.client.put_object(
            bucket_name=self.bucket,
            object_name=key.lstrip("/"),
            data=io.BytesIO(data),
            length=len(data),
            content_type="application/json",
        )
        return key

    def get_json(self, key: str) -> Any:
        response = self.client.get_object(self.bucket, key.lstrip("/"))
        try:
            return json.loads(response.read().decode("utf-8"))
        finally:
            response.close()
            response.release_conn()

    def put_csv(self, key: str, content: str) -> str:
        self.ensure_bucket()
        data = content.encode("utf-8")
        self.client.put_object(
            bucket_name=self.bucket,
            object_name=key.lstrip("/"),
            data=io.BytesIO(data),
            length=len(data),
            content_type="text/csv; charset=utf-8",
        )
        return key

    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        self.ensure_bucket()
        self.client.put_object(
            bucket_name=self.bucket,
            object_name=key.lstrip("/"),
            data=io.BytesIO(data),
            length=len(data),
            content_type=content_type,
        )
        return key

    def get_bytes(self, key: str) -> bytes:
        response = self.client.get_object(self.bucket, key.lstrip("/"))
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def put_parquet(self, key: str, df: Any) -> str:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        return self.put_bytes(key, buf.getvalue(), content_type="application/octet-stream")

    def get_parquet(self, key: str) -> Any:
        import pandas as pd

        return pd.read_parquet(io.BytesIO(self.get_bytes(key)))

    def exists(self, key: str) -> bool:
        try:
            self.client.stat_object(self.bucket, key.lstrip("/"))
            return True
        except Exception:
            return False

    def list(self, prefix: str = "") -> list[str]:
        self.ensure_bucket()
        objects = self.client.list_objects(self.bucket, prefix=prefix.lstrip("/"), recursive=True)
        return sorted(obj.object_name for obj in objects)


def get_storage(use_local: bool = False, settings: Settings | None = None) -> MinIOStorage | LocalBronzeStorage:
    """Retourne un client MinIO (ou un stockage local si use_local=True)."""
    if use_local:
        cfg = settings or get_settings()
        return LocalBronzeStorage(base_dir=cfg.local_data_dir)
    return MinIOStorage(settings=settings)


class Storage(MinIOStorage):
    """Compatibilité P1 : client MinIO configurable par arguments, avec read_bytes / read_csv."""

    def __init__(
        self,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        bucket: str | None = None,
        secure: bool | None = None,
    ) -> None:
        from dataclasses import replace

        base = get_settings()
        overrides = {
            "minio_endpoint": endpoint,
            "minio_root_user": access_key,
            "minio_root_password": secret_key,
            "minio_bucket": bucket,
            "minio_secure": secure,
        }
        super().__init__(settings=replace(base, **{k: v for k, v in overrides.items() if v is not None}))

    def read_bytes(self, key: str) -> bytes:
        return self.get_bytes(key)

    def read_csv(self, key: str) -> Any:
        import pandas as pd

        return pd.read_csv(io.BytesIO(self.get_bytes(key)))
