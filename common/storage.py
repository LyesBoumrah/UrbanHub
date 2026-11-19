"""Client MinIO (data lake, couche bronze) : put_json, put_csv, exists, list.

Responsable : commun.
"""

from __future__ import annotations

import io
import json
from typing import Iterable

import pandas as pd
from minio import Minio

from .config import settings
from .log import setup_logging

log = setup_logging(__name__)


class Storage:
    """Encapsule les écritures/lectures MinIO du data lake."""

    def __init__(
        self,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        bucket: str | None = None,
        secure: bool = False,
    ) -> None:
        self.bucket = bucket or settings.minio_bucket
        self.client = Minio(
            endpoint or settings.minio_endpoint,
            access_key=access_key or settings.minio_root_user,
            secret_key=secret_key or settings.minio_root_password,
            secure=secure,
        )
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    # ------------------------------------------------------------------
    # Écriture
    # ------------------------------------------------------------------
    def put_bytes(self, key: str, data: bytes, content_type: str) -> None:
        self.client.put_object(
            self.bucket,
            key,
            io.BytesIO(data),
            length=len(data),
            content_type=content_type,
        )

    def put_json(self, key: str, payload: dict | list) -> None:
        data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.put_bytes(key, data, "application/json")

    def put_csv(self, key: str, df: pd.DataFrame) -> None:
        self.put_bytes(key, df.to_csv(index=False).encode("utf-8"), "text/csv")

    # ------------------------------------------------------------------
    # Lecture / existence
    # ------------------------------------------------------------------
    def exists(self, key: str) -> bool:
        try:
            self.client.stat_object(self.bucket, key)
            return True
        except Exception:
            return False

    def read_bytes(self, key: str) -> bytes:
        resp = self.client.get_object(self.bucket, key)
        try:
            return resp.read()
        finally:
            resp.close()
            resp.release_conn()

    def read_csv(self, key: str) -> pd.DataFrame:
        return pd.read_csv(io.BytesIO(self.read_bytes(key)))

    def list(self, prefix: str = "") -> Iterable[str]:
        for obj in self.client.list_objects(self.bucket, prefix=prefix, recursive=True):
            yield obj.object_name
