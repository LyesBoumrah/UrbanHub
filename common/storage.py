"""Client MinIO multi-buckets Medallion (`bronze`, `silver`, `gold`) : put_json, put_csv, put_parquet, exists, list.

Chaque couche Medallion correspond à son propre bucket MinIO :
  - `bronze` : données brutes issues des APIs (`bronze/<source>/...` -> `s3://bronze/<source>/...`)
  - `silver` : fichiers Parquet nettoyés (`silver/<source>/...` -> `s3://silver/<source>/...`)
  - `gold`   : fichiers JSON métier (`gold/<domaine>/...` -> `s3://gold/<domaine>/...`)

Inclut un backend fichier local (LocalBronzeStorage) respectant la même interface
pour les tests ou l'exécution hors Docker, ainsi que l'alias `Storage` pour P1.

Responsable : commun.
"""

from __future__ import annotations

import io
import json
import threading
from pathlib import Path
from typing import Any

from common.config import Settings, get_settings
from common.log import get_logger

logger = get_logger(__name__)

MEDALLION_LAYERS: tuple[str, ...] = ("bronze", "silver", "gold")


def split_layer_key(key: str) -> tuple[str | None, str]:
    """Sépare une clé logique `<couche>/<objet>` en `(couche, objet)` si `<couche>` ∈ {bronze, silver, gold}."""
    clean = key.lstrip("/")
    for layer in MEDALLION_LAYERS:
        if clean == layer:
            return layer, ""
        if clean.startswith(f"{layer}/"):
            return layer, clean[len(layer) + 1:]
    return None, clean


class LocalBronzeStorage:
    """Stockage objet sur disque local (data/bronze, data/silver, data/gold) avec la même API que MinIOStorage."""

    def __init__(self, base_dir: Path | None = None) -> None:
        self.base_dir = Path(base_dir or get_settings().local_data_dir)
        self._lock = threading.RLock()
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _resolve(self, key: str) -> Path:
        clean_key = key.lstrip("/")
        return self.base_dir / clean_key

    def _ensure_parent(self, target: Path) -> None:
        with self._lock:
            target.parent.mkdir(parents=True, exist_ok=True)

    def ensure_bucket(self) -> None:
        with self._lock:
            self.base_dir.mkdir(parents=True, exist_ok=True)
            for layer in MEDALLION_LAYERS:
                (self.base_dir / layer).mkdir(parents=True, exist_ok=True)

    def put_json(self, key: str, payload: Any) -> str:
        target = self._resolve(key)
        self._ensure_parent(target)
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        target.write_text(raw, encoding="utf-8")
        return key

    def get_json(self, key: str) -> Any:
        target = self._resolve(key)
        return json.loads(target.read_text(encoding="utf-8"))

    def put_csv(self, key: str, content: Any) -> str:
        text = content.to_csv(index=False) if hasattr(content, "to_csv") else str(content)
        target = self._resolve(key)
        self._ensure_parent(target)
        target.write_text(text, encoding="utf-8")
        return key

    def read_csv(self, key: str) -> Any:
        import pandas as pd

        return pd.read_csv(io.BytesIO(self.get_bytes(key)))

    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        del content_type
        target = self._resolve(key)
        self._ensure_parent(target)
        target.write_bytes(data)
        return key

    def get_bytes(self, key: str) -> bytes:
        return self._resolve(key).read_bytes()

    read_bytes = get_bytes

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
    """Client MinIO/Silo multi-buckets pour le Data Lake (buckets dédiés `bronze`, `silver`, `gold`)."""

    def __init__(
        self,
        settings: Settings | None = None,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        bucket: str | None = None,
        secure: bool | None = None,
        bucket_bronze: str | None = None,
        bucket_silver: str | None = None,
        bucket_gold: str | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self._endpoint = endpoint or self.settings.minio_endpoint
        self._access_key = access_key or self.settings.minio_root_user
        self._secret_key = secret_key or self.settings.minio_root_password
        self._secure = self.settings.minio_secure if secure is None else secure
        self.bucket_bronze = bucket_bronze or self.settings.minio_bucket_bronze
        self.bucket_silver = bucket_silver or self.settings.minio_bucket_silver
        self.bucket_gold = bucket_gold or self.settings.minio_bucket_gold
        self.buckets: dict[str, str] = {
            "bronze": self.bucket_bronze,
            "silver": self.bucket_silver,
            "gold": self.bucket_gold,
        }
        self.bucket = bucket or self.bucket_bronze
        self._client: Any = None
        self._lock = threading.RLock()
        self._ready_buckets: set[str] = set()

    @property
    def client(self) -> Any:
        if self._client is None:
            with self._lock:
                if self._client is None:
                    from minio import Minio

                    self._client = Minio(
                        endpoint=self._endpoint,
                        access_key=self._access_key,
                        secret_key=self._secret_key,
                        secure=self._secure,
                    )
        return self._client

    def _resolve_bucket_and_object(self, key: str) -> tuple[str, str]:
        """Résout une clé logique (`bronze/...`, `silver/...`, `gold/...`) en `(nom_bucket, nom_objet)`."""
        layer, obj_name = split_layer_key(key)
        if layer is not None:
            return self.buckets[layer], obj_name
        return self.bucket_bronze, obj_name

    def ensure_bucket(self, bucket_name: str | None = None) -> None:
        """Crée le bucket demandé, ou les 3 buckets Medallion (`bronze`, `silver`, `gold`) par défaut."""
        targets = (bucket_name,) if bucket_name is not None else tuple(dict.fromkeys(self.buckets.values()))
        if all(b in self._ready_buckets for b in targets):
            return
        client = self.client
        with self._lock:
            for target in targets:
                if target in self._ready_buckets:
                    continue
                if not client.bucket_exists(target):
                    try:
                        client.make_bucket(target)
                        logger.info("Bucket MinIO créé : %s", target)
                    except Exception as exc:  # noqa: BLE001 — création concurrente par un autre worker
                        if "BucketAlready" not in str(exc):
                            raise
                self._ready_buckets.add(target)

    def put_json(self, key: str, payload: Any) -> str:
        bucket_name, obj_name = self._resolve_bucket_and_object(key)
        self.ensure_bucket(bucket_name)
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.client.put_object(
            bucket_name=bucket_name,
            object_name=obj_name,
            data=io.BytesIO(data),
            length=len(data),
            content_type="application/json",
        )
        return key

    def get_json(self, key: str) -> Any:
        bucket_name, obj_name = self._resolve_bucket_and_object(key)
        response = self.client.get_object(bucket_name, obj_name)
        try:
            return json.loads(response.read().decode("utf-8"))
        finally:
            response.close()
            response.release_conn()

    def put_csv(self, key: str, content: Any) -> str:
        bucket_name, obj_name = self._resolve_bucket_and_object(key)
        self.ensure_bucket(bucket_name)
        text = content.to_csv(index=False) if hasattr(content, "to_csv") else str(content)
        data = text.encode("utf-8")
        self.client.put_object(
            bucket_name=bucket_name,
            object_name=obj_name,
            data=io.BytesIO(data),
            length=len(data),
            content_type="text/csv; charset=utf-8",
        )
        return key

    def read_csv(self, key: str) -> Any:
        import pandas as pd

        return pd.read_csv(io.BytesIO(self.get_bytes(key)))

    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        bucket_name, obj_name = self._resolve_bucket_and_object(key)
        self.ensure_bucket(bucket_name)
        self.client.put_object(
            bucket_name=bucket_name,
            object_name=obj_name,
            data=io.BytesIO(data),
            length=len(data),
            content_type=content_type,
        )
        return key

    def get_bytes(self, key: str) -> bytes:
        bucket_name, obj_name = self._resolve_bucket_and_object(key)
        response = self.client.get_object(bucket_name, obj_name)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    read_bytes = get_bytes

    def put_parquet(self, key: str, df: Any) -> str:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        return self.put_bytes(key, buf.getvalue(), content_type="application/octet-stream")

    def get_parquet(self, key: str) -> Any:
        import pandas as pd

        return pd.read_parquet(io.BytesIO(self.get_bytes(key)))

    def exists(self, key: str) -> bool:
        bucket_name, obj_name = self._resolve_bucket_and_object(key)
        try:
            self.client.stat_object(bucket_name, obj_name)
            return True
        except Exception:
            return False

    def list(self, prefix: str = "") -> list[str]:
        clean = prefix.lstrip("/")
        if not clean:
            self.ensure_bucket()
            results: list[str] = []
            for layer in MEDALLION_LAYERS:
                bucket_name = self.buckets[layer]
                for obj in self.client.list_objects(bucket_name, prefix="", recursive=True):
                    results.append(f"{layer}/{obj.object_name}")
            return sorted(results)

        layer, obj_prefix = split_layer_key(clean)
        if layer is not None:
            bucket_name = self.buckets[layer]
            self.ensure_bucket(bucket_name)
            objects = self.client.list_objects(bucket_name, prefix=obj_prefix, recursive=True)
            return sorted(f"{layer}/{obj.object_name}" for obj in objects)

        bucket_name = self.bucket_bronze
        self.ensure_bucket(bucket_name)
        objects = self.client.list_objects(bucket_name, prefix=obj_prefix, recursive=True)
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
