"""P3 — Écriture et lecture dans les couches bronze, silver et gold : disque local ou MinIO, au choix via .env.

  STORAGE=local  → fichiers sous data/<couche>/<clé>           (tests, développement)
  STORAGE=minio  → 3 buckets MinIO dédiés : s3://bronze/<clé>, s3://silver/<clé>, s3://gold/<clé>

Les deux backends exposent la même interface (write_json, read_json, exists, list_keys,
write_parquet, read_parquet) et la même arborescence logique de clés : passer de l'un à l'autre ne
demande aucun changement de code.

Responsable : P3.
"""
import gzip
import io
import json
import logging
import os
import threading
from pathlib import Path

from common.storage import MEDALLION_LAYERS, split_layer_key

ROOT = Path(__file__).resolve().parents[1]

log = logging.getLogger(__name__)


def encode_json(obj, compress=False):
    """Sérialise en JSON compact UTF-8, éventuellement compressé en gzip."""
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return gzip.compress(raw) if compress else raw


def decode_json(raw_bytes):
    """Désérialise des octets JSON (décompresse automatiquement si gzip)."""
    if raw_bytes[:2] == b"\x1f\x8b":
        raw_bytes = gzip.decompress(raw_bytes)
    return json.loads(raw_bytes.decode("utf-8"))


class LocalStorage:
    """Bronze & Silver sur disque local (data/...)."""

    def __init__(self, base_dir=ROOT / "data"):
        self.base_dir = Path(base_dir)

    def write_json(self, key, obj, compress=False):
        path = self.base_dir / key.lstrip("/")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(encode_json(obj, compress))
        return str(path)

    def read_json(self, key):
        path = self.base_dir / key.lstrip("/")
        return decode_json(path.read_bytes())

    def exists(self, key):
        return (self.base_dir / key.lstrip("/")).exists()

    def list_keys(self, prefix=""):
        clean_prefix = prefix.lstrip("/")
        target = self.base_dir / clean_prefix
        if target.is_file():
            return [clean_prefix]
        search_root = target if target.is_dir() else self.base_dir
        if not search_root.exists():
            return []
        keys = []
        for p in sorted(search_root.rglob("*")):
            if p.is_file():
                rel = p.relative_to(self.base_dir).as_posix()
                if rel.startswith(clean_prefix):
                    keys.append(rel)
        return keys

    list = list_keys

    def write_parquet(self, key, df):
        path = self.base_dir / key.lstrip("/")
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
        return str(path)

    put_parquet = write_parquet

    def read_parquet(self, key):
        import pandas as pd

        path = self.base_dir / key.lstrip("/")
        return pd.read_parquet(path)

    get_parquet = read_parquet


class MinioStorage:
    """Client MinIO multi-buckets Medallion (`bronze`, `silver`, `gold`) pour P3."""

    def __init__(
        self,
        endpoint,
        access_key,
        secret_key,
        bucket=None,
        secure=False,
        bucket_bronze="bronze",
        bucket_silver="silver",
        bucket_gold="gold",
    ):
        from minio import Minio  # import paresseux : inutile en mode local

        self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        self.bucket_bronze = bucket_bronze
        self.bucket_silver = bucket_silver
        self.bucket_gold = bucket_gold
        self.buckets = {
            "bronze": self.bucket_bronze,
            "silver": self.bucket_silver,
            "gold": self.bucket_gold,
        }
        self.bucket = bucket or self.bucket_bronze
        self._lock = threading.RLock()
        self._ready_buckets: set[str] = set()
        self.ensure_buckets()

    def _resolve_bucket_and_object(self, key):
        layer, obj_name = split_layer_key(key)
        if layer is not None:
            return self.buckets[layer], obj_name
        return self.bucket_bronze, obj_name

    def ensure_buckets(self, bucket_name=None):
        targets = (bucket_name,) if bucket_name is not None else tuple(dict.fromkeys(self.buckets.values()))
        with self._lock:
            for target in targets:
                if target in self._ready_buckets:
                    continue
                if not self.client.bucket_exists(target):
                    try:
                        self.client.make_bucket(target)
                        log.info("Bucket MinIO créé : %s", target)
                    except Exception as exc:  # noqa: BLE001 — création concurrente par un autre worker
                        if "BucketAlready" not in str(exc):
                            raise
                self._ready_buckets.add(target)

    def write_json(self, key, obj, compress=False):
        bucket_name, obj_name = self._resolve_bucket_and_object(key)
        self.ensure_buckets(bucket_name)
        data = encode_json(obj, compress)
        content_type = "application/gzip" if compress else "application/json"
        self.client.put_object(bucket_name, obj_name, io.BytesIO(data), len(data), content_type=content_type)
        return f"s3://{bucket_name}/{obj_name}"

    def read_json(self, key):
        bucket_name, obj_name = self._resolve_bucket_and_object(key)
        resp = self.client.get_object(bucket_name, obj_name)
        try:
            return decode_json(resp.read())
        finally:
            resp.close()
            resp.release_conn()

    def exists(self, key):
        from minio.error import S3Error

        bucket_name, obj_name = self._resolve_bucket_and_object(key)
        try:
            self.client.stat_object(bucket_name, obj_name)
            return True
        except S3Error as exc:
            if exc.code in ("NoSuchKey", "NoSuchObject", "NoSuchBucket"):
                return False
            raise

    def list_keys(self, prefix=""):
        clean = prefix.lstrip("/")
        if not clean:
            self.ensure_buckets()
            keys = []
            for layer in MEDALLION_LAYERS:
                bucket_name = self.buckets[layer]
                for obj in self.client.list_objects(bucket_name, prefix="", recursive=True):
                    keys.append(f"{layer}/{obj.object_name}")
            return sorted(keys)

        layer, obj_prefix = split_layer_key(clean)
        if layer is not None:
            bucket_name = self.buckets[layer]
            self.ensure_buckets(bucket_name)
            objs = self.client.list_objects(bucket_name, prefix=obj_prefix, recursive=True)
            return sorted(f"{layer}/{o.object_name}" for o in objs)

        bucket_name = self.bucket_bronze
        self.ensure_buckets(bucket_name)
        objs = self.client.list_objects(bucket_name, prefix=obj_prefix, recursive=True)
        return sorted(o.object_name for o in objs)

    list = list_keys

    def write_parquet(self, key, df):
        bucket_name, obj_name = self._resolve_bucket_and_object(key)
        self.ensure_buckets(bucket_name)
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        raw = buf.getvalue()
        self.client.put_object(
            bucket_name,
            obj_name,
            io.BytesIO(raw),
            len(raw),
            content_type="application/octet-stream",
        )
        return f"s3://{bucket_name}/{obj_name}"

    put_parquet = write_parquet

    def read_parquet(self, key):
        import pandas as pd

        bucket_name, obj_name = self._resolve_bucket_and_object(key)
        resp = self.client.get_object(bucket_name, obj_name)
        try:
            return pd.read_parquet(io.BytesIO(resp.read()))
        finally:
            resp.close()
            resp.release_conn()

    get_parquet = read_parquet


def storage_from_env(use_local=None):
    """Instancie le backend choisi par `use_local` ou la variable STORAGE (défaut : local)."""
    if use_local is True:
        return LocalStorage()
    kind = "minio" if use_local is False else os.environ.get("STORAGE", "local").lower()
    if kind == "local":
        return LocalStorage()
    if kind == "minio":
        return MinioStorage(
            endpoint=os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.environ.get("MINIO_ROOT_USER", "urbanhub"),
            secret_key=os.environ.get("MINIO_ROOT_PASSWORD", "change-me"),
            bucket_bronze=os.environ.get("MINIO_BUCKET_BRONZE", "bronze"),
            bucket_silver=os.environ.get("MINIO_BUCKET_SILVER", "silver"),
            bucket_gold=os.environ.get("MINIO_BUCKET_GOLD", "gold"),
            secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
        )
    raise ValueError(f"STORAGE inconnu : {kind!r} (attendu : local ou minio)")
