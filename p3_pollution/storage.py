"""P3 — Écriture dans la couche bronze : disque local ou MinIO, au choix via .env.

  STORAGE=local  → fichiers sous data/<clé>          (tests, développement)
  STORAGE=minio  → objets s3://<MINIO_BUCKET>/<clé>  (machine finale)

Les deux backends exposent la même interface (write_json, exists) et la même arborescence
de clés : passer de l'un à l'autre ne demande aucun changement de code.

À déplacer dans common/storage.py lors de la fusion pour être partagé avec P1 / P2.

Responsable : P3.
"""
import gzip
import io
import json
import logging
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

log = logging.getLogger(__name__)


def encode_json(obj, compress=False):
    """Sérialise en JSON compact UTF-8, éventuellement compressé en gzip."""
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return gzip.compress(raw) if compress else raw


class LocalStorage:
    """Bronze sur disque local (data/...)."""

    def __init__(self, base_dir=ROOT / "data"):
        self.base_dir = Path(base_dir)

    def write_json(self, key, obj, compress=False):
        path = self.base_dir / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(encode_json(obj, compress))
        return str(path)

    def exists(self, key):
        return (self.base_dir / key).exists()


class MinioStorage:
    """Bronze dans un bucket MinIO (S3 compatible)."""

    def __init__(self, endpoint, access_key, secret_key, bucket, secure=False):
        from minio import Minio  # import paresseux : inutile en mode local

        self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        self.bucket = bucket
        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)
            log.info("Bucket MinIO créé : %s", bucket)

    def write_json(self, key, obj, compress=False):
        data = encode_json(obj, compress)
        content_type = "application/gzip" if compress else "application/json"
        self.client.put_object(self.bucket, key, io.BytesIO(data), len(data), content_type=content_type)
        return f"s3://{self.bucket}/{key}"

    def exists(self, key):
        from minio.error import S3Error

        try:
            self.client.stat_object(self.bucket, key)
            return True
        except S3Error as exc:
            if exc.code in ("NoSuchKey", "NoSuchObject"):
                return False
            raise


def storage_from_env():
    """Instancie le backend choisi par la variable STORAGE (défaut : local)."""
    kind = os.environ.get("STORAGE", "local").lower()
    if kind == "local":
        return LocalStorage()
    if kind == "minio":
        return MinioStorage(
            endpoint=os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.environ["MINIO_ROOT_USER"],
            secret_key=os.environ["MINIO_ROOT_PASSWORD"],
            bucket=os.environ.get("MINIO_BUCKET", "urbanhub"),
            secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
        )
    raise ValueError(f"STORAGE inconnu : {kind!r} (attendu : local ou minio)")
