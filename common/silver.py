"""Lecture de la couche silver (Parquet dans Silo) pour les analyses, les notebooks et le dashboard.

    read_silver(storage, "silver/meteo/")                 → toutes les partitions year=…
    read_silver(storage, "silver/velos/ville=lyon/")      → un sous-ensemble
    read_table(storage, "silver/pollution/stations.parquet")

Responsable : commun.
"""
from __future__ import annotations

import io

import pandas as pd


def read_table(storage, key: str, columns=None) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(storage.get_bytes(key)), columns=columns)


def read_silver(storage, prefix: str, columns=None, filename: str | None = None) -> pd.DataFrame:
    """Concatène toutes les partitions Parquet sous `prefix` (fichiers contenant `/year=`)."""
    keys = [k for k in storage.list(prefix) if k.endswith(".parquet") and "/year=" in k
            and (filename is None or k.endswith(filename))]
    if not keys:
        raise FileNotFoundError(f"aucune partition Parquet sous {prefix}")
    return pd.concat([read_table(storage, k, columns) for k in sorted(keys)], ignore_index=True)
