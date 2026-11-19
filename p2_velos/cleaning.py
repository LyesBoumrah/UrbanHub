"""P2 — Normalisation et nettoyage unifiés (streaming JSON + historique 5 ans Parquet) -> silver.

Extrait les 7 variables demandées (station_id, station_name, latitude, longitude,
bikes_available, free_slots, timestamp), applique les règles qualité P2 (incluant
cohérence capacité / declared_slots, compteurs >= 0 et bbox de la ville), dédoublonne
et charge les tables silver (ClickHouse ou Parquet/JSONL local).

Utilisation CLI (rejeu complet de la couche bronze) :
- Tout le bronze (streaming + historique 5 ans) : python -m p2_velos.cleaning --all [--local]
- Uniquement le streaming JSON :                  python -m p2_velos.cleaning --streaming [--local]
- Uniquement l'historique Parquet :               python -m p2_velos.cleaning --historique [--local]

Responsable : P2.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from common.config import get_settings, load_ref_ville
from common.log import get_logger
from common.storage import LocalBronzeStorage, MinIOStorage, get_storage
from common.warehouse import ClickHouseWarehouse, get_warehouse
from p2_velos.quality import (
    DEFAULT_MAX_AGE_MINUTES,
    QualityReport,
    classify_station_status,
    filter_and_audit_records,
    parse_citybikes_timestamp,
)

logger = get_logger(__name__)

DEFAULT_CLICKHOUSE_BATCH_SIZE = 50_000

_HIST_BRONZE_KEY_RE = re.compile(
    r"^bronze/velos_historique/(?P<ville_id>[^/]+)/(?P<year>\d{4})/(?P<ym>\d{6})-(?P<network>.+)-stats\.parquet$"
)
_SLOTS_JSON_RE = r'"slots"\s*:\s*(\d+)'
_VIRTUAL_JSON_RE = r'"virtual"\s*:\s*true'

STATION_COLUMNS = [
    "ville_id",
    "network_id",
    "station_id",
    "station_name",
    "latitude",
    "longitude",
    "capacity",
    "is_virtual",
    "updated_at_utc",
]

RELEVE_COLUMNS = [
    "ville_id",
    "network_id",
    "station_id",
    "station_name",
    "timestamp",
    "heure_utc",
    "latitude",
    "longitude",
    "bikes_available",
    "free_slots",
    "capacity",
    "taux_occupation",
    "taux_utilisation",
    "est_vide",
    "est_pleine",
    "est_critique",
    "ingested_at_utc",
]


def _safe_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


def _safe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_historical_bronze_key(key: str) -> dict[str, str] | None:
    """Extrait (ville_id, year, ym, network_id) depuis une clé bronze historique Parquet."""
    match = _HIST_BRONZE_KEY_RE.match(key.lstrip("/"))
    if not match:
        return None
    return {
        "ville_id": match.group("ville_id"),
        "year": match.group("year"),
        "ym": match.group("ym"),
        "network_id": match.group("network"),
    }


def extract_raw_records(
    bronze_envelope: Mapping[str, Any],
    ville_id: str | None = None,
    network_id: str | None = None,
) -> list[dict[str, Any]]:
    """Extrait les enregistrements bruts normalisés depuis un payload CityBikes v2 (enveloppé ou brut)."""
    if "payload" in bronze_envelope and isinstance(bronze_envelope["payload"], Mapping):
        meta = bronze_envelope.get("meta") or {}
        network = bronze_envelope["payload"].get("network") or {}
        resolved_ville = ville_id or meta.get("ville_id") or ""
        resolved_network = network_id or meta.get("network_id") or network.get("id") or ""
    else:
        network = bronze_envelope.get("network") or {}
        resolved_ville = ville_id or ""
        resolved_network = network_id or network.get("id") or ""

    stations = network.get("stations") or []
    records: list[dict[str, Any]] = []

    for station in stations:
        extra = station.get("extra") if isinstance(station.get("extra"), Mapping) else {}
        station_name = str(station.get("name") or "").strip()
        station_id = str(station.get("id") or "").strip()

        bikes = _safe_int(station.get("free_bikes"))
        slots = _safe_int(station.get("empty_slots"))
        declared_slots = _safe_int(extra.get("slots"))

        # Si empty_slots est absent sur un réseau, on le déduit de extra.slots si disponible
        if slots is None and declared_slots is not None and bikes is not None and declared_slots >= bikes:
            slots = declared_slots - bikes

        records.append(
            {
                "ville_id": resolved_ville,
                "network_id": resolved_network,
                "station_id": station_id,
                "station_name": station_name,
                "latitude": _safe_float(station.get("latitude")),
                "longitude": _safe_float(station.get("longitude")),
                "bikes_available": bikes,
                "free_slots": slots,
                "declared_slots": declared_slots,
                "is_virtual": 1 if extra.get("virtual") else 0,
                "timestamp": station.get("timestamp"),
            }
        )

    return records


def deduplicate_records(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Dédoublonne les relevés sur (ville_id, station_id, timestamp) en conservant le dernier vu."""
    seen: dict[tuple[str, str, str], dict[str, Any]] = {}
    for rec in records:
        ts_val = rec.get("timestamp")
        ts_key = ts_val.isoformat() if isinstance(ts_val, datetime) else str(ts_val)
        key = (str(rec.get("ville_id", "")), str(rec.get("station_id", "")), ts_key)
        seen[key] = dict(rec)
    return list(seen.values())


def enrich_valid_records(
    valid_records: Sequence[Mapping[str, Any]],
    ingested_at_utc: datetime | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Enrichit les relevés valides et sépare dimension (velo_station) et faits (velo_releve)."""
    now_utc = ingested_at_utc or datetime.now(timezone.utc)
    stations_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    releves: list[dict[str, Any]] = []

    for rec in valid_records:
        ts_utc = parse_citybikes_timestamp(rec["timestamp"])
        heure_utc = ts_utc.replace(minute=0, second=0, microsecond=0)
        bikes = int(rec["bikes_available"])
        slots = int(rec["free_slots"])
        metrics = classify_station_status(bikes, slots)

        releve = {
            "ville_id": rec["ville_id"],
            "network_id": rec["network_id"],
            "station_id": rec["station_id"],
            "station_name": rec["station_name"],
            "timestamp": ts_utc,
            "heure_utc": heure_utc,
            "latitude": float(rec["latitude"]),
            "longitude": float(rec["longitude"]),
            "bikes_available": bikes,
            "free_slots": slots,
            "capacity": int(metrics["capacity"]),
            "taux_occupation": float(metrics["taux_occupation"]),
            "taux_utilisation": float(metrics["taux_utilisation"]),
            "est_vide": int(metrics["est_vide"]),
            "est_pleine": int(metrics["est_pleine"]),
            "est_critique": int(metrics["est_critique"]),
            "ingested_at_utc": now_utc,
        }
        releves.append(releve)

        station_key = (rec["ville_id"], rec["station_id"])
        stations_by_key[station_key] = {
            "ville_id": rec["ville_id"],
            "network_id": rec["network_id"],
            "station_id": rec["station_id"],
            "station_name": rec["station_name"],
            "latitude": float(rec["latitude"]),
            "longitude": float(rec["longitude"]),
            "capacity": int(metrics["capacity"]),
            "is_virtual": int(rec.get("is_virtual", 0)),
            "updated_at_utc": ts_utc,
        }

    return list(stations_by_key.values()), releves


def clean_bronze_payload(
    bronze_envelope: Mapping[str, Any],
    bbox: Mapping[str, float] | None = None,
    now_utc: datetime | None = None,
    max_age_minutes: int | None = DEFAULT_MAX_AGE_MINUTES,
    ville_id: str | None = None,
    network_id: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], QualityReport]:
    """Pipeline complet bronze JSON streaming -> silver (extraction, qualité, dédoublonnage)."""
    raw_records = extract_raw_records(bronze_envelope, ville_id=ville_id, network_id=network_id)
    report = filter_and_audit_records(
        raw_records,
        bbox=bbox,
        now_utc=now_utc,
        max_age_minutes=max_age_minutes,
    )
    deduped = deduplicate_records(report.valid_records)
    stations, releves = enrich_valid_records(deduped, ingested_at_utc=now_utc)
    return stations, releves, report


def clean_historical_parquet(
    parquet_bytes: bytes,
    ville_id: str,
    network_id: str,
    bbox: Mapping[str, float] | None = None,
    freq: str | None = "1h",
    ingested_at_utc: datetime | None = None,
) -> tuple[Any, Any, dict[str, Any]]:
    """Nettoie, valide et rééchantillonne un dump Parquet historique data.citybik.es -> silver.

    Applique de manière vectorisée toutes les règles qualité P2 (7 variables requises,
    compteurs >= 0, capacité > 0, cohérence extra.slots, coordonnées + bbox de la ville,
    absence de date future), sans limite de fraîcheur 30 min (archive historique).
    """
    import pandas as pd
    import pyarrow.parquet as pq

    now_utc = ingested_at_utc or datetime.now(timezone.utc)
    cols_wanted = ["id", "name", "latitude", "longitude", "bikes", "free", "timestamp", "extra"]
    pf = pq.ParquetFile(io.BytesIO(parquet_bytes))
    available_cols = set(pf.schema_arrow.names)
    read_cols = [c for c in cols_wanted if c in available_cols]
    df = pq.read_table(io.BytesIO(parquet_bytes), columns=read_cols).to_pandas()

    raw_count = len(df)
    if df.empty:
        return (
            pd.DataFrame(columns=STATION_COLUMNS),
            pd.DataFrame(columns=RELEVE_COLUMNS),
            {"raw_count": 0, "valid_count": 0, "rejected_count": 0, "rejection_rate": 0.0, "resampled_count": 0},
        )

    df = df.rename(
        columns={
            "id": "station_id",
            "name": "station_name",
            "bikes": "bikes_available",
            "free": "free_slots",
        }
    )

    df["station_id"] = df["station_id"].astype(str).str.strip()
    df["station_name"] = df["station_name"].astype(str).str.strip()
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["bikes_available"] = pd.to_numeric(df["bikes_available"], errors="coerce")
    df["free_slots"] = pd.to_numeric(df["free_slots"], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    observed_capacity = df["bikes_available"] + df["free_slots"]
    valid_mask = (
        (df["station_id"] != "")
        & (df["station_name"] != "")
        & df["latitude"].notna()
        & df["longitude"].notna()
        & df["bikes_available"].notna()
        & df["free_slots"].notna()
        & df["timestamp"].notna()
        & (df["bikes_available"] >= 0)
        & (df["free_slots"] >= 0)
        & (observed_capacity > 0)
        & (df["latitude"].between(-90.0, 90.0))
        & (df["longitude"].between(-180.0, 180.0))
        & ~((df["latitude"] == 0.0) & (df["longitude"] == 0.0))
        & (df["timestamp"] <= pd.Timestamp(now_utc) + pd.Timedelta(minutes=5))
    )

    # Cohérence capacité observée vs extra.slots (même règle que quality.validate_station_record)
    if "extra" in df.columns:
        extra_str = df["extra"].astype(str)
        declared_slots = pd.to_numeric(extra_str.str.extract(_SLOTS_JSON_RE, expand=False), errors="coerce")
        has_declared = declared_slots.notna() & (declared_slots > 0)
        tolerance = (declared_slots * 0.25).fillna(0).astype(int).clip(lower=2)
        valid_mask &= (~has_declared) | (observed_capacity <= declared_slots + tolerance)

    if bbox is not None:
        valid_mask &= df["latitude"].between(bbox["lat_min"], bbox["lat_max"]) & df["longitude"].between(
            bbox["lon_min"], bbox["lon_max"]
        )

    df = df.loc[valid_mask].copy()
    valid_count = len(df)
    rejected_count = raw_count - valid_count
    rejection_rate = round(rejected_count / raw_count, 4) if raw_count else 0.0

    if df.empty:
        return (
            pd.DataFrame(columns=STATION_COLUMNS),
            pd.DataFrame(columns=RELEVE_COLUMNS),
            {
                "raw_count": raw_count,
                "valid_count": 0,
                "rejected_count": rejected_count,
                "rejection_rate": rejection_rate,
                "resampled_count": 0,
                "stations_count": 0,
            },
        )

    df["bikes_available"] = df["bikes_available"].astype("int32")
    df["free_slots"] = df["free_slots"].astype("int32")
    df = df.sort_values(["station_id", "timestamp"])

    if freq:
        df["_bucket"] = df["timestamp"].dt.floor(freq)
        df = df.drop_duplicates(subset=["station_id", "_bucket"], keep="last").drop(columns=["_bucket"])
    else:
        df = df.drop_duplicates(subset=["station_id", "timestamp"], keep="last")

    df["ville_id"] = ville_id
    df["network_id"] = network_id
    df["heure_utc"] = df["timestamp"].dt.floor("h")
    df["capacity"] = (df["bikes_available"] + df["free_slots"]).astype("int32")
    df["taux_occupation"] = (df["bikes_available"] / df["capacity"]).round(4).astype("float32")
    df["taux_utilisation"] = (df["free_slots"] / df["capacity"]).round(4).astype("float32")
    df["est_vide"] = (df["bikes_available"] == 0).astype("uint8")
    df["est_pleine"] = (df["free_slots"] == 0).astype("uint8")
    df["est_critique"] = ((df["taux_occupation"] <= 0.10) | (df["taux_occupation"] >= 0.90)).astype("uint8")
    df["ingested_at_utc"] = pd.Timestamp(now_utc)

    df_releves = df[RELEVE_COLUMNS].copy()

    latest = df.groupby("station_id", as_index=False).last()
    if "extra" in latest.columns:
        latest["is_virtual"] = (
            latest["extra"].astype(str).str.contains(_VIRTUAL_JSON_RE, case=False, regex=True, na=False).astype("uint8")
        )
    else:
        latest["is_virtual"] = 0
    latest["updated_at_utc"] = latest["timestamp"]
    df_stations = latest[STATION_COLUMNS].copy()

    return (
        df_stations,
        df_releves,
        {
            "raw_count": raw_count,
            "valid_count": valid_count,
            "rejected_count": rejected_count,
            "rejection_rate": rejection_rate,
            "resampled_count": len(df_releves),
            "stations_count": len(df_stations),
        },
    )


def get_bbox_map() -> dict[str, dict[str, float]]:
    """Retourne un dictionnaire ville_id -> bbox depuis config/ref_ville.json."""
    return {v["ville_id"]: v["bbox"] for v in load_ref_ville() if "bbox" in v}


def records_to_dataframes(
    stations: Sequence[Mapping[str, Any]],
    releves: Sequence[Mapping[str, Any]],
) -> tuple[Any, Any]:
    """Convertit les listes normalisées en DataFrames pandas typés pour silver."""
    import pandas as pd

    df_stations = pd.DataFrame(stations, columns=STATION_COLUMNS)
    df_releves = pd.DataFrame(releves, columns=RELEVE_COLUMNS)
    return df_stations, df_releves


def build_silver_streaming_key(ts_utc: datetime | None = None, prefix: str = "velo_releve") -> str:
    """Construit la clé Parquet silver partitionnée par date/heure UTC dans le data lake."""
    ref = (ts_utc or datetime.now(timezone.utc)).astimezone(timezone.utc)
    date_part = ref.strftime("%Y-%m-%d")
    hour_part = ref.strftime("%H")
    stamp = ref.strftime("%Y%m%dT%H%M%SZ")
    return f"silver/velos/dt={date_part}/hr={hour_part}/{prefix}_{stamp}.parquet"


def build_silver_historical_key(ville_id: str, year: int | str, ym: str) -> str:
    """Construit la clé Parquet silver historique partitionnée par ville et année."""
    return f"silver/velos_historique/{ville_id}/{year}/velo_releve_{ym}.parquet"


def save_silver_parquet_to_storage(
    storage: MinIOStorage | LocalBronzeStorage,
    stations: Any,
    releves: Any,
    releve_key: str,
    station_key: str | None = "silver/velos/velo_station_latest.parquet",
) -> str | None:
    """Écrit les données nettoyées au format Parquet dans la couche silver (MinIO ou local)."""
    try:
        import pandas as pd
    except ImportError:
        logger.warning("pandas/pyarrow non disponibles : écriture Parquet silver ignorée")
        return None

    df_stations = (
        stations if isinstance(stations, pd.DataFrame) else pd.DataFrame(stations, columns=STATION_COLUMNS)
    )
    df_releves = (
        releves if isinstance(releves, pd.DataFrame) else pd.DataFrame(releves, columns=RELEVE_COLUMNS)
    )

    written_key: str | None = None
    if len(df_releves) > 0:
        storage.put_parquet(releve_key, df_releves)
        written_key = releve_key
        logger.info("Silver Parquet écrit (%d relevés) -> %s", len(df_releves), releve_key)
    if station_key and len(df_stations) > 0:
        storage.put_parquet(station_key, df_stations)
    return written_key


def save_silver_local(
    stations: Sequence[Mapping[str, Any]],
    releves: Sequence[Mapping[str, Any]],
    output_dir: Path | None = None,
    filename_prefix: str = "velo_releve",
) -> Path:
    """Enregistre un batch silver sur disque local (Parquet si pyarrow dispo, sinon JSONL)."""
    base = Path(output_dir or (get_settings().local_data_dir / "silver" / "velos"))
    base.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    try:
        df_stations, df_releves = records_to_dataframes(stations, releves)
        releve_path = base / f"{filename_prefix}_{stamp}.parquet"
        if len(df_releves) > 0:
            df_releves.to_parquet(releve_path, index=False)
        if len(df_stations) > 0:
            station_path = base / "velo_station_latest.parquet"
            df_stations.to_parquet(station_path, index=False)
        logger.info("Silver local écrit (Parquet) : %d relevés -> %s", len(releves), releve_path)
        return releve_path
    except ImportError:
        releve_path = base / f"{filename_prefix}_{stamp}.jsonl"
        with releve_path.open("w", encoding="utf-8") as f:
            for row in releves:
                serializable = {
                    k: (v.isoformat() if isinstance(v, datetime) else v)
                    for k, v in row.items()
                }
                f.write(json.dumps(serializable, ensure_ascii=False) + "\n")
        logger.info("Silver local écrit (JSONL) : %d relevés -> %s", len(releves), releve_path)
        return releve_path


def insert_df_in_chunks(
    warehouse: ClickHouseWarehouse,
    table: str,
    df: Any,
    chunk_size: int = DEFAULT_CLICKHOUSE_BATCH_SIZE,
) -> int:
    """Insère un DataFrame dans ClickHouse par lots pour maîtriser la consommation mémoire."""
    total = 0
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start:start + chunk_size]
        total += warehouse.insert_df(table, chunk)
    return total


def load_warehouse_from_silver_parquet(
    storage: MinIOStorage | LocalBronzeStorage,
    warehouse: ClickHouseWarehouse,
    releve_key: str | None,
    station_key: str | None = None,
) -> tuple[int, int]:
    """Charge ClickHouse strictement à partir des fichiers Parquet de la couche silver."""
    inserted_stations = 0
    inserted_releves = 0

    if station_key and storage.exists(station_key):
        df_stations = storage.get_parquet(station_key)
        if len(df_stations) > 0:
            inserted_stations = insert_df_in_chunks(
                warehouse, "urbanhub.velo_station", df_stations[STATION_COLUMNS]
            )

    if releve_key and storage.exists(releve_key):
        df_releves = storage.get_parquet(releve_key)
        if len(df_releves) > 0:
            inserted_releves = insert_df_in_chunks(
                warehouse, "urbanhub.velo_releve", df_releves[RELEVE_COLUMNS]
            )

    logger.info(
        "ClickHouse chargé depuis Silver Parquet (%s) : %d stations, %d relevés",
        releve_key or station_key,
        inserted_stations,
        inserted_releves,
    )
    return inserted_stations, inserted_releves


def sync_silver_to_warehouse(
    scope: str = "all",
    cities: Sequence[str] | None = None,
    storage: MinIOStorage | LocalBronzeStorage | None = None,
    warehouse: ClickHouseWarehouse | None = None,
    max_workers: int = 5,
) -> dict[str, Any]:
    """Charge dans ClickHouse l'ensemble des fichiers Parquet présents dans la couche silver MinIO."""
    if scope not in {"all", "streaming", "historique"}:
        raise ValueError(f"Scope invalide : {scope!r}")

    target_storage = storage or get_storage(use_local=False)
    target_warehouse = warehouse or get_warehouse()
    wanted_cities = {c.strip().lower() for c in cities} if cities else None

    loaded_files = 0
    total_stations = 0
    total_releves = 0
    errors: list[dict[str, str]] = []

    if scope in {"all", "streaming"}:
        station_key = "silver/velos/velo_station_latest.parquet"
        if target_storage.exists(station_key):
            st_cnt, _ = load_warehouse_from_silver_parquet(target_storage, target_warehouse, None, station_key)
            total_stations += st_cnt

        keys = [
            k
            for k in target_storage.list("silver/velos/")
            if k.endswith(".parquet") and not k.endswith("velo_station_latest.parquet")
        ]
        for key in keys:
            try:
                _, rel_cnt = load_warehouse_from_silver_parquet(target_storage, target_warehouse, key, None)
                total_releves += rel_cnt
                loaded_files += 1
            except Exception as exc:
                logger.error("[silver->ch][streaming] Erreur sur %s : %s", key, exc)
                errors.append({"key": key, "error": str(exc)})

    if scope in {"all", "historique"}:
        hist_keys = [k for k in target_storage.list("silver/velos_historique/") if k.endswith(".parquet")]
        by_year: dict[str, list[str]] = {}
        station_keys: list[str] = []
        for key in hist_keys:
            parts = key.split("/")
            ville_id = parts[2] if len(parts) >= 3 else ""
            if wanted_cities and ville_id.lower() not in wanted_cities:
                continue
            if key.endswith("velo_station_latest.parquet"):
                station_keys.append(key)
            elif len(parts) >= 4:
                by_year.setdefault(parts[3], []).append(key)

        for st_key in station_keys:
            try:
                st_cnt, _ = load_warehouse_from_silver_parquet(target_storage, target_warehouse, None, st_key)
                total_stations += st_cnt
            except Exception as exc:
                errors.append({"key": st_key, "error": str(exc)})

        def _load_year(keys_year: list[str]) -> tuple[int, int, list[dict[str, str]]]:
            yr_st = storage if storage is not None else get_storage(use_local=False)
            yr_wh = warehouse if warehouse is not None else get_warehouse()
            yf, yr, ye = 0, 0, []
            for k in keys_year:
                try:
                    _, rc = load_warehouse_from_silver_parquet(yr_st, yr_wh, k, None)
                    yr += rc
                    yf += 1
                except Exception as exc:
                    ye.append({"key": k, "error": str(exc)})
            return yf, yr, ye

        if by_year:
            workers = max(1, min(int(max_workers), len(by_year)))
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [executor.submit(_load_year, ks) for _, ks in sorted(by_year.items())]
                for fut in as_completed(futures):
                    yf, yr, ye = fut.result()
                    loaded_files += yf
                    total_releves += yr
                    errors.extend(ye)

    summary = {
        "scope": scope,
        "silver_files_loaded": loaded_files,
        "inserted_stations": total_stations,
        "inserted_releves": total_releves,
        "errors": errors,
    }
    logger.info("Synchronisation Silver Parquet -> ClickHouse terminée : %s", summary)
    return summary


def load_silver_clickhouse(
    warehouse: ClickHouseWarehouse,
    stations: Sequence[Mapping[str, Any]],
    releves: Sequence[Mapping[str, Any]],
) -> tuple[int, int]:
    """Charge les enregistrements silver dans urbanhub.velo_station et urbanhub.velo_releve."""
    station_rows = [tuple(row[col] for col in STATION_COLUMNS) for row in stations]
    releve_rows = [tuple(row[col] for col in RELEVE_COLUMNS) for row in releves]

    inserted_stations = warehouse.insert_rows("urbanhub.velo_station", station_rows, STATION_COLUMNS)
    inserted_releves = warehouse.insert_rows("urbanhub.velo_releve", releve_rows, RELEVE_COLUMNS)
    logger.info(
        "ClickHouse silver chargé : %d stations, %d relevés",
        inserted_stations,
        inserted_releves,
    )
    return inserted_stations, inserted_releves


def replay_bronze_to_silver(
    scope: str = "all",
    cities: Sequence[str] | None = None,
    freq: str | None = "1h",
    use_local: bool = False,
    storage: MinIOStorage | LocalBronzeStorage | None = None,
    warehouse: ClickHouseWarehouse | None = None,
    local_silver_base: Path | None = None,
    max_workers: int = 5,
) -> dict[str, Any]:
    """Rejoue le nettoyage complet depuis la couche bronze (streaming et/ou historique 5 ans) -> silver."""
    if scope not in {"all", "streaming", "historique"}:
        raise ValueError(f"Scope invalide : {scope!r} (attendu: all, streaming, historique)")

    wanted_cities = {c.strip().lower() for c in cities} if cities else None
    bbox_map = get_bbox_map()
    target_storage = storage or get_storage(use_local=use_local)
    target_warehouse = None if use_local else (warehouse or get_warehouse())
    silver_root = Path(local_silver_base or (get_settings().local_data_dir / "silver"))

    streaming_files = 0
    streaming_rows = 0
    hist_files = 0
    hist_rows = 0
    errors: list[dict[str, str]] = []

    # 1. Rejeu du bronze streaming JSON (bronze/velos/{ville_id}/...)
    if scope in {"all", "streaming"}:
        keys = [k for k in target_storage.list("bronze/velos/") if k.endswith(".json")]
        all_stations: list[dict[str, Any]] = []
        all_releves: list[dict[str, Any]] = []

        for key in keys:
            parts = key.split("/")
            ville_id = parts[2] if len(parts) >= 3 else ""
            if wanted_cities and ville_id.lower() not in wanted_cities:
                continue
            try:
                envelope = target_storage.get_json(key)
                st_rows, rel_rows, _ = clean_bronze_payload(
                    envelope,
                    bbox=bbox_map.get(ville_id),
                    max_age_minutes=None,  # rejeu d'archives bronze : pas de limite 30 min
                    ville_id=ville_id or None,
                )
                all_stations.extend(st_rows)
                all_releves.extend(rel_rows)
                streaming_files += 1
            except Exception as exc:
                logger.error("[streaming] Erreur sur %s : %s", key, exc)
                errors.append({"key": key, "error": str(exc)})

        if all_releves:
            deduped_releves = deduplicate_records(all_releves)
            stations_by_key = {(s["ville_id"], s["station_id"]): s for s in all_stations}
            deduped_stations = list(stations_by_key.values())
            streaming_rows = len(deduped_releves)

            if use_local:
                save_silver_local(
                    deduped_stations,
                    deduped_releves,
                    output_dir=silver_root / "velos",
                    filename_prefix="velo_releve_replay",
                )
            else:
                releve_key = save_silver_parquet_to_storage(
                    target_storage,
                    deduped_stations,
                    deduped_releves,
                    releve_key=build_silver_streaming_key(prefix="velo_releve_replay"),
                )
                if target_warehouse is not None:
                    load_warehouse_from_silver_parquet(
                        target_storage,
                        target_warehouse,
                        releve_key=releve_key,
                        station_key="silver/velos/velo_station_latest.parquet",
                    )

    # 2. Rejeu du bronze historique Parquet en parallèle par année
    if scope in {"all", "historique"}:
        hist_keys = [k for k in target_storage.list("bronze/velos_historique/") if k.endswith(".parquet")]
        keys_by_year: dict[str, list[tuple[str, dict[str, str]]]] = {}
        for key in hist_keys:
            meta = parse_historical_bronze_key(key)
            if meta is None:
                continue
            if wanted_cities and meta["ville_id"].lower() not in wanted_cities:
                continue
            keys_by_year.setdefault(meta["year"], []).append((key, meta))

        def _replay_year(year: str, items: list[tuple[str, dict[str, str]]]) -> tuple[int, int, list[dict[str, str]]]:
            yr_storage = storage if storage is not None else get_storage(use_local=use_local)
            yr_wh = None if use_local else (warehouse if warehouse is not None else get_warehouse())
            yr_files = 0
            yr_rows = 0
            yr_errors: list[dict[str, str]] = []
            for key, meta in items:
                ville_id = meta["ville_id"]
                try:
                    raw_bytes = yr_storage.get_bytes(key)
                    df_stations, df_releves, stats = clean_historical_parquet(
                        parquet_bytes=raw_bytes,
                        ville_id=ville_id,
                        network_id=meta["network_id"],
                        bbox=bbox_map.get(ville_id),
                        freq=freq,
                    )
                    if use_local:
                        out_dir = silver_root / "velos_historique" / ville_id
                        out_dir.mkdir(parents=True, exist_ok=True)
                        out_path = out_dir / f"velo_releve_{meta['ym']}.parquet"
                        df_releves.to_parquet(out_path, index=False)
                    else:
                        hist_station_key = f"silver/velos_historique/{ville_id}/velo_station_latest.parquet"
                        hist_releve_key = save_silver_parquet_to_storage(
                            yr_storage,
                            df_stations,
                            df_releves,
                            releve_key=build_silver_historical_key(ville_id, year, meta["ym"]),
                            station_key=hist_station_key,
                        )
                        if yr_wh is not None:
                            load_warehouse_from_silver_parquet(
                                yr_storage,
                                yr_wh,
                                releve_key=hist_releve_key,
                                station_key=hist_station_key,
                            )

                    yr_files += 1
                    yr_rows += stats["resampled_count"]
                    logger.info(
                        "[historique][%s][%s][%s] Re-nettoyé : %d bruts -> %d valides -> %d silver (rejets: %.1f %%)",
                        year,
                        ville_id,
                        meta["ym"],
                        stats["raw_count"],
                        stats["valid_count"],
                        stats["resampled_count"],
                        stats["rejection_rate"] * 100.0,
                    )
                except Exception as exc:
                    logger.error("[historique][%s] Erreur sur %s : %s", year, key, exc)
                    yr_errors.append({"key": key, "error": str(exc)})
            return yr_files, yr_rows, yr_errors

        if keys_by_year:
            workers = max(1, min(int(max_workers), len(keys_by_year)))
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(_replay_year, y, items): y
                    for y, items in sorted(keys_by_year.items())
                }
                for fut in as_completed(futures):
                    yf, yr, ye = fut.result()
                    hist_files += yf
                    hist_rows += yr
                    errors.extend(ye)

    summary = {
        "scope": scope,
        "streaming_files": streaming_files,
        "streaming_silver_rows": streaming_rows,
        "historique_files": hist_files,
        "historique_silver_rows": hist_rows,
        "total_silver_rows": streaming_rows + hist_rows,
        "errors": errors,
    }
    logger.info("Rejeu cleaning bronze -> silver terminé : %s", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="P2 — Rejeu du nettoyage unifié bronze -> silver (streaming + historique 5 ans)"
    )
    scope_group = parser.add_mutually_exclusive_group()
    scope_group.add_argument("--all", action="store_true", help="Nettoyer streaming + historique 5 ans (défaut)")
    scope_group.add_argument("--streaming", action="store_true", help="Nettoyer uniquement bronze/velos/")
    scope_group.add_argument("--historique", action="store_true", help="Nettoyer uniquement bronze/velos_historique/")
    parser.add_argument("--cities", nargs="*", help="Filtrer sur certains ville_id")
    parser.add_argument(
        "--freq",
        default="1h",
        help="Granularité de rééchantillonnage historique (défaut: 1h ; 'raw' pour brut)",
    )
    parser.add_argument("--local", action="store_true", help="Lire/écrire dans data/ au lieu de MinIO/ClickHouse")
    parser.add_argument(
        "--load-warehouse",
        action="store_true",
        help="Synchroniser uniquement les fichiers Parquet existants de silver/ vers ClickHouse",
    )
    parser.add_argument("--max-workers", type=int, default=5, help="Threads parallèles par année (défaut: 5)")
    args = parser.parse_args()

    if args.streaming:
        scope = "streaming"
    elif args.historique:
        scope = "historique"
    else:
        scope = "all"

    if args.load_warehouse:
        sync_silver_to_warehouse(scope=scope, cities=args.cities, max_workers=args.max_workers)
        return

    freq_val = None if str(args.freq).lower() == "raw" else args.freq
    replay_bronze_to_silver(
        scope=scope,
        cities=args.cities,
        freq=freq_val,
        use_local=args.local,
        max_workers=args.max_workers,
    )


if __name__ == "__main__":
    main()
