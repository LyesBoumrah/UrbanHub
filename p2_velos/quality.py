"""P2 — Règles de qualité pures (compteurs >= 0, capacité, bbox de la ville, fraîcheur).

Module 100 % bibliothèque standard Python (aucune dépendance externe) afin d'être
exécutable et testable directement en CI sans installer la stack complète.

Responsable : P2.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

REQUIRED_FIELDS = (
    "station_id",
    "station_name",
    "latitude",
    "longitude",
    "bikes_available",
    "free_slots",
    "timestamp",
)

# Seuils de détection de criticité (rééquilibrage)
DEFAULT_LOW_OCCUPANCY_THRESHOLD = 0.10
DEFAULT_HIGH_OCCUPANCY_THRESHOLD = 0.90
DEFAULT_MAX_AGE_MINUTES = 30
DEFAULT_MAX_FUTURE_SKEW_MINUTES = 5

# Motif CityBikes réel observé : "2026-11-19T11:27:15.476329+00:00Z" (double suffixe UTC)
_DOUBLE_UTC_SUFFIX_RE = re.compile(r"([+-]\d{2}:\d{2})Z$", re.IGNORECASE)


@dataclass(frozen=True)
class QualityReport:
    """Résultat agrégé d'un contrôle qualité sur un lot de relevés stations."""

    valid_records: list[dict[str, Any]] = field(default_factory=list)
    rejected_records: list[dict[str, Any]] = field(default_factory=list)
    reasons_summary: dict[str, int] = field(default_factory=dict)

    @property
    def total_count(self) -> int:
        return len(self.valid_records) + len(self.rejected_records)

    @property
    def valid_count(self) -> int:
        return len(self.valid_records)

    @property
    def rejected_count(self) -> int:
        return len(self.rejected_records)

    @property
    def rejection_rate(self) -> float:
        if self.total_count == 0:
            return 0.0
        return round(self.rejected_count / self.total_count, 4)


def parse_citybikes_timestamp(raw_ts: Any) -> datetime:
    """Parse un timestamp CityBikes en datetime UTC conscient du fuseau.

    Gère notamment la particularité de l'API CityBikes v2 qui renvoie des dates
    se terminant par '+00:00Z', ainsi que 'Z' ou un objet datetime existant.
    """
    if isinstance(raw_ts, datetime):
        dt = raw_ts
    elif isinstance(raw_ts, str):
        cleaned = raw_ts.strip()
        if not cleaned:
            raise ValueError("Timestamp vide")
        # Corrige '+00:00Z' -> '+00:00'
        cleaned = _DOUBLE_UTC_SUFFIX_RE.sub(r"\1", cleaned)
        if cleaned.endswith(("Z", "z")):
            cleaned = cleaned[:-1] + "+00:00"
        dt = datetime.fromisoformat(cleaned)
    else:
        raise ValueError(f"Format de timestamp non supporté : {type(raw_ts).__name__}")

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _is_valid_number(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if not isinstance(value, (int, float)):
        return False
    return value == value and abs(value) != float("inf")


def _is_valid_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def validate_coordinates(
    latitude: Any,
    longitude: Any,
    bbox: Mapping[str, float] | None = None,
) -> list[str]:
    """Vérifie la validité géographique (bornes mondiales, non nul-island, bbox ville)."""
    errors: list[str] = []
    if not (_is_valid_number(latitude) and _is_valid_number(longitude)):
        return ["invalid_coordinates_type"]

    lat = float(latitude)
    lon = float(longitude)

    if not (-90.0 <= lat <= 90.0) or not (-180.0 <= lon <= 180.0):
        errors.append("coordinates_out_of_world_bounds")
        return errors

    if lat == 0.0 and lon == 0.0:
        errors.append("null_island_coordinates")
        return errors

    if bbox is not None:
        lat_min = bbox.get("lat_min")
        lat_max = bbox.get("lat_max")
        lon_min = bbox.get("lon_min")
        lon_max = bbox.get("lon_max")
        if (
            lat_min is not None
            and lat_max is not None
            and lon_min is not None
            and lon_max is not None
            and not (lat_min <= lat <= lat_max and lon_min <= lon <= lon_max)
        ):
            errors.append("coordinates_outside_city_bbox")

    return errors


def validate_freshness(
    ts_utc: datetime,
    now_utc: datetime | None = None,
    max_age_minutes: int | None = DEFAULT_MAX_AGE_MINUTES,
    max_future_skew_minutes: int = DEFAULT_MAX_FUTURE_SKEW_MINUTES,
) -> list[str]:
    """Vérifie la fraîcheur d'un relevé streaming par rapport à l'instant de référence UTC."""
    errors: list[str] = []
    ref_now = now_utc or datetime.now(timezone.utc)
    if ref_now.tzinfo is None:
        ref_now = ref_now.replace(tzinfo=timezone.utc)

    if ts_utc > ref_now + timedelta(minutes=max_future_skew_minutes):
        errors.append("timestamp_in_future")

    if max_age_minutes is not None and ts_utc < ref_now - timedelta(minutes=max_age_minutes):
        errors.append("stale_timestamp")

    return errors


def classify_station_status(
    bikes_available: int,
    free_slots: int,
    low_threshold: float = DEFAULT_LOW_OCCUPANCY_THRESHOLD,
    high_threshold: float = DEFAULT_HIGH_OCCUPANCY_THRESHOLD,
) -> dict[str, Any]:
    """Calcule les indicateurs métier et le diagnostic de rééquilibrage d'une station."""
    capacity = bikes_available + free_slots
    if capacity <= 0:
        return {
            "capacity": 0,
            "taux_occupation": 0.0,
            "taux_utilisation": 0.0,
            "est_vide": 1,
            "est_pleine": 1,
            "est_critique": 1,
            "statut": "INACTIVE",
            "action_reequilibrage": "VERIFIER_STATION",
            "delta_velos_equilibre": 0,
        }

    taux_occupation = round(bikes_available / capacity, 4)
    # Taux d'utilisation = proportion de bornes libérées (vélos en circulation)
    taux_utilisation = round(free_slots / capacity, 4)
    est_vide = 1 if bikes_available == 0 else 0
    est_pleine = 1 if free_slots == 0 else 0

    if taux_occupation <= low_threshold:
        statut = "PENURIE_CRITIQUE"
        action = "REAPPROVISIONNER"
        est_critique = 1
    elif taux_occupation >= high_threshold:
        statut = "SATURATION_CRITIQUE"
        action = "DESENGORGER"
        est_critique = 1
    elif taux_occupation <= 0.20 or taux_occupation >= 0.80:
        statut = "SOUS_TENSION"
        action = "AUCUNE"
        est_critique = 0
    else:
        statut = "NORMAL"
        action = "AUCUNE"
        est_critique = 0

    # Nombre de vélos à ajouter (>0) ou retirer (<0) pour ramener la station à 50 % de sa capacité
    cible = round(capacity * 0.5)
    delta_velos = cible - bikes_available

    return {
        "capacity": capacity,
        "taux_occupation": taux_occupation,
        "taux_utilisation": taux_utilisation,
        "est_vide": est_vide,
        "est_pleine": est_pleine,
        "est_critique": est_critique,
        "statut": statut,
        "action_reequilibrage": action,
        "delta_velos_equilibre": delta_velos,
    }


def validate_station_record(
    record: Mapping[str, Any],
    bbox: Mapping[str, float] | None = None,
    now_utc: datetime | None = None,
    max_age_minutes: int | None = DEFAULT_MAX_AGE_MINUTES,
) -> tuple[bool, list[str]]:
    """Valide un relevé normalisé de station selon toutes les règles qualité P2.

    Règles contrôlées :
    1. Présence des 7 champs obligatoires (station_id, station_name, latitude,
       longitude, bikes_available, free_slots, timestamp).
    2. Compteurs entiers >= 0 (bikes_available, free_slots).
    3. Capacité strictement positive (bikes_available + free_slots > 0) et cohérence
       avec declared_slots si présent.
    4. Coordonnées géographiques valides et incluses dans la bbox de la ville.
    5. Timestamp UTC valide et frais (ni dans le futur, ni trop ancien).
    """
    errors: list[str] = []

    for field_name in REQUIRED_FIELDS:
        if field_name not in record or record[field_name] is None:
            errors.append(f"missing_{field_name}")

    station_id = record.get("station_id")
    if isinstance(station_id, str) and not station_id.strip():
        errors.append("empty_station_id")

    station_name = record.get("station_name")
    if isinstance(station_name, str) and not station_name.strip():
        errors.append("empty_station_name")

    bikes = record.get("bikes_available")
    slots = record.get("free_slots")

    if bikes is not None:
        if not _is_valid_int(bikes):
            errors.append("invalid_bikes_available_type")
        elif bikes < 0:
            errors.append("negative_bikes_available")

    if slots is not None:
        if not _is_valid_int(slots):
            errors.append("invalid_free_slots_type")
        elif slots < 0:
            errors.append("negative_free_slots")

    if _is_valid_int(bikes) and _is_valid_int(slots) and bikes >= 0 and slots >= 0:
        observed_capacity = bikes + slots
        if observed_capacity <= 0:
            errors.append("zero_capacity")
        declared_slots = record.get("declared_slots")
        if _is_valid_int(declared_slots) and declared_slots > 0:
            # Tolère des bornes hors service (observed <= declared) ou 2 vélos en surplus virtuel
            if observed_capacity > declared_slots + max(2, int(declared_slots * 0.25)):
                errors.append("capacity_exceeds_declared_slots")

    if record.get("latitude") is not None and record.get("longitude") is not None:
        errors.extend(validate_coordinates(record["latitude"], record["longitude"], bbox=bbox))

    raw_ts = record.get("timestamp")
    if raw_ts is not None:
        try:
            ts_utc = parse_citybikes_timestamp(raw_ts)
            errors.extend(validate_freshness(ts_utc, now_utc=now_utc, max_age_minutes=max_age_minutes))
        except ValueError:
            errors.append("invalid_timestamp")

    return (len(errors) == 0, errors)


def filter_and_audit_records(
    records: Sequence[Mapping[str, Any]],
    bbox: Mapping[str, float] | None = None,
    now_utc: datetime | None = None,
    max_age_minutes: int | None = DEFAULT_MAX_AGE_MINUTES,
) -> QualityReport:
    """Filtre un lot de relevés et retourne un rapport détaillé des rejets."""
    valid: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()

    for rec in records:
        is_valid, reasons = validate_station_record(
            rec,
            bbox=bbox,
            now_utc=now_utc,
            max_age_minutes=max_age_minutes,
        )
        if is_valid:
            valid.append(dict(rec))
        else:
            for r in reasons:
                reason_counts[r] += 1
            rejected.append({"record": dict(rec), "reasons": reasons})

    return QualityReport(
        valid_records=valid,
        rejected_records=rejected,
        reasons_summary=dict(reason_counts),
    )
