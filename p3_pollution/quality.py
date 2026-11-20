"""P3 — Règles de qualité pures (normalisation des polluants, conversion d'unités ppb/ppm/mg -> µg/m³,
bornes de plausibilité par polluant, seuils OMS / ATMO, fraîcheur).

Responsable : P3.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

CANONICAL_UNIT = "µg/m³"

# Correspondance souple des codes polluants (OpenAQ v3 + libellés métier de l'énoncé)
POLLUTANT_ALIASES: dict[str, str] = {
    "pm25": "pm25",
    "pm2.5": "pm25",
    "pm2_5": "pm25",
    "pm10": "pm10",
    "no2": "no2",
    "o3": "o3",
    "co": "co",
}

POLLUTANT_LABELS: dict[str, str] = {
    "pm25": "PM2.5",
    "pm10": "PM10",
    "no2": "NO2",
    "o3": "O3",
    "co": "CO",
}

TARGET_POLLUTANTS: tuple[str, ...] = ("pm25", "pm10", "no2", "o3", "co")

# Masse molaire (g/mol) des gaz pour conversion ppb/ppm -> µg/m³ à 20 °C et 1013,25 hPa
# (volume molaire Vm = 24,055 L/mol, convention directives européennes qualité de l'air)
MOLAR_VOLUME_20C = 24.055
GAS_MOLAR_MASS: dict[str, float] = {
    "no2": 46.0055,
    "o3": 48.00,
    "co": 28.0101,
}

# Bornes physiques de plausibilité en µg/m³ (min, max)
POLLUTANT_BOUNDS: dict[str, tuple[float, float]] = {
    "pm25": (0.0, 1000.0),
    "pm10": (0.0, 2000.0),
    "no2": (0.0, 1000.0),
    "o3": (0.0, 600.0),
    "co": (0.0, 50000.0),
}

# Valeurs guides OMS 2021 (court terme / journalier) en µg/m³ — servent à normaliser les 5 polluants
WHO_THRESHOLDS_UGM3: dict[str, float] = {
    "pm25": 15.0,
    "pm10": 45.0,
    "no2": 25.0,
    "o3": 100.0,
    "co": 4000.0,
}

# Seuils d'épisode de pollution (indice ATMO « Mauvais » / seuil d'information) en µg/m³
EPISODE_THRESHOLDS_UGM3: dict[str, float] = {
    "pm25": 25.0,
    "pm10": 50.0,
    "no2": 120.0,
    "o3": 130.0,
    "co": 10000.0,
}


def normalize_pollutant(raw: Any) -> str | None:
    """Normalise un nom de polluant ('PM2.5', 'pm25', 'O3'...) vers le code canonique ('pm25', 'o3'...)."""
    if raw is None:
        return None
    cleaned = str(raw).strip().lower().replace(" ", "")
    return POLLUTANT_ALIASES.get(cleaned)


def pollutant_label(code: str) -> str:
    """Retourne le libellé d'affichage officiel ('PM2.5', 'PM10', 'NO2', 'O3', 'CO')."""
    norm = normalize_pollutant(code) or str(code)
    return POLLUTANT_LABELS.get(norm, str(code).upper())


def normalize_unit(raw_unit: Any) -> str:
    """Normalise l'écriture d'une unité de concentration."""
    if raw_unit is None:
        return ""
    u = str(raw_unit).strip().lower().replace(" ", "")
    if u in {"µg/m³", "ug/m3", "µg/m3", "ug/m³", "μg/m³", "μg/m3"}:
        return CANONICAL_UNIT
    if u in {"mg/m³", "mg/m3"}:
        return "mg/m³"
    if u in {"ppb", "ppbv"}:
        return "ppb"
    if u in {"ppm", "ppmv"}:
        return "ppm"
    return str(raw_unit).strip()


def convert_to_ugm3(value: Any, pollutant: Any, unit: Any) -> tuple[float | None, str]:
    """Convertit une concentration en µg/m³.

    Renvoie `(valeur_ugm3, flag_erreur)` où `flag_erreur == ""` si la conversion réussit.
    """
    try:
        val = float(value)
    except (TypeError, ValueError):
        return None, "valeur_manquante"
    if math.isnan(val) or math.isinf(val):
        return None, "valeur_manquante"

    pol = normalize_pollutant(pollutant)
    if pol is None:
        return None, "polluant_inconnu"

    norm_u = normalize_unit(unit)
    if norm_u == CANONICAL_UNIT:
        return round(val, 3), ""
    if norm_u == "mg/m³":
        return round(val * 1000.0, 3), ""
    if norm_u in {"ppb", "ppm"}:
        if pol not in GAS_MOLAR_MASS:
            return None, "unite_incompatible"
        factor_ppb = GAS_MOLAR_MASS[pol] / MOLAR_VOLUME_20C
        mult = factor_ppb if norm_u == "ppb" else factor_ppb * 1000.0
        return round(val * mult, 3), ""
    return None, "unite_inconnue"


def validate_value_bounds(value_ugm3: float | None, pollutant: str) -> str:
    """Vérifie qu'une concentration en µg/m³ respecte les bornes physiques du polluant."""
    if value_ugm3 is None or math.isnan(value_ugm3):
        return "valeur_manquante"
    if value_ugm3 < 0.0:
        return "valeur_negative"
    lo, hi = POLLUTANT_BOUNDS.get(pollutant, (0.0, 100000.0))
    if value_ugm3 < lo or value_ugm3 > hi:
        return "valeur_hors_bornes"
    return ""


def validate_coordinates_france(lat: Any, lon: Any) -> str:
    """Vérifie que les coordonnées GPS sont situées en France métropolitaine."""
    try:
        lat_f, lon_f = float(lat), float(lon)
    except (TypeError, ValueError):
        return "coordonnees_manquantes"
    if math.isnan(lat_f) or math.isnan(lon_f):
        return "coordonnees_manquantes"
    if not (41.0 <= lat_f <= 51.6 and -5.8 <= lon_f <= 10.0):
        return "coordonnees_hors_france"
    return ""


def check_freshness(ts_utc: datetime | None, now_utc: datetime | None = None, max_age_hours: float = 30.0) -> bool:
    """True si la mesure date de moins de `max_age_hours` (seuil capteur muet = 30 h)."""
    if ts_utc is None:
        return False
    ref = now_utc or datetime.now(timezone.utc)
    age = ref - ts_utc.astimezone(timezone.utc)
    return timedelta(minutes=-10) <= age <= timedelta(hours=max_age_hours)


def compute_who_ratio(value_ugm3: float | None, pollutant: str) -> float | None:
    """Ratio de la concentration par rapport au seuil OMS 2021 (1.0 = seuil OMS atteint)."""
    if value_ugm3 is None or math.isnan(value_ugm3) or value_ugm3 < 0:
        return None
    pol = normalize_pollutant(pollutant)
    thresh = WHO_THRESHOLDS_UGM3.get(pol or "")
    if not thresh:
        return None
    return round(value_ugm3 / thresh, 4)


def check_quality_df(df: pd.DataFrame) -> pd.DataFrame:
    """Valide un DataFrame de mesures pollution (convertit en µg/m³ et ajoute `quality_flags` + `is_valid`)."""
    out = df.copy()
    if out.empty:
        out["quality_flags"] = pd.Series(dtype=str)
        out["is_valid"] = pd.Series(dtype=bool)
        return out

    flags_list: list[str] = []
    conv_values: list[float | None] = []
    norm_polls: list[str | None] = []

    has_lat_lon = "latitude" in out.columns and "longitude" in out.columns
    has_ts = "timestamp" in out.columns

    for idx, row in out.iterrows():
        row_flags: list[str] = []
        pol = normalize_pollutant(row.get("pollutant"))
        norm_polls.append(pol)
        if pol is None:
            row_flags.append("polluant_inconnu")

        val_ugm3, conv_err = convert_to_ugm3(row.get("value"), row.get("pollutant"), row.get("unit", CANONICAL_UNIT))
        if conv_err:
            if conv_err not in row_flags:
                row_flags.append(conv_err)
        elif pol is not None:
            b_err = validate_value_bounds(val_ugm3, pol)
            if b_err:
                row_flags.append(b_err)

        if has_lat_lon:
            c_err = validate_coordinates_france(row.get("latitude"), row.get("longitude"))
            if c_err:
                row_flags.append(c_err)

        if has_ts and pd.isna(row.get("timestamp")):
            row_flags.append("timestamp_manquant")

        conv_values.append(val_ugm3)
        flags_list.append(";".join(row_flags))

    out["pollutant"] = norm_polls
    out["value"] = pd.Series(conv_values, index=out.index, dtype="float64")
    out["unit"] = CANONICAL_UNIT
    out["quality_flags"] = flags_list
    out["is_valid"] = out["quality_flags"] == ""
    return out


def quality_summary(df: pd.DataFrame) -> dict[str, Any]:
    """Résumé statistique du contrôle qualité P3."""
    if df.empty:
        return {"total": 0, "valides": 0, "invalides": 0, "taux_validite": 0.0, "motifs": {}}
    total = len(df)
    valid_mask = df["is_valid"].astype(bool) if "is_valid" in df else pd.Series(True, index=df.index)
    valides = int(valid_mask.sum())
    motifs: dict[str, int] = {}
    if "quality_flags" in df:
        for fl in df.loc[~valid_mask, "quality_flags"]:
            for part in str(fl).split(";"):
                if part:
                    motifs[part] = motifs.get(part, 0) + 1
    return {
        "total": total,
        "valides": valides,
        "invalides": total - valides,
        "taux_validite": round(valides / total * 100.0, 2),
        "motifs": motifs,
    }
