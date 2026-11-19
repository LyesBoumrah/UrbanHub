"""P1 — Règles de qualité pures (bornes de plausibilité, codes qualité ISD).

Responsable : P1.
"""

from __future__ import annotations

import pandas as pd

TEMP_MIN, TEMP_MAX = -60.0, 60.0
PRESSURE_MIN, PRESSURE_MAX = 870.0, 1085.0
WIND_MIN, WIND_MAX = 0.0, 100.0
VISIBILITY_MIN, VISIBILITY_MAX = 0.0, 100_000.0
PRECIPITATION_MIN = 0.0


def check_quality(df: pd.DataFrame) -> pd.DataFrame:
    """Ajoute une colonne is_valid et quality_flags.

    Une ligne est valide si toutes les valeurs présentes respectent les bornes.
    """
    if df.empty:
        out = df.copy()
        out["is_valid"] = pd.Series(dtype=bool)
        out["quality_flags"] = pd.Series(dtype=str)
        return out

    out = df.copy()
    flags = pd.Series("", index=out.index, dtype=object)

    def flag(condition: pd.Series, label: str) -> None:
        nonlocal flags
        flags = flags.mask(condition, flags + label + ";")

    if "temperature" in out:
        flag(
            out["temperature"].notna()
            & ((out["temperature"] < TEMP_MIN) | (out["temperature"] > TEMP_MAX)),
            "temperature_hors_bornes",
        )
    if "pressure" in out:
        flag(
            out["pressure"].notna()
            & ((out["pressure"] < PRESSURE_MIN) | (out["pressure"] > PRESSURE_MAX)),
            "pression_hors_bornes",
        )
    if "wind_speed" in out:
        flag(
            out["wind_speed"].notna()
            & ((out["wind_speed"] < WIND_MIN) | (out["wind_speed"] > WIND_MAX)),
            "vent_hors_bornes",
        )
    if "visibility" in out:
        flag(
            out["visibility"].notna()
            & ((out["visibility"] < VISIBILITY_MIN) | (out["visibility"] > VISIBILITY_MAX)),
            "visibilite_hors_bornes",
        )
    if "precipitation" in out:
        flag(
            out["precipitation"].notna() & (out["precipitation"] < PRECIPITATION_MIN),
            "precipitation_negative",
        )
    if "heure_utc" in out:
        flag(out["heure_utc"].isna(), "timestamp_manquant")

    out["quality_flags"] = flags.str.rstrip(";")
    out["is_valid"] = out["quality_flags"] == ""
    return out


def quality_summary(df: pd.DataFrame) -> dict:
    """Retourne un résumé du contrôle qualité."""
    if df.empty:
        return {"total": 0, "valides": 0, "invalides": 0, "taux_validite": 0.0}
    total = len(df)
    valides = int(df["is_valid"].sum())
    return {
        "total": total,
        "valides": valides,
        "invalides": total - valides,
        "taux_validite": round(valides / total * 100, 2),
    }
