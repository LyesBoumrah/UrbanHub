"""P1 — Règles de qualité pures (codes qualité ISD, bornes de plausibilité, sauts temporels).

Responsable : P1.
"""

from __future__ import annotations

import pandas as pd

# Bornes physiques de plausibilité (France métropolitaine, marges larges)
TEMP_MIN, TEMP_MAX = -60.0, 60.0
DEW_MIN, DEW_MAX = -70.0, 40.0
PRESSURE_MIN, PRESSURE_MAX = 870.0, 1085.0
WIND_MIN, WIND_MAX = 0.0, 100.0
WIND_DIR_MIN, WIND_DIR_MAX = 0.0, 360.0
VISIBILITY_MIN, VISIBILITY_MAX = 0.0, 160_000.0
PRECIPITATION_MIN, PRECIPITATION_MAX = 0.0, 300.0  # mm/h
MAX_TEMP_JUMP_PER_HOUR = 15.0  # °C : au-delà, saut physiquement improbable (pic capteur)

# Codes qualité ISD (NOAA) : 2/6 = suspect, 3/7 = erroné -> valeur rejetée
ISD_BAD_QUALITY = frozenset({"2", "3", "6", "7"})

BOUNDS: dict[str, tuple[float, float, str]] = {
    "temperature": (TEMP_MIN, TEMP_MAX, "temperature_hors_bornes"),
    "dew_point": (DEW_MIN, DEW_MAX, "point_rosee_hors_bornes"),
    "pressure": (PRESSURE_MIN, PRESSURE_MAX, "pression_hors_bornes"),
    "wind_speed": (WIND_MIN, WIND_MAX, "vent_hors_bornes"),
    "wind_direction": (WIND_DIR_MIN, WIND_DIR_MAX, "direction_vent_hors_bornes"),
    "visibility": (VISIBILITY_MIN, VISIBILITY_MAX, "visibilite_hors_bornes"),
    "precipitation": (PRECIPITATION_MIN, PRECIPITATION_MAX, "precipitation_hors_bornes"),
}


def is_bad_isd_quality(code: object) -> bool:
    """True si le code qualité ISD signale une valeur suspecte ou erronée."""
    return str(code).strip() in ISD_BAD_QUALITY


def check_quality(df: pd.DataFrame) -> pd.DataFrame:
    """Ajoute les colonnes `quality_flags` (motifs séparés par « ; ») et `is_valid`.

    Une ligne est valide si toutes les valeurs présentes respectent les bornes.
    """
    out = df.copy()
    if df.empty:
        out["is_valid"] = pd.Series(dtype=bool)
        out["quality_flags"] = pd.Series(dtype=str)
        return out

    flags = pd.Series("", index=out.index, dtype=object)

    def flag(condition: pd.Series, label: str) -> None:
        nonlocal flags
        flags = flags.mask(condition.fillna(False).astype(bool), flags + label + ";")

    for col, (lo, hi, label) in BOUNDS.items():
        if col in out:
            values = pd.to_numeric(out[col], errors="coerce")
            if label == "precipitation_hors_bornes":
                flag(values.notna() & (values < lo), "precipitation_negative")
                flag(values.notna() & (values > hi), label)
            else:
                flag(values.notna() & ((values < lo) | (values > hi)), label)
    if "temperature" in out and "dew_point" in out:
        t, d = pd.to_numeric(out["temperature"], errors="coerce"), pd.to_numeric(out["dew_point"], errors="coerce")
        flag(t.notna() & d.notna() & (d > t + 0.5), "rosee_superieure_temperature")
    if "heure_utc" in out:
        flag(out["heure_utc"].isna(), "timestamp_manquant")

    out["quality_flags"] = flags.str.rstrip(";")
    out["is_valid"] = out["quality_flags"] == ""
    return out


def nullify_out_of_bounds(df: pd.DataFrame) -> pd.DataFrame:
    """Remplace par NaN les valeurs hors bornes (la ligne reste exploitable pour les autres variables)."""
    out = df.copy()
    for col, (lo, hi, _) in BOUNDS.items():
        if col in out:
            values = pd.to_numeric(out[col], errors="coerce")
            out[col] = values.where((values >= lo) & (values <= hi))
    if "temperature" in out and "dew_point" in out:
        out["dew_point"] = out["dew_point"].where(~(out["dew_point"] > out["temperature"] + 0.5))
    return out


def flag_temperature_spikes(df: pd.DataFrame, max_jump: float = MAX_TEMP_JUMP_PER_HOUR) -> pd.Series:
    """Détecte les pics isolés de température (saut > max_jump °C vers l'heure précédente ET suivante).

    `df` doit être trié par (station_id, heure_utc).
    """
    if df.empty or "temperature" not in df:
        return pd.Series(False, index=df.index)
    g = df.groupby("station_id", sort=False)["temperature"]
    prev_jump = (df["temperature"] - g.shift(1)).abs()
    next_jump = (df["temperature"] - g.shift(-1)).abs()
    return ((prev_jump > max_jump) & (next_jump > max_jump)).fillna(False)


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


def completeness(df: pd.DataFrame, columns: list[str]) -> dict[str, float]:
    """Taux de remplissage (%) par variable."""
    if df.empty:
        return {c: 0.0 for c in columns}
    return {c: round(float(df[c].notna().mean() * 100), 2) if c in df else 0.0 for c in columns}
