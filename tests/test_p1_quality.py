"""Tests P1 — règles de qualité météo."""

from __future__ import annotations

import pandas as pd

from p1_meteo.quality import check_quality, quality_summary


def test_temperature_hors_bornes():
    df = pd.DataFrame(
        {
            "heure_utc": [pd.Timestamp("2026-01-01T00:00:00Z")],
            "temperature": [99.0],
        }
    )
    result = check_quality(df)
    assert result.loc[0, "is_valid"] == False  # noqa: E712
    assert "temperature_hors_bornes" in result.loc[0, "quality_flags"]


def test_donnees_valides():
    df = pd.DataFrame(
        {
            "heure_utc": [pd.Timestamp("2026-01-01T00:00:00Z")],
            "temperature": [12.5],
            "pressure": [1013.2],
            "wind_speed": [4.1],
            "visibility": [16000.0],
            "precipitation": [0.0],
        }
    )
    result = check_quality(df)
    assert result.loc[0, "is_valid"] == True  # noqa: E712
    assert result.loc[0, "quality_flags"] == ""


def test_precipitation_negative():
    df = pd.DataFrame(
        {
            "heure_utc": [pd.Timestamp("2026-01-01T00:00:00Z")],
            "precipitation": [-1.0],
        }
    )
    result = check_quality(df)
    assert result.loc[0, "is_valid"] == False  # noqa: E712


def test_resume_qualite():
    df = pd.DataFrame({"is_valid": [True, False, True]})
    assert quality_summary(df) == {
        "total": 3,
        "valides": 2,
        "invalides": 1,
        "taux_validite": 66.67,
    }
