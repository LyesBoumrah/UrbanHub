"""Période d'étude commune aux 3 sources : les n dernières années glissantes, en mois complets.

Ex. le 20/11/2026 avec n=5 : du 01/11/2021 au 31/10/2026 (le mois en cours, incomplet, est exclu).
Toutes les extractions (P1 météo, P2 vélos, P3 pollution) utilisent cette même fonction,
pour que la jointure P4 porte sur des dates strictement identiques.

Responsable : commun.
"""
from __future__ import annotations

from datetime import date, timedelta

DEFAULT_YEARS = 5


def study_period(nb_years: int = DEFAULT_YEARS, today: date | None = None) -> tuple[date, date]:
    """(premier jour, dernier jour) de la période d'étude, bornes incluses."""
    today = today or date.today()
    first_of_month = today.replace(day=1)
    end = first_of_month - timedelta(days=1)
    start = first_of_month.replace(year=first_of_month.year - nb_years)
    return start, end


def year_month(d: date) -> str:
    """date(2024, 11, 3) -> '202411' (format des dumps CityBikes)."""
    return f"{d.year}{d.month:02d}"
