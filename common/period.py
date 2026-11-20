"""Utilitaires partagés pour choisir et calculer une durée de récupération flexible :
1, 2, 3... jour(s), semaine(s), mois ou année(s).

Exemples acceptés par `parse_duration` :
  - "1 jour", "7 jours", "7j", "7d"
  - "1 semaine", "2 semaines", "3s", "3w"
  - "1 mois", "3 mois", "6m"
  - "1 an", "1 année", "2 ans", "5 années", "5y"

Responsable : commun.
"""

from __future__ import annotations

import calendar
import re
from datetime import date, datetime, timedelta, timezone

UNIT_ALIASES: dict[str, str] = {
    "j": "days",
    "jr": "days",
    "jour": "days",
    "jours": "days",
    "jour(s)": "days",
    "d": "days",
    "day": "days",
    "days": "days",
    "s": "weeks",
    "sem": "weeks",
    "semaine": "weeks",
    "semaines": "weeks",
    "semaine(s)": "weeks",
    "w": "weeks",
    "week": "weeks",
    "weeks": "weeks",
    "m": "months",
    "mois": "months",
    "mo": "months",
    "month": "months",
    "months": "months",
    "a": "years",
    "an": "years",
    "ans": "years",
    "annee": "years",
    "annees": "years",
    "année": "years",
    "années": "years",
    "année(s)": "years",
    "y": "years",
    "yr": "years",
    "year": "years",
    "years": "years",
}

UI_DURATION_UNITS: tuple[str, ...] = ("jour(s)", "semaine(s)", "mois", "année(s)")
DEFAULT_FETCH_DURATION = "1 semaine"

_DURATION_RE = re.compile(r"^\s*(\d+)\s*([a-zA-Zéèêàù()]+)\s*$")


def parse_duration(spec: str) -> tuple[int, str]:
    """Parse une chaîne de durée (ex. '2 semaines', '3 mois', '5 ans') -> `(quantite, unite_canonique)`."""
    if not spec or not isinstance(spec, str):
        raise ValueError(f"Durée invalide : {spec!r}")
    match = _DURATION_RE.match(spec.strip())
    if not match:
        raise ValueError(
            f"Format de durée non reconnu : {spec!r} "
            "(exemples : '3 jours', '2 semaines', '1 mois', '5 ans')"
        )
    amount = int(match.group(1))
    raw_unit = match.group(2).lower()
    if amount <= 0:
        raise ValueError(f"La durée doit être strictement positive : {spec!r}")
    unit = UNIT_ALIASES.get(raw_unit)
    if unit is None:
        raise ValueError(
            f"Unité de durée inconnue : {raw_unit!r} "
            "(attendu : jour(s), semaine(s), mois, année(s))"
        )
    return amount, unit


def subtract_months(dt: datetime, months: int) -> datetime:
    """Recule un datetime d'un nombre exact de mois calendaires (gère fin de mois / 29 fév)."""
    total_months = dt.year * 12 + (dt.month - 1) - months
    year, month_zero = divmod(total_months, 12)
    month = month_zero + 1
    last_day = calendar.monthrange(year, month)[1]
    return dt.replace(year=year, month=month, day=min(dt.day, last_day))


def compute_window(
    *,
    days: int | None = None,
    weeks: int | None = None,
    months: int | None = None,
    years: int | None = None,
    duration: str | None = None,
    default_years: int = 5,
    now: datetime | None = None,
) -> tuple[datetime, datetime]:
    """Calcule la fenêtre temporelle `(start_utc, end_utc)` selon la durée choisie.

    Priorité : `duration` (chaîne) > `days` / `weeks` / `months` / `years` > `default_years`.
    """
    end = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)

    if duration:
        amount, unit = parse_duration(duration)
        if unit == "days":
            return end - timedelta(days=amount), end
        if unit == "weeks":
            return end - timedelta(weeks=amount), end
        if unit == "months":
            return subtract_months(end, amount), end
        return subtract_months(end, amount * 12), end

    if days is not None:
        if days <= 0:
            raise ValueError("days doit être > 0")
        return end - timedelta(days=int(days)), end
    if weeks is not None:
        if weeks <= 0:
            raise ValueError("weeks doit être > 0")
        return end - timedelta(weeks=int(weeks)), end
    if months is not None:
        if months <= 0:
            raise ValueError("months doit être > 0")
        return subtract_months(end, int(months)), end
    if years is not None:
        if years <= 0:
            raise ValueError("years doit être > 0")
        return subtract_months(end, int(years) * 12), end

    return subtract_months(end, int(default_years) * 12), end


def compute_date_period(
    *,
    days: int | None = None,
    weeks: int | None = None,
    months: int | None = None,
    years: int | None = None,
    duration: str | None = None,
    default_years: int = 5,
    today: date | None = None,
) -> tuple[str, str]:
    """Retourne `(start_date_iso, end_date_iso)` au format `YYYY-MM-DD`."""
    ref = (
        datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
        if today is not None
        else datetime.now(timezone.utc)
    )
    start_dt, end_dt = compute_window(
        days=days,
        weeks=weeks,
        months=months,
        years=years,
        duration=duration,
        default_years=default_years,
        now=ref,
    )
    return start_dt.date().isoformat(), end_dt.date().isoformat()


def months_span(start_utc: datetime, end_utc: datetime) -> int:
    """Nombre de mois calendaires distincts couverts par `[start_utc, end_utc]` (minimum 1)."""
    diff = (end_utc.year - start_utc.year) * 12 + (end_utc.month - start_utc.month) + 1
    return max(1, diff)


def add_duration_arguments(parser: object, default_help_suffix: str = "") -> None:
    """Ajoute les options CLI homogènes `--duration`, `--days`, `--weeks`, `--months`, `--years`."""
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--duration",
        type=str,
        default=None,
        help=(
            "Durée flexible (ex. '3 jours', '2 semaines', '1 mois', '2 ans')"
            + (f" — {default_help_suffix}" if default_help_suffix else "")
        ),
    )
    group.add_argument("--days", type=int, default=None, help="Durée en jours (ex. 1, 3, 7)")
    group.add_argument("--weeks", type=int, default=None, help="Durée en semaines (ex. 1, 2, 3)")
    group.add_argument("--months", type=int, default=None, help="Durée en mois (ex. 1, 3, 6)")
    group.add_argument("--years", type=int, default=None, help="Durée en années (ex. 1, 2, 5)")
