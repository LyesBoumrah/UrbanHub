"""Tests unitaires pour le sélecteur de durée flexible (jours, semaines, mois, années)."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from common.period import compute_date_period, compute_window, months_span, parse_duration

REF_NOW = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)


def test_parse_duration_formats_fr_et_en():
    assert parse_duration("1 jour") == (1, "days")
    assert parse_duration("3 jours") == (3, "days")
    assert parse_duration("2 semaines") == (2, "weeks")
    assert parse_duration("1 semaine(s)") == (1, "weeks")
    assert parse_duration("3 mois") == (3, "months")
    assert parse_duration("1 an") == (1, "years")
    assert parse_duration("5 années") == (5, "years")
    assert parse_duration("2w") == (2, "weeks")
    assert parse_duration("6m") == (6, "months")
    assert parse_duration("2y") == (2, "years")


def test_parse_duration_invalide():
    with pytest.raises(ValueError):
        parse_duration("0 semaines")
    with pytest.raises(ValueError):
        parse_duration("trois mois")
    with pytest.raises(ValueError):
        parse_duration("2 siecles")


def test_compute_window_et_dates():
    start_d, end_d = compute_date_period(duration="3 jours", today=date(2026, 11, 20))
    assert (start_d, end_d) == ("2026-11-17", "2026-11-20")

    start_w, end_w = compute_date_period(weeks=2, today=date(2026, 11, 20))
    assert (start_w, end_w) == ("2026-11-06", "2026-11-20")

    start_m, end_m = compute_date_period(months=3, today=date(2026, 11, 20))
    assert (start_m, end_m) == ("2026-08-20", "2026-11-20")

    start_y, end_y = compute_date_period(duration="1 an", today=date(2026, 11, 20))
    assert (start_y, end_y) == ("2025-11-20", "2026-11-20")

    # Cas limite : 31 mars - 1 mois -> 28 février
    s_edge, _ = compute_window(months=1, now=datetime(2026, 3, 31, 10, 0, tzinfo=timezone.utc))
    assert s_edge.date() == date(2026, 2, 28)

    s_3m, e_3m = compute_window(months=2, now=REF_NOW)
    assert months_span(s_3m, e_3m) == 3
