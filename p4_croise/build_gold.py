"""P4 — Construction de la couche Gold (fichiers JSON métier uniquement).

Architecture stricte Medallion respectée :
  - Couche Bronze (`bronze/`) : données brutes issues des APIs.
  - Couche Silver (`silver/`) : UNIQUEMENT les fichiers Parquet obtenus après nettoyage du bronze.
  - Couche Gold (`gold/`)     : UNIQUEMENT des fichiers JSON (`.json`) répondant aux questions métier
                                des 4 parties de l'exercice :
      * `gold/p1_meteo/q1_periodes_anormales.json` … `q4_jours_extremes.json`
      * `gold/p2_velos/q1_utilisation_stations.json` … `q5_stations_critiques_reequilibrage.json`
      * `gold/p3_pollution/q1_zones_plus_polluees.json` … `q4_episodes_pollution_anormale.json`
      * `gold/p4_croise/q1_meteo_pollution.json` … `q4_interactions_fortes.json`
      * `gold/p4_croise/gold_ville_heure.json` & `gold/synthese_indicateurs_urbains.json`

Usage :
  python -m p4_croise.build_gold [--local]

Responsable : commun / P4.
"""

from __future__ import annotations

import argparse
import math
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from common.config import get_settings, load_ref_ville
from common.log import get_logger
from common.storage import get_storage
from p1_meteo.cleaning import (
    SILVER_KEY as METEO_SILVER_KEY,
    aggregate_ville_heure as aggregate_meteo_ville_heure,
)
from p3_pollution.cleaning import (
    MESURE_SILVER_KEY,
    aggregate_ville_heure as aggregate_air_ville_heure,
)

log = get_logger("p4.build_gold")

GOLD_PREFIX = "gold"
GOLD_JSON_KEY = f"{GOLD_PREFIX}/p4_croise/gold_ville_heure.json"
GOLD_SYNTHESE_KEY = f"{GOLD_PREFIX}/synthese_indicateurs_urbains.json"

GOLD_COLS = [
    "ville_id",
    "heure_utc",
    "temperature",
    "dew_point",
    "relative_humidity",
    "pressure",
    "wind_speed",
    "precipitation",
    "visibility",
    "nb_stations_velo",
    "velos_dispo_moyens",
    "taux_utilisation_velo",
    "taux_occupation_velo",
    "pct_stations_vides",
    "pct_stations_pleines",
    "pct_stations_critiques",
    "pm25",
    "pm10",
    "no2",
    "o3",
    "co",
    "indice_pollution_oms",
    "polluant_dominant",
    "indice_confort_meteo",
    "score_mobilite_douce",
    "condition_favorable_velo",
    "score_interaction_urbaine",
    "regime_interaction",
]


def _json_safe_value(val: Any) -> Any:
    """Convertit une valeur pandas/numpy en type JSON natif strict (pas de NaN/Inf/Timestamp)."""
    if val is None or val is pd.NA:
        return None
    if isinstance(val, (pd.Timestamp, datetime)):
        return None if pd.isna(val) else val.isoformat()
    if isinstance(val, date):
        return val.isoformat()
    if isinstance(val, (bool, np.bool_)):
        return bool(val)
    if isinstance(val, (int, np.integer)):
        return int(val)
    if isinstance(val, (float, np.floating)):
        fval = float(val)
        return None if (math.isnan(fval) or math.isinf(fval)) else fval
    if isinstance(val, (list, tuple)):
        return [_json_safe_value(x) for x in val]
    if isinstance(val, dict):
        return {str(k): _json_safe_value(v) for k, v in val.items()}
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return str(val)


def _df_to_records(df: pd.DataFrame | None, max_rows: int | None = None) -> list[dict[str, Any]]:
    """Transforme un DataFrame pandas en liste de dictionnaires strictement sérialisables en JSON."""
    if df is None or df.empty:
        return []
    sub = df.head(max_rows) if max_rows is not None else df
    return [
        {str(col): _json_safe_value(val) for col, val in row.items()}
        for row in sub.to_dict(orient="records")
    ]


def _make_gold_payload(
    partie: str,
    question_id: str,
    question_metier: str,
    resultats: Any,
    nb_enregistrements: int,
) -> dict[str, Any]:
    """Construit l'enveloppe JSON normalisée d'un livrable métier en couche Gold."""
    return {
        "partie": partie,
        "question_id": question_id,
        "question_metier": question_metier,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "nb_enregistrements": int(nb_enregistrements),
        "resultats": resultats,
    }


def aggregate_velos_ville_heure(velos_df: pd.DataFrame) -> pd.DataFrame:
    """Agrège un DataFrame de relevés vélos (P2) au grain `(ville_id, heure_utc)`."""
    if velos_df.empty:
        return pd.DataFrame(
            columns=[
                "ville_id", "heure_utc", "nb_stations_velo", "velos_dispo_moyens",
                "taux_utilisation_velo", "taux_occupation_velo", "pct_stations_vides",
                "pct_stations_pleines", "pct_stations_critiques",
            ]
        )
    df = velos_df.copy()
    if "heure_utc" not in df:
        df["heure_utc"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dt.floor("h")
    else:
        df["heure_utc"] = pd.to_datetime(df["heure_utc"], utc=True, errors="coerce").dt.floor("h")

    if "taux_utilisation_moyen" in df and "taux_utilisation_velo" not in df:
        df = df.rename(
            columns={
                "nb_stations_actives": "nb_stations_velo",
                "taux_utilisation_moyen": "taux_utilisation_velo",
                "taux_occupation_moyen": "taux_occupation_velo",
            }
        )
        cols = [
            "ville_id", "heure_utc", "nb_stations_velo", "velos_dispo_moyens",
            "taux_utilisation_velo", "taux_occupation_velo", "pct_stations_vides",
            "pct_stations_pleines", "pct_stations_critiques",
        ]
        for c in cols:
            if c not in df:
                df[c] = np.nan
        return df[cols].drop_duplicates(subset=["ville_id", "heure_utc"], keep="last").reset_index(drop=True)

    agg = (
        df.groupby(["ville_id", "heure_utc"], as_index=False)
        .agg(
            nb_stations_velo=("station_id", "nunique"),
            velos_dispo_moyens=("bikes_available", "mean"),
            taux_utilisation_velo=("taux_utilisation", "mean"),
            taux_occupation_velo=("taux_occupation", "mean"),
            pct_stations_vides=("est_vide", "mean"),
            pct_stations_pleines=("est_pleine", "mean"),
            pct_stations_critiques=("est_critique", "mean"),
        )
    )
    for col in (
        "velos_dispo_moyens", "taux_utilisation_velo", "taux_occupation_velo",
        "pct_stations_vides", "pct_stations_pleines", "pct_stations_critiques",
    ):
        agg[col] = agg[col].round(4)
    return agg


def enrich_gold_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calcule les indicateurs urbains croisés P4 sur la table jointe `(ville_id, heure_utc)`."""
    if df.empty:
        return pd.DataFrame(columns=GOLD_COLS)

    out = df.copy()
    for col in GOLD_COLS:
        if col not in out.columns:
            out[col] = "" if col in ("polluant_dominant", "regime_interaction") else np.nan

    temp = pd.to_numeric(out["temperature"], errors="coerce")
    precip = pd.to_numeric(out["precipitation"], errors="coerce").fillna(0.0)
    wind = pd.to_numeric(out["wind_speed"], errors="coerce").fillna(3.0)
    vis = pd.to_numeric(out["visibility"], errors="coerce").fillna(10000.0)
    oms = pd.to_numeric(out["indice_pollution_oms"], errors="coerce")
    util_velo = pd.to_numeric(out["taux_utilisation_velo"], errors="coerce")
    crit_velo = pd.to_numeric(out["pct_stations_critiques"], errors="coerce").fillna(0.0)

    # 1. Indice de confort météo (0..100)
    temp_score = (45.0 - (temp.fillna(15.0) - 19.0).abs() * 2.2).clip(lower=0.0, upper=45.0)
    rain_score = np.where(precip <= 0.1, 30.0, np.where(precip <= 0.8, 16.0, np.where(precip <= 2.5, 5.0, 0.0)))
    wind_score = (15.0 - (wind - 3.0).clip(lower=0.0) * 1.8).clip(lower=0.0, upper=15.0)
    vis_score = np.where(vis >= 8000.0, 10.0, np.where(vis >= 3000.0, 6.0, np.where(vis >= 1000.0, 2.0, 0.0)))
    confort_meteo = (temp_score + rain_score + wind_score + vis_score).round(1)
    out["indice_confort_meteo"] = confort_meteo.astype("float32")

    # 2. Score de praticabilité mobilité douce (0..100) : 75 % météo + 25 % qualité de l'air
    air_health_score = (25.0 - (oms.fillna(0.7) - 0.5).clip(lower=0.0) * 15.0).clip(lower=0.0, upper=25.0)
    out["score_mobilite_douce"] = (confort_meteo * 0.75 + air_health_score).round(1).astype("float32")

    # 3. Créneau favorable à la mobilité douce (binaire 0/1)
    favorable = (
        temp.between(10.0, 26.0)
        & (precip <= 0.2)
        & (wind <= 6.0)
        & (vis >= 4000.0)
        & (oms.fillna(0.8) <= 1.2)
    )
    out["condition_favorable_velo"] = favorable.fillna(False).astype("uint8")

    # 4. Score d'interaction tripartite météo × mobilité × pollution (0..100)
    stagnation_stress = (
        np.where(wind < 2.2, 20.0, np.where(wind < 3.5, 10.0, 0.0))
        + np.where((temp < 5.0) | (temp >= 26.0), 10.0, 0.0)
        + np.where(precip > 1.0, 10.0, 0.0)
    )
    pollution_stress = (oms.fillna(0.5) * 25.0).clip(lower=0.0, upper=40.0)
    mobility_intensity = (util_velo.fillna(0.35) * 22.0 + crit_velo * 15.0).clip(lower=0.0, upper=30.0)
    interaction = (stagnation_stress + pollution_stress + mobility_intensity).clip(0.0, 100.0).round(1)
    out["score_interaction_urbaine"] = interaction.astype("float32")

    # 5. Qualification métier du régime d'interaction
    regime = np.select(
        [
            (oms.fillna(0.0) >= 1.1) & (wind < 3.0) & (util_velo.fillna(0.0) >= 0.35),
            (temp >= 23.0) & (pd.to_numeric(out["o3"], errors="coerce").fillna(0.0) >= 90.0),
            (precip > 1.0) | (wind >= 8.0),
            (out["condition_favorable_velo"] == 1) & (oms.fillna(0.5) <= 0.8),
        ],
        [
            "exposition_cyclistes_pointe",
            "episode_photochimique_chaleur",
            "intemperies_report_modal",
            "dispersion_favorable",
        ],
        default="normal",
    )
    out["regime_interaction"] = regime
    out["polluant_dominant"] = out["polluant_dominant"].fillna("").astype(str)
    return out[GOLD_COLS].sort_values(["ville_id", "heure_utc"]).reset_index(drop=True)


def join_gold_dataframes(
    meteo_df: pd.DataFrame | None = None,
    velos_df: pd.DataFrame | None = None,
    pollution_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Joint les agrégats horaires Météo (P1), Vélos (P2) et Pollution (P3) sur `(ville_id, heure_utc)`."""
    frames: list[pd.DataFrame] = []

    if meteo_df is not None and not meteo_df.empty:
        m = meteo_df.copy()
        m["heure_utc"] = pd.to_datetime(m["heure_utc"], utc=True, errors="coerce").dt.floor("h")
        m_cols = [
            c for c in [
                "ville_id", "heure_utc", "temperature", "dew_point", "relative_humidity",
                "pressure", "wind_speed", "precipitation", "visibility",
            ]
            if c in m.columns
        ]
        m = m[m_cols].drop_duplicates(subset=["ville_id", "heure_utc"], keep="last")
        frames.append(m)

    if velos_df is not None and not velos_df.empty:
        v = aggregate_velos_ville_heure(velos_df)
        if not v.empty:
            frames.append(v)

    if pollution_df is not None and not pollution_df.empty:
        p = pollution_df.copy()
        if "pollutant" in p.columns and "value" in p.columns:
            p = aggregate_air_ville_heure(p)
        else:
            p["heure_utc"] = pd.to_datetime(p["heure_utc"], utc=True, errors="coerce").dt.floor("h")
            p_cols = [
                c for c in [
                    "ville_id", "heure_utc", "pm25", "pm10", "no2", "o3", "co",
                    "indice_pollution_oms", "polluant_dominant",
                ]
                if c in p.columns
            ]
            p = p[p_cols].drop_duplicates(subset=["ville_id", "heure_utc"], keep="last")
        if not p.empty:
            frames.append(p)

    if not frames:
        return pd.DataFrame(columns=GOLD_COLS)

    merged = frames[0]
    for nxt in frames[1:]:
        merged = pd.merge(merged, nxt, on=["ville_id", "heure_utc"], how="outer")

    merged = merged.dropna(subset=["ville_id", "heure_utc"])
    return enrich_gold_indicators(merged)


RECOMMANDATIONS_REGIMES: dict[str, str] = {
    "exposition_cyclistes_pointe": (
        "Forte fréquentation vélo combinée à un vent calme (< 3 m/s) et un dépassement OMS : "
        "orienter les cyclistes vers des axes secondaires apaisés et activer l'information qualité de l'air."
    ),
    "episode_photochimique_chaleur": (
        "Chaleur (≥ 23 °C) et pic d'ozone (O₃ ≥ 90 µg/m³) : "
        "déconseiller l'effort intense l'après-midi (14h-17h) et renforcer les îlots de fraîcheur."
    ),
    "intemperies_report_modal": (
        "Pluie forte (> 1 mm/h) ou vent soutenu (≥ 8 m/s) : "
        "anticiper la chute de l'usage vélo et le report modal vers les transports en commun."
    ),
    "dispersion_favorable": (
        "Air sain (indice OMS ≤ 0,8) et météo douce : "
        "créneaux optimaux pour promouvoir la mobilité douce et ajuster la disponibilité vélos en centre-ville."
    ),
    "normal": "Conditions urbaines standards : maintenir la régulation courante des flottes et la surveillance IoT.",
}

CRITERES_MOBILITE_DOUCE: dict[str, Any] = {
    "temperature_min_c": 10.0,
    "temperature_max_c": 26.0,
    "precipitation_max_mm_h": 0.2,
    "vent_max_m_s": 6.0,
    "visibilite_min_m": 4000.0,
    "indice_pollution_oms_max": 1.2,
}


def _deduplicate_velos_silver(df: pd.DataFrame) -> pd.DataFrame:
    """Dédoublonne les relevés Silver vélos sur `(ville_id, station_id, timestamp)`."""
    if df.empty:
        return df
    keys = [c for c in ("ville_id", "station_id", "timestamp") if c in df.columns]
    if len(keys) == 3:
        out = df.copy()
        out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
        return (
            out.dropna(subset=["timestamp"])
            .sort_values(keys)
            .drop_duplicates(subset=keys, keep="last")
            .reset_index(drop=True)
        )
    return df


def _build_synthese_urbaine(
    written_keys: list[str],
    question_index: list[dict[str, Any]],
    meteo_hourly_df: pd.DataFrame | None,
    velos_raw_df: pd.DataFrame | None,
    pollution_raw_df: pd.DataFrame | None,
    gold_df: pd.DataFrame | None,
) -> dict[str, Any]:
    """Construit le tableau de bord exécutif Smart City (`gold/synthese_indicateurs_urbains.json`)."""
    ville_noms = {v["ville_id"]: v["nom"] for v in load_ref_ville()}
    gdf = gold_df if (gold_df is not None and not gold_df.empty) else pd.DataFrame()

    def _range(mask: pd.Series) -> dict[str, Any]:
        if gdf.empty or not mask.any():
            return {"debut_utc": None, "fin_utc": None}
        sub_h = gdf.loc[mask, "heure_utc"]
        return {"debut_utc": _json_safe_value(sub_h.min()), "fin_utc": _json_safe_value(sub_h.max())}

    m_meteo = gdf["temperature"].notna() if not gdf.empty else pd.Series(dtype=bool)
    m_velos = gdf["taux_utilisation_velo"].notna() if not gdf.empty else pd.Series(dtype=bool)
    m_air = gdf["indice_pollution_oms"].notna() if not gdf.empty else pd.Series(dtype=bool)
    m_all3 = (m_meteo & m_velos & m_air) if not gdf.empty else pd.Series(dtype=bool)

    n_meteo = int(m_meteo.sum())
    n_velos_h = int(m_velos.sum())
    n_air_h = int(m_air.sum())
    n_all3 = int(m_all3.sum())

    periode_debut = _json_safe_value(gdf["heure_utc"].min()) if not gdf.empty else None
    periode_fin = _json_safe_value(gdf["heure_utc"].max()) if not gdf.empty else None
    p1_range = _range(m_meteo)
    p2_range = _range(m_velos)
    p3_range = _range(m_air)
    all3_range = _range(m_all3)

    indicateurs_par_ville: list[dict[str, Any]] = []
    if not gdf.empty:
        for ville_id, sub in gdf.groupby("ville_id"):
            dom_pol = sub["polluant_dominant"].replace("", np.nan).dropna()
            dom_reg = sub["regime_interaction"].replace("", np.nan).dropna()
            indicateurs_par_ville.append({
                "ville_id": str(ville_id),
                "ville_nom": ville_noms.get(str(ville_id), str(ville_id)),
                "nb_heures_gold": int(len(sub)),
                "temperature_moy_c": _json_safe_value(round(float(sub["temperature"].mean()), 1))
                if sub["temperature"].notna().any()
                else None,
                "taux_utilisation_velo_pct": _json_safe_value(
                    round(float(sub["taux_utilisation_velo"].mean() * 100), 1)
                )
                if sub["taux_utilisation_velo"].notna().any()
                else None,
                "pct_stations_critiques": _json_safe_value(round(float(sub["pct_stations_critiques"].mean() * 100), 1))
                if sub["pct_stations_critiques"].notna().any()
                else None,
                "indice_pollution_oms_moy": _json_safe_value(round(float(sub["indice_pollution_oms"].mean()), 2))
                if sub["indice_pollution_oms"].notna().any()
                else None,
                "polluant_dominant": str(dom_pol.mode().iloc[0]) if not dom_pol.empty else None,
                "score_mobilite_douce_moy": _json_safe_value(round(float(sub["score_mobilite_douce"].mean()), 1)),
                "pct_creneaux_favorables": _json_safe_value(
                    round(float(sub["condition_favorable_velo"].mean() * 100), 1)
                ),
                "score_interaction_max": _json_safe_value(round(float(sub["score_interaction_urbaine"].max()), 1)),
                "regime_dominant": str(dom_reg.mode().iloc[0]) if not dom_reg.empty else "normal",
            })

    kpis_globaux = {
        "nb_villes_actives": int(gdf["ville_id"].nunique()) if not gdf.empty else 0,
        "nb_heures_gold": int(len(gdf)),
        "temperature_moy_france_c": _json_safe_value(round(float(gdf["temperature"].mean()), 1))
        if (not gdf.empty and gdf["temperature"].notna().any())
        else None,
        "taux_utilisation_velo_france_pct": _json_safe_value(
            round(float(gdf["taux_utilisation_velo"].mean() * 100), 1)
        )
        if (not gdf.empty and gdf["taux_utilisation_velo"].notna().any())
        else None,
        "indice_pollution_oms_france": _json_safe_value(round(float(gdf["indice_pollution_oms"].mean()), 2))
        if (not gdf.empty and gdf["indice_pollution_oms"].notna().any())
        else None,
        "score_mobilite_douce_france": _json_safe_value(round(float(gdf["score_mobilite_douce"].mean()), 1))
        if not gdf.empty
        else None,
        "pct_creneaux_favorables_france": _json_safe_value(
            round(float(gdf["condition_favorable_velo"].mean() * 100), 1)
        )
        if not gdf.empty
        else None,
    }

    return {
        "plateforme": "UrbanHub — Jumeau numérique urbain",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "format_couche_gold": "JSON uniquement (logiques métier P1 à P4)",
        "couverture_flux": {
            "periode_debut_utc": periode_debut,
            "periode_fin_utc": periode_fin,
            "p1_meteo_heures": n_meteo,
            "p1_meteo_lignes_silver": int(len(meteo_hourly_df)) if meteo_hourly_df is not None else 0,
            "p1_meteo_periode": p1_range,
            "p2_velos_heures": n_velos_h,
            "p2_velos_releves_silver": int(len(velos_raw_df)) if velos_raw_df is not None else 0,
            "p2_velos_periode": p2_range,
            "p3_pollution_heures": n_air_h,
            "p3_pollution_mesures_silver": int(len(pollution_raw_df)) if pollution_raw_df is not None else 0,
            "p3_pollution_periode": p3_range,
            "croisement_complet_3_flux_heures": n_all3,
            "croisement_3_flux_periode": all3_range,
        },
        "kpis_globaux_france": kpis_globaux,
        "indicateurs_par_ville": indicateurs_par_ville,
        "questions_metier_couvertes": question_index,
        "nb_fichiers_gold_json": len(written_keys) + 1,
        "fichiers_generes": sorted(written_keys + [GOLD_SYNTHESE_KEY]),
    }


def export_gold_business_json(
    storage: Any,
    meteo_hourly_df: pd.DataFrame | None = None,
    velos_raw_df: pd.DataFrame | None = None,
    pollution_raw_df: pd.DataFrame | None = None,
    gold_df: pd.DataFrame | None = None,
) -> list[str]:
    """Écrit exclusivement des fichiers JSON dans `gold/` répondant aux questions métier P1, P2, P3 et P4."""
    from p1_meteo.analytics import (
        anomalous_periods,
        daily_aggregates,
        daily_anomalies,
        extreme_days,
        national_anomaly_series,
        seasonal_temperature,
        visibility_by_class,
        visibility_correlations,
    )
    from p2_velos.analytics import (
        daily_usage_peaks,
        detect_critical_stations,
        detect_undersupplied_zones,
        geographic_imbalance,
        top_utilisation_stations,
    )
    from p3_pollution.analytics import (
        city_pollution_ranking,
        detect_pollution_episodes,
        diurnal_pollution_profile,
        dominant_pollutants_by_city,
        ensure_enriched_pollution_df,
        most_polluted_zones,
    )
    from p4_croise.analytics import (
        favorable_cycling_conditions,
        pollution_by_weather_regime,
        strong_tripartite_interactions,
        weather_bike_usage_impact,
        weather_pollution_correlations,
    )

    written_keys: list[str] = []
    question_index: list[dict[str, Any]] = []

    def _save(key: str, payload: dict[str, Any], register_question: bool = True) -> None:
        storage.put_json(key, payload)
        written_keys.append(key)
        if register_question and "question_id" in payload:
            question_index.append({
                "partie": payload["partie"],
                "question_id": payload["question_id"],
                "question_metier": payload["question_metier"],
                "fichier_gold": key,
                "nb_enregistrements": payload["nb_enregistrements"],
            })

    # --- P1 : Questions métier Météo ---
    if meteo_hourly_df is not None and not meteo_hourly_df.empty:
        daily = daily_aggregates(meteo_hourly_df)
        anom = daily_anomalies(daily)
        period_frames = [
            anomalous_periods(anom, var=v, z_threshold=1.5, min_days=3)
            for v in ("tmoy", "pression_moy", "vent_moy", "precip_jour")
        ]
        non_empty_p = [f for f in period_frames if not f.empty]
        periods = (
            pd.concat(non_empty_p, ignore_index=True)
            .sort_values("z_moyen", key=lambda s: -s.abs())
            .reset_index(drop=True)
            if non_empty_p
            else pd.DataFrame()
        )
        nat_series = national_anomaly_series(anom)
        top_days_anom = (
            anom.dropna(subset=["z_tmoy"])
            .assign(_abs_z=lambda d: d["z_tmoy"].abs())
            .sort_values("_abs_z", ascending=False)
            .drop(columns=["_abs_z"])
            .head(50)
        )
        _save(
            f"{GOLD_PREFIX}/p1_meteo/q1_periodes_anormales.json",
            _make_gold_payload(
                "P1",
                "Q1",
                "Peut-on identifier des périodes météorologiques anormales ?",
                {
                    "episodes_anormaux": _df_to_records(periods, max_rows=200),
                    "jours_anomalies_marquees": _df_to_records(top_days_anom),
                    "serie_nationale_anomalies": _df_to_records(nat_series, max_rows=366),
                },
                len(periods) if not periods.empty else len(top_days_anom),
            ),
        )

        vis_corr = visibility_correlations(meteo_hourly_df)
        vis_cls = {k: _df_to_records(v) for k, v in visibility_by_class(meteo_hourly_df).items()}
        _save(
            f"{GOLD_PREFIX}/p1_meteo/q2_correlation_visibilite.json",
            _make_gold_payload(
                "P1",
                "Q2",
                "Existe-t-il une corrélation entre conditions météorologiques et visibilité ?",
                {"correlations_spearman": _df_to_records(vis_corr), "par_classe": vis_cls},
                len(vis_corr),
            ),
        )

        seas = seasonal_temperature(meteo_hourly_df)
        _save(
            f"{GOLD_PREFIX}/p1_meteo/q3_evolution_saisonniere.json",
            _make_gold_payload(
                "P1",
                "Q3",
                "Quelle est l'évolution saisonnière de la température en France et dans les principales villes ?",
                {
                    "mensuel_france": _df_to_records(seas["mensuel_france"]),
                    "mensuel_ville": _df_to_records(seas["mensuel_ville"]),
                    "climatologie_ville": _df_to_records(seas["climatologie_ville"]),
                    "saisons": _df_to_records(seas["saisons"]),
                    "amplitude_annuelle": _df_to_records(seas["amplitude"]),
                },
                len(seas["climatologie_ville"]),
            ),
        )

        ext = extreme_days(daily)
        ext_by_city = (
            ext.groupby("ville_id", as_index=False)
            .agg(
                nb_jours_extremes=("jour", "count"),
                score_extreme_max=("score_extreme", "max"),
                tmax_max=("tmax", "max"),
                tmin_min=("tmin", "min"),
                vent_max=("vent_max", "max"),
                precip_jour_max=("precip_jour", "max"),
            )
            .sort_values("nb_jours_extremes", ascending=False)
            if not ext.empty
            else pd.DataFrame()
        )
        _save(
            f"{GOLD_PREFIX}/p1_meteo/q4_jours_extremes.json",
            _make_gold_payload(
                "P1",
                "Q4",
                "Quels sont les jours présentant des conditions météorologiques extrêmes ?",
                {
                    "resume_par_ville": _df_to_records(ext_by_city),
                    "jours_extremes": _df_to_records(ext, max_rows=300),
                },
                len(ext),
            ),
        )

    # --- P2 : Questions métier Mobilité Vélos ---
    if velos_raw_df is not None and not velos_raw_df.empty and "station_id" in velos_raw_df.columns:
        city_centers = {v["ville_id"]: (float(v["lat"]), float(v["lon"])) for v in load_ref_ville()}

        top_st = top_utilisation_stations(velos_raw_df, top_n=50)
        top_st_by_city = {
            str(cid): _df_to_records(top_utilisation_stations(sub, top_n=10))
            for cid, sub in velos_raw_df.groupby("ville_id")
        }
        _save(
            f"{GOLD_PREFIX}/p2_velos/q1_utilisation_stations.json",
            _make_gold_payload(
                "P2",
                "Q1",
                "Quelles stations présentent le plus fort taux d'utilisation ?",
                {
                    "top_stations": _df_to_records(top_st),
                    "top_stations_par_ville": top_st_by_city,
                },
                len(top_st),
            ),
        )

        zones_so = detect_undersupplied_zones(velos_raw_df)
        so_by_city = (
            zones_so.groupby("ville_id", as_index=False)
            .agg(
                nb_zones=("nb_stations", "count"),
                nb_zones_critiques=("pct_temps_sous_offre", lambda s: int((s >= 25.0).sum())),
                pct_temps_sous_offre_moy=("pct_temps_sous_offre", lambda s: round(float(s.mean()), 1)),
                pct_temps_vide_moy=("pct_temps_vide", lambda s: round(float(s.mean()), 1)),
            )
            .sort_values("pct_temps_sous_offre_moy", ascending=False)
            if not zones_so.empty
            else pd.DataFrame()
        )
        _save(
            f"{GOLD_PREFIX}/p2_velos/q2_zones_sous_offre.json",
            _make_gold_payload(
                "P2",
                "Q2",
                "Peut-on identifier des zones où l'offre de vélos est insuffisante ?",
                {
                    "synthese_par_ville": _df_to_records(so_by_city),
                    "zones_sous_offre": _df_to_records(zones_so, max_rows=200),
                },
                len(zones_so),
            ),
        )

        peaks = daily_usage_peaks(velos_raw_df)
        detected_peaks: list[dict[str, Any]] = []
        if not peaks.empty:
            for (cid, day_type), sub in peaks.groupby(["ville_id", "type_jour"]):
                top_row = sub.sort_values("taux_utilisation_pct", ascending=False).iloc[0]
                matin = sub[sub["heure_locale"].between(6, 11)]
                soir = sub[sub["heure_locale"].between(16, 21)]
                pic_matin = (
                    int(matin.sort_values("taux_utilisation_pct", ascending=False).iloc[0]["heure_locale"])
                    if not matin.empty
                    else None
                )
                pic_soir = (
                    int(soir.sort_values("taux_utilisation_pct", ascending=False).iloc[0]["heure_locale"])
                    if not soir.empty
                    else None
                )
                detected_peaks.append({
                    "ville_id": str(cid),
                    "type_jour": str(day_type),
                    "heure_pic_global": int(top_row["heure_locale"]),
                    "taux_utilisation_pic_pct": float(top_row["taux_utilisation_pct"]),
                    "heure_pic_matin": pic_matin,
                    "heure_pic_soir": pic_soir,
                })
        _save(
            f"{GOLD_PREFIX}/p2_velos/q3_pics_journaliers.json",
            _make_gold_payload(
                "P2",
                "Q3",
                "Quels sont les pics d'utilisation journaliers du réseau de vélos ?",
                {
                    "pics_detectes": detected_peaks,
                    "profil_horaire": _df_to_records(peaks),
                },
                len(peaks),
            ),
        )

        imbal = geographic_imbalance(velos_raw_df, city_centers=city_centers)
        imbal_summary: list[dict[str, Any]] = []
        if not imbal.empty:
            for cid, sub in imbal.groupby("ville_id"):
                low_s = sub.sort_values("taux_occupation_pct", ascending=True).iloc[0]
                high_s = sub.sort_values("taux_occupation_pct", ascending=False).iloc[0]
                imbal_summary.append({
                    "ville_id": str(cid),
                    "amplitude_occupation_pts": round(
                        float(high_s["taux_occupation_pct"] - low_s["taux_occupation_pct"]), 1
                    ),
                    "secteur_moins_dote": str(low_s["secteur_geo"]),
                    "occupation_min_pct": float(low_s["taux_occupation_pct"]),
                    "secteur_plus_charge": str(high_s["secteur_geo"]),
                    "occupation_max_pct": float(high_s["taux_occupation_pct"]),
                })
        _save(
            f"{GOLD_PREFIX}/p2_velos/q4_desequilibres_geographiques.json",
            _make_gold_payload(
                "P2",
                "Q4",
                "Existe-t-il des déséquilibres géographiques dans la disponibilité des vélos ?",
                {
                    "synthese_desequilibre_par_ville": imbal_summary,
                    "desequilibres_secteurs": _df_to_records(imbal),
                },
                len(imbal),
            ),
        )

        crit = detect_critical_stations(velos_raw_df)
        rebal_plan: list[dict[str, Any]] = []
        if not crit.empty:
            for cid, sub in crit.groupby("ville_id"):
                reappro = sub[sub["action_reequilibrage"] == "REAPPROVISIONNER"]
                deseng = sub[sub["action_reequilibrage"] == "DESENGORGER"]
                rebal_plan.append({
                    "ville_id": str(cid),
                    "nb_stations_critiques": int(len(sub)),
                    "nb_a_reapprovisionner": int(len(reappro)),
                    "nb_a_desengorger": int(len(deseng)),
                    "velos_a_apporter": int(reappro["delta_velos_equilibre"].clip(lower=0).sum()),
                    "velos_a_retirer": int((-deseng["delta_velos_equilibre"]).clip(lower=0).sum()),
                })
        _save(
            f"{GOLD_PREFIX}/p2_velos/q5_stations_critiques_reequilibrage.json",
            _make_gold_payload(
                "P2",
                "Q5",
                "Peut-on détecter des stations critiques nécessitant un rééquilibrage des vélos ?",
                {
                    "plan_reequilibrage_par_ville": rebal_plan,
                    "stations_critiques": _df_to_records(crit, max_rows=300),
                },
                len(crit),
            ),
        )

    # --- P3 : Questions métier Pollution IoT ---
    if pollution_raw_df is not None and not pollution_raw_df.empty and "pollutant" in pollution_raw_df.columns:
        zones_p = most_polluted_zones(pollution_raw_df, top_n=50)
        cities_p = city_pollution_ranking(pollution_raw_df)
        top_by_pol = {
            pol: _df_to_records(most_polluted_zones(pollution_raw_df, pollutant=pol, top_n=5))
            for pol in ("pm25", "pm10", "no2", "o3", "co")
        }
        _save(
            f"{GOLD_PREFIX}/p3_pollution/q1_zones_plus_polluees.json",
            _make_gold_payload(
                "P3",
                "Q1",
                "Quelles zones présentent les niveaux de pollution les plus élevés ?",
                {
                    "classement_villes": _df_to_records(cities_p),
                    "zones_stations": _df_to_records(zones_p),
                    "top_par_polluant": top_by_pol,
                },
                len(zones_p),
            ),
        )

        diurne = diurnal_pollution_profile(pollution_raw_df)
        diurnal_peaks: list[dict[str, Any]] = []
        if not diurne.empty:
            for (cid, pol, pol_lbl, day_type), sub in diurne.groupby(
                ["ville_id", "pollutant", "pollutant_label", "type_jour"]
            ):
                p_max = sub.sort_values("concentration_moyenne", ascending=False).iloc[0]
                p_min = sub.sort_values("concentration_moyenne", ascending=True).iloc[0]
                diurnal_peaks.append({
                    "ville_id": str(cid),
                    "pollutant": str(pol),
                    "pollutant_label": str(pol_lbl),
                    "type_jour": str(day_type),
                    "heure_pic": int(p_max["heure_locale"]),
                    "concentration_pic_ugm3": float(p_max["concentration_moyenne"]),
                    "heure_creuse": int(p_min["heure_locale"]),
                    "concentration_min_ugm3": float(p_min["concentration_moyenne"]),
                    "amplitude_diurne_ugm3": round(
                        float(p_max["concentration_moyenne"] - p_min["concentration_moyenne"]), 2
                    ),
                })
        _save(
            f"{GOLD_PREFIX}/p3_pollution/q2_variation_journaliere.json",
            _make_gold_payload(
                "P3",
                "Q2",
                "Existe-t-il une variation journalière de la pollution ?",
                {
                    "heures_de_pic_par_polluant": diurnal_peaks,
                    "profil_journalier": _df_to_records(diurne),
                },
                len(diurne),
            ),
        )

        dom = dominant_pollutants_by_city(pollution_raw_df)
        _save(
            f"{GOLD_PREFIX}/p3_pollution/q3_polluants_dominants.json",
            _make_gold_payload(
                "P3",
                "Q3",
                "Quels polluants sont les plus dominants dans la ville étudiée ?",
                {
                    "synthese_ville": _df_to_records(dom["synthese_ville"]),
                    "detail_par_polluant": _df_to_records(dom["par_polluant"]),
                },
                len(dom["synthese_ville"]),
            ),
        )

        ep_air = detect_pollution_episodes(pollution_raw_df)
        air_enr = ensure_enriched_pollution_df(pollution_raw_df)
        oms_spikes = (
            air_enr[air_enr["depasse_oms"] == 1]
            .sort_values("ratio_oms", ascending=False)[
                ["ville_id", "location_name", "pollutant_label", "heure_utc", "value", "ratio_oms"]
            ]
            .head(50)
            if not air_enr.empty
            else pd.DataFrame()
        )
        _save(
            f"{GOLD_PREFIX}/p3_pollution/q4_episodes_pollution_anormale.json",
            _make_gold_payload(
                "P3",
                "Q4",
                "Peut-on identifier des épisodes de pollution anormale ?",
                {
                    "episodes_anormaux": _df_to_records(ep_air, max_rows=300),
                    "pics_horaires_depassement_oms": _df_to_records(oms_spikes),
                },
                len(ep_air) if not ep_air.empty else len(oms_spikes),
            ),
        )

    # --- P4 : Questions métier Analyse croisée ---
    if gold_df is not None and not gold_df.empty:
        corrs_mp = weather_pollution_correlations(gold_df)
        regimes_mp = pollution_by_weather_regime(gold_df)
        n_mp = int((gold_df["temperature"].notna() & gold_df["indice_pollution_oms"].notna()).sum())
        top_corr_mp = (
            _df_to_records(corrs_mp.iloc[[corrs_mp["correlation"].abs().argmax()]])[0]
            if not corrs_mp.empty
            else None
        )
        _save(
            f"{GOLD_PREFIX}/p4_croise/q1_meteo_pollution.json",
            _make_gold_payload(
                "P4",
                "Q1",
                "Existe-t-il une relation entre conditions météorologiques et pollution ?",
                {
                    "indicateurs_cles": {
                        "nb_heures_croisees": n_mp,
                        "plus_forte_correlation": top_corr_mp,
                    },
                    "correlations_spearman": _df_to_records(corrs_mp),
                    "par_vent_et_pluie": _df_to_records(regimes_mp["par_vent_pluie"]),
                    "par_temperature": _df_to_records(regimes_mp["par_temperature"]),
                },
                len(corrs_mp),
            ),
        )

        impact_v = weather_bike_usage_impact(gold_df)
        tr_df = impact_v["par_temperature_pluie"]
        corr_v = impact_v["correlations"]
        n_mv = int((gold_df["temperature"].notna() & gold_df["taux_utilisation_velo"].notna()).sum())
        sec_avg = (
            float(tr_df[tr_df["classe_pluie"].str.startswith("1.")]["taux_utilisation_pct"].mean())
            if not tr_df.empty
            else np.nan
        )
        rain_avg = (
            float(tr_df[~tr_df["classe_pluie"].str.startswith("1.")]["taux_utilisation_pct"].mean())
            if not tr_df.empty
            else np.nan
        )
        top_corr_v = (
            _df_to_records(corr_v.iloc[[corr_v["correlation_utilisation_velo"].abs().argmax()]])[0]
            if not corr_v.empty
            else None
        )
        _save(
            f"{GOLD_PREFIX}/p4_croise/q2_meteo_velos.json",
            _make_gold_payload(
                "P4",
                "Q2",
                "Les conditions météo influencent-elles l'utilisation des vélos urbains ?",
                {
                    "indicateurs_cles": {
                        "nb_heures_croisees": n_mv,
                        "utilisation_sec_pct": _json_safe_value(round(sec_avg, 1)) if pd.notna(sec_avg) else None,
                        "utilisation_pluie_pct": _json_safe_value(round(rain_avg, 1)) if pd.notna(rain_avg) else None,
                        "ecart_pluie_pts": _json_safe_value(round(rain_avg - sec_avg, 1))
                        if (pd.notna(sec_avg) and pd.notna(rain_avg))
                        else None,
                        "facteur_meteo_principal": top_corr_v,
                    },
                    "correlations": _df_to_records(corr_v),
                    "par_temperature_et_pluie": _df_to_records(tr_df),
                    "par_ville_et_pluie": _df_to_records(impact_v["par_ville_pluie"]),
                },
                len(tr_df),
            ),
        )

        fav = favorable_cycling_conditions(gold_df)
        hourly_fav = gold_df.copy()
        hourly_fav["heure_locale"] = pd.to_datetime(hourly_fav["heure_utc"], utc=True).dt.tz_convert(
            "Europe/Paris"
        ).dt.hour
        fav_by_hour = (
            hourly_fav.groupby("heure_locale", as_index=False)
            .agg(
                score_mobilite_moyen=("score_mobilite_douce", lambda s: round(float(s.mean()), 1)),
                pct_heures_favorables=("condition_favorable_velo", lambda s: round(float(s.mean() * 100.0), 1)),
                nb_heures=("heure_utc", "count"),
            )
        )
        _save(
            f"{GOLD_PREFIX}/p4_croise/q3_conditions_favorables_mobilite_douce.json",
            _make_gold_payload(
                "P4",
                "Q3",
                "Peut-on identifier des conditions météorologiques favorables à la mobilité douce ?",
                {
                    "criteres_favorables": CRITERES_MOBILITE_DOUCE,
                    "par_ville": _df_to_records(fav["par_ville"]),
                    "par_mois": _df_to_records(fav["par_mois"]),
                    "par_heure_locale": _df_to_records(fav_by_hour),
                },
                len(fav["par_ville"]),
            ),
        )

        inter = strong_tripartite_interactions(gold_df)
        detected_regimes = (
            list(inter["repartition_regimes"]["regime_interaction"].astype(str).unique())
            if not inter["repartition_regimes"].empty
            else []
        )
        recommandations = [
            {
                "regime": reg,
                "recommandation": RECOMMANDATIONS_REGIMES.get(reg, RECOMMANDATIONS_REGIMES["normal"]),
            }
            for reg in detected_regimes
        ]
        _save(
            f"{GOLD_PREFIX}/p4_croise/q4_interactions_fortes.json",
            _make_gold_payload(
                "P4",
                "Q4",
                "Existe-t-il des périodes où mobilité, pollution et météo interagissent fortement ?",
                {
                    "repartition_regimes": _df_to_records(inter["repartition_regimes"]),
                    "episodes_interactions_fortes": _df_to_records(inter["episodes"], max_rows=200),
                    "heures_critiques_top": _df_to_records(inter["heures_critiques"], max_rows=50),
                    "recommandations_decideurs": recommandations,
                },
                len(inter["episodes"]),
            ),
        )

        # Jeu croisé sérialisé en JSON pour le dashboard/notebook (aucun Parquet en couche Gold)
        _save(
            GOLD_JSON_KEY,
            _make_gold_payload(
                "P4",
                "GOLD_TABLE",
                "Indicateurs croisés horaires (ville_id, heure_utc) météo × vélos × pollution",
                _df_to_records(gold_df),
                len(gold_df),
            ),
            register_question=False,
        )

    if written_keys:
        synthese = _build_synthese_urbaine(
            written_keys=written_keys,
            question_index=question_index,
            meteo_hourly_df=meteo_hourly_df,
            velos_raw_df=velos_raw_df,
            pollution_raw_df=pollution_raw_df,
            gold_df=gold_df,
        )
        storage.put_json(GOLD_SYNTHESE_KEY, synthese)
        written_keys.append(GOLD_SYNTHESE_KEY)

    return written_keys


def _load_local_velos_silver(base_dir: Path) -> pd.DataFrame:
    """Charge tous les Parquet silver vélos présents sous `data/silver/velos*`."""
    frames: list[pd.DataFrame] = []
    for sub in ("silver/velos", "silver/velos_historique"):
        folder = base_dir / sub
        if folder.exists():
            for p in sorted(folder.rglob("velo_releve_*.parquet")):
                try:
                    frames.append(pd.read_parquet(p))
                except Exception as exc:  # noqa: BLE001
                    log.warning("Parquet vélo ignoré %s : %s", p, exc)
    return _deduplicate_velos_silver(pd.concat(frames, ignore_index=True)) if frames else pd.DataFrame()


def build_gold(use_local: bool = False, storage: Any = None) -> dict[str, Any]:
    """Lit strictement les fichiers Parquet Silver (`silver/`) et génère uniquement les JSON métier en `gold/`."""
    st = storage if storage is not None else get_storage(use_local=use_local)
    base_dir = Path(getattr(st, "base_dir", get_settings().local_data_dir))

    meteo_df = pd.DataFrame()
    velos_raw_df = pd.DataFrame()
    pollution_raw_df = pd.DataFrame()

    # Lecture stricte depuis les fichiers Parquet de la couche Silver
    if st.exists(METEO_SILVER_KEY):
        meteo_df = aggregate_meteo_ville_heure(st.get_parquet(METEO_SILVER_KEY))

    if use_local or hasattr(st, "base_dir"):
        velos_raw_df = _load_local_velos_silver(base_dir)
    else:
        velo_keys = [k for k in st.list("silver/velos") if "velo_releve_" in k and k.endswith(".parquet")]
        if velo_keys:
            velos_raw_df = _deduplicate_velos_silver(
                pd.concat([st.get_parquet(k) for k in velo_keys], ignore_index=True)
            )

    if st.exists(MESURE_SILVER_KEY):
        pollution_raw_df = st.get_parquet(MESURE_SILVER_KEY)

    gold_df = join_gold_dataframes(meteo_df=meteo_df, velos_df=velos_raw_df, pollution_df=pollution_raw_df)
    gold_json_files = export_gold_business_json(
        storage=st,
        meteo_hourly_df=meteo_df,
        velos_raw_df=velos_raw_df,
        pollution_raw_df=pollution_raw_df,
        gold_df=gold_df,
    )
    log.info("Couche Gold JSON générée : %d fichiers JSON", len(gold_json_files))

    return {
        "meteo_rows": len(meteo_df),
        "velos_rows": len(velos_raw_df),
        "pollution_rows": len(pollution_raw_df),
        "gold_rows": len(gold_df),
        "gold_json_files": gold_json_files,
    }


def load_gold_df(use_local: bool = True, storage: Any = None) -> pd.DataFrame:
    """Charge la table croisée `gold_ville_heure` depuis `gold/p4_croise/gold_ville_heure.json`."""
    st = storage if storage is not None else get_storage(use_local=use_local)
    if st.exists(GOLD_JSON_KEY):
        payload = st.get_json(GOLD_JSON_KEY)
        records = payload.get("resultats", []) if isinstance(payload, dict) else payload
        if records:
            df = pd.DataFrame(records)
            if "heure_utc" in df.columns:
                df["heure_utc"] = pd.to_datetime(df["heure_utc"], utc=True, errors="coerce")
            return df
    return pd.DataFrame(columns=GOLD_COLS)


def main() -> None:
    parser = argparse.ArgumentParser(description="P4 — Génération de la couche Gold (JSON métier P1 à P4)")
    parser.add_argument(
        "--local",
        action="store_true",
        help="Lire Silver local (data/silver/) et écrire Gold JSON (data/gold/)",
    )
    args = parser.parse_args()
    print(build_gold(use_local=args.local))


if __name__ == "__main__":
    main()
