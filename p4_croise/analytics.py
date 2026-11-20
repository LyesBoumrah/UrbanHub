"""P4 — Fonctions analytiques croisées Météo × Mobilité × Pollution répondant aux 4 questions métier.

Q1  Relation entre conditions météorologiques et pollution
    -> `weather_pollution_correlations(df)` / `pollution_by_weather_regime(df)`
Q2  Influence des conditions météo sur l'utilisation des vélos urbains
    -> `weather_bike_usage_impact(df)`
Q3  Identification des conditions favorables à la mobilité douce
    -> `favorable_cycling_conditions(df)`
Q4  Périodes où mobilité, pollution et météo interagissent fortement
    -> `strong_tripartite_interactions(df)`

Responsable : commun / P4.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from p4_croise.build_gold import enrich_gold_indicators

WEATHER_VARS = ["temperature", "wind_speed", "precipitation", "pressure", "relative_humidity", "visibility"]
POLLUTION_VARS = ["pm25", "pm10", "no2", "o3", "co", "indice_pollution_oms"]
MOBILITY_VARS = ["taux_utilisation_velo", "taux_occupation_velo", "pct_stations_critiques"]


def _rank_corr(s1: pd.Series, s2: pd.Series, method: str = "spearman") -> float:
    """Corrélation de Spearman (via rangs, sans dépendance scipy) ou Pearson."""
    pair = pd.DataFrame({"a": pd.to_numeric(s1, errors="coerce"), "b": pd.to_numeric(s2, errors="coerce")}).dropna()
    if len(pair) < 3 or pair["a"].nunique() < 2 or pair["b"].nunique() < 2:
        return np.nan
    if method == "spearman":
        val = pair["a"].rank().corr(pair["b"].rank(), method="pearson")
    else:
        val = pair["a"].corr(pair["b"], method="pearson")
    return round(float(val), 3) if pd.notna(val) else np.nan


# ---------------------------------------------------------------- Q1 : Météo × Pollution

def weather_pollution_correlations(gold_df: pd.DataFrame, method: str = "spearman") -> pd.DataFrame:
    """Q1 — Matrice de corrélation (format long) entre variables météo et polluants."""
    if gold_df.empty:
        return pd.DataFrame(columns=["meteo_var", "polluant", "correlation", "n"])
    df = enrich_gold_indicators(gold_df)
    w_cols = [c for c in WEATHER_VARS if c in df.columns]
    p_cols = [c for c in POLLUTION_VARS if c in df.columns]

    rows = []
    for w in w_cols:
        for p in p_cols:
            pair = df[[w, p]].dropna()
            if len(pair) >= 3:
                corr = _rank_corr(pair[w], pair[p], method=method)
                if pd.notna(corr):
                    rows.append({"meteo_var": w, "polluant": p, "correlation": corr, "n": len(pair)})
    return pd.DataFrame(rows)


def pollution_by_weather_regime(gold_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Q1 — Niveaux moyens de pollution par régime de vent, de pluie et de température."""
    if gold_df.empty:
        return {"par_vent_pluie": pd.DataFrame(), "par_temperature": pd.DataFrame()}
    df = enrich_gold_indicators(gold_df)

    df["classe_vent"] = pd.cut(
        df["wind_speed"],
        bins=[-0.1, 2.0, 5.0, 8.0, 100.0],
        labels=["1. Calme (<2 m/s)", "2. Faible (2-5 m/s)", "3. Modéré (5-8 m/s)", "4. Fort (>=8 m/s)"],
    )
    df["classe_pluie"] = np.where(df["precipitation"].fillna(0) > 0.2, "Pluie (>0.2 mm/h)", "Temps sec")
    df["classe_temperature"] = pd.cut(
        df["temperature"],
        bins=[-50.0, 5.0, 12.0, 20.0, 26.0, 60.0],
        labels=[
            "1. Froid (<5 °C)",
            "2. Frais (5-12 °C)",
            "3. Doux (12-20 °C)",
            "4. Chaud (20-26 °C)",
            "5. Très chaud (>=26 °C)",
        ],
    )

    by_wind_rain = (
        df.groupby(["classe_vent", "classe_pluie"], observed=True, as_index=False)
        .agg(
            pm25_moyen=("pm25", "mean"),
            pm10_moyen=("pm10", "mean"),
            no2_moyen=("no2", "mean"),
            o3_moyen=("o3", "mean"),
            indice_oms_moyen=("indice_pollution_oms", "mean"),
            nb_heures=("heure_utc", "count"),
        )
    )
    num_wr = by_wind_rain.select_dtypes(include="number").columns
    by_wind_rain[num_wr] = by_wind_rain[num_wr].round(2)

    by_temp = (
        df.groupby("classe_temperature", observed=True, as_index=False)
        .agg(
            pm25_moyen=("pm25", "mean"),
            no2_moyen=("no2", "mean"),
            o3_moyen=("o3", "mean"),
            indice_oms_moyen=("indice_pollution_oms", "mean"),
            nb_heures=("heure_utc", "count"),
        )
    )
    num_t = by_temp.select_dtypes(include="number").columns
    by_temp[num_t] = by_temp[num_t].round(2)

    return {"par_vent_pluie": by_wind_rain, "par_temperature": by_temp}


# ---------------------------------------------------------------- Q2 : Météo × Vélos

def weather_bike_usage_impact(gold_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Q2 — Impact de la température, de la pluie et du vent sur l'utilisation des vélos."""
    empty_res = {
        "par_temperature_pluie": pd.DataFrame(),
        "correlations": pd.DataFrame(),
        "par_ville_pluie": pd.DataFrame(),
    }
    if gold_df.empty:
        return empty_res
    df = enrich_gold_indicators(gold_df).dropna(subset=["taux_utilisation_velo", "temperature"]).copy()
    if df.empty:
        return empty_res

    df["classe_temperature"] = pd.cut(
        df["temperature"],
        bins=[-50.0, 5.0, 12.0, 20.0, 26.0, 60.0],
        labels=[
            "1. Froid (<5 °C)",
            "2. Frais (5-12 °C)",
            "3. Doux (12-20 °C)",
            "4. Chaud agréable (20-26 °C)",
            "5. Très chaud (>=26 °C)",
        ],
    )
    df["classe_pluie"] = np.where(
        df["precipitation"].fillna(0) <= 0.1,
        "1. Sec",
        np.where(df["precipitation"] <= 1.0, "2. Pluie faible (0.1-1 mm/h)", "3. Pluie forte (>1 mm/h)"),
    )

    by_tr = (
        df.groupby(["classe_temperature", "classe_pluie"], observed=True, as_index=False)
        .agg(
            taux_utilisation_pct=("taux_utilisation_velo", lambda s: round(float(s.mean() * 100.0), 2)),
            taux_occupation_pct=("taux_occupation_velo", lambda s: round(float(s.mean() * 100.0), 2)),
            pct_stations_critiques=("pct_stations_critiques", lambda s: round(float(s.mean() * 100.0), 2)),
            nb_heures=("heure_utc", "count"),
        )
    )

    corr_rows = []
    for w in WEATHER_VARS:
        if w in df.columns:
            c_val = _rank_corr(df[w], df["taux_utilisation_velo"], method="spearman")
            if pd.notna(c_val):
                corr_rows.append({"meteo_var": w, "correlation_utilisation_velo": c_val, "n": int(df[w].notna().sum())})

    by_city_rain = (
        df.groupby(["ville_id", "classe_pluie"], as_index=False)
        .agg(
            taux_utilisation_pct=("taux_utilisation_velo", lambda s: round(float(s.mean() * 100.0), 2)),
            nb_heures=("heure_utc", "count"),
        )
    )
    return {
        "par_temperature_pluie": by_tr,
        "correlations": pd.DataFrame(corr_rows),
        "par_ville_pluie": by_city_rain,
    }


# ---------------------------------------------------------------- Q3 : Conditions favorables mobilité douce

def favorable_cycling_conditions(gold_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Q3 — Quantification des créneaux favorables à la mobilité douce par ville et par mois."""
    if gold_df.empty:
        return {"par_ville": pd.DataFrame(), "par_mois": pd.DataFrame()}
    df = enrich_gold_indicators(gold_df).copy()

    local_ts = pd.to_datetime(df["heure_utc"], utc=True).dt.tz_convert("Europe/Paris")
    df["mois"] = local_ts.dt.month

    city_rows = []
    for ville_id, sub in df.groupby("ville_id"):
        fav = sub[sub["condition_favorable_velo"] == 1]
        unfav = sub[sub["condition_favorable_velo"] == 0]
        u_fav = (
            float(fav["taux_utilisation_velo"].mean() * 100.0)
            if not fav.empty and fav["taux_utilisation_velo"].notna().any()
            else np.nan
        )
        u_unfav = (
            float(unfav["taux_utilisation_velo"].mean() * 100.0)
            if not unfav.empty and unfav["taux_utilisation_velo"].notna().any()
            else np.nan
        )
        city_rows.append({
            "ville_id": ville_id,
            "score_mobilite_moyen": round(float(sub["score_mobilite_douce"].mean()), 1),
            "indice_confort_meteo_moyen": round(float(sub["indice_confort_meteo"].mean()), 1),
            "pct_heures_favorables": round(float(sub["condition_favorable_velo"].mean() * 100.0), 1),
            "util_velo_favorable_pct": round(u_fav, 1) if pd.notna(u_fav) else np.nan,
            "util_velo_defavorable_pct": round(u_unfav, 1) if pd.notna(u_unfav) else np.nan,
            "gain_utilisation_pts": round(u_fav - u_unfav, 1) if (pd.notna(u_fav) and pd.notna(u_unfav)) else np.nan,
            "nb_heures": len(sub),
        })
    by_city = pd.DataFrame(city_rows).sort_values("pct_heures_favorables", ascending=False).reset_index(drop=True)

    by_month = (
        df.groupby(["ville_id", "mois"], as_index=False)
        .agg(
            score_mobilite_moyen=("score_mobilite_douce", lambda s: round(float(s.mean()), 1)),
            pct_heures_favorables=("condition_favorable_velo", lambda s: round(float(s.mean() * 100.0), 1)),
            taux_utilisation_pct=(
                "taux_utilisation_velo",
                lambda s: round(float(s.mean() * 100.0), 1) if s.notna().any() else np.nan,
            ),
            nb_heures=("heure_utc", "count"),
        )
    )
    return {"par_ville": by_city, "par_mois": by_month}


# ---------------------------------------------------------------- Q4 : Interactions tripartites fortes

def strong_tripartite_interactions(
    gold_df: pd.DataFrame,
    min_score: float = 55.0,
    min_episode_hours: int = 2,
) -> dict[str, pd.DataFrame]:
    """Q4 — Détecte les heures et épisodes où mobilité, pollution et météo interagissent fortement."""
    if gold_df.empty:
        return {"heures_critiques": pd.DataFrame(), "episodes": pd.DataFrame(), "repartition_regimes": pd.DataFrame()}
    df = enrich_gold_indicators(gold_df).copy()

    # 1. Répartition globale des régimes d'interaction
    regimes = (
        df.groupby("regime_interaction", as_index=False)
        .agg(
            nb_heures=("heure_utc", "count"),
            temperature_moy=("temperature", "mean"),
            vent_moy=("wind_speed", "mean"),
            taux_utilisation_velo_pct=(
                "taux_utilisation_velo",
                lambda s: float(s.mean() * 100.0) if s.notna().any() else np.nan,
            ),
            indice_oms_moy=("indice_pollution_oms", "mean"),
            score_interaction_moy=("score_interaction_urbaine", "mean"),
        )
    )
    regimes["pct_heures"] = (regimes["nb_heures"] / len(df) * 100.0).round(1)
    num_r = regimes.select_dtypes(include="number").columns
    regimes[num_r] = regimes[num_r].round(2)
    regimes = regimes.sort_values("score_interaction_moy", ascending=False).reset_index(drop=True)

    # 2. Heures dépassant le seuil d'interaction forte
    crit = df[df["score_interaction_urbaine"] >= min_score].sort_values(
        ["score_interaction_urbaine", "heure_utc"], ascending=[False, False]
    ).copy()

    # 3. Regroupement en épisodes continus par ville
    episodes_rows = []
    for ville_id, sub in df[df["score_interaction_urbaine"] >= min_score].sort_values("heure_utc").groupby("ville_id"):
        gap_h = sub["heure_utc"].diff().dt.total_seconds().div(3600.0).fillna(0)
        sub = sub.assign(_ep=(gap_h > 2.0).cumsum())
        for _, ep in sub.groupby("_ep"):
            duree = int(round((ep["heure_utc"].max() - ep["heure_utc"].min()).total_seconds() / 3600.0)) + 1
            if len(ep) < min_episode_hours and duree < min_episode_hours:
                continue
            dom_regime = (
                ep["regime_interaction"].mode().iloc[0]
                if not ep["regime_interaction"].mode().empty
                else "normal"
            )
            temp_m = round(float(ep["temperature"].mean()), 1) if ep["temperature"].notna().any() else np.nan
            episodes_rows.append({
                "ville_id": ville_id,
                "debut_utc": ep["heure_utc"].min(),
                "fin_utc": ep["heure_utc"].max(),
                "duree_heures": duree,
                "regime_dominant": dom_regime,
                "score_interaction_max": round(float(ep["score_interaction_urbaine"].max()), 1),
                "score_interaction_moyen": round(float(ep["score_interaction_urbaine"].mean()), 1),
                "temperature_moy": temp_m,
                "vent_moy": round(float(ep["wind_speed"].mean()), 1) if ep["wind_speed"].notna().any() else np.nan,
                "taux_utilisation_velo_pct": round(float(ep["taux_utilisation_velo"].mean() * 100.0), 1)
                if ep["taux_utilisation_velo"].notna().any()
                else np.nan,
                "indice_oms_max": round(float(ep["indice_pollution_oms"].max()), 2)
                if ep["indice_pollution_oms"].notna().any()
                else np.nan,
                "polluant_dominant": ep["polluant_dominant"].mode().iloc[0]
                if ep["polluant_dominant"].replace("", np.nan).dropna().any()
                else "",
            })

    ep_df = pd.DataFrame(episodes_rows)
    if not ep_df.empty:
        ep_df = ep_df.sort_values(["score_interaction_max", "duree_heures"], ascending=False).reset_index(drop=True)

    return {
        "heures_critiques": crit.head(200).reset_index(drop=True),
        "episodes": ep_df,
        "repartition_regimes": regimes,
    }
