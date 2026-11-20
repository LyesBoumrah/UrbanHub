"""P4 — Analyse croisée météo × pollution × vélos, à partir de silver/croise (ville × heure UTC).

Principe : un « indice » rapporte chaque mesure à ce qui est HABITUEL pour la même ville, le même mois et la même
heure locale (et, pour le vélo, le même type de jour ouvré / week-end). Indice 1 = normal ; 1,3 = 30 % au-dessus.
On isole ainsi l'effet de la météo des cycles journaliers et saisonniers, qui sinon dominent tout
(la nuit : moins de vent, plus de NO2, moins de vélos ; l'hiver : plus de particules, moins de soleil).

Questions du sujet :
  Q1  Météo ↔ pollution (5 ans)          → vent / PM et NO2, température / O3, pluie / PM (lessivage), anticyclone
  Q2  Météo ↔ usage du vélo              → activité selon pluie, température, vent ; jours secs vs pluvieux
  Q3  Conditions favorables au vélo      → score de favorabilité validé par l'activité observée
  Q4  Interactions fortes                → épisodes combinés + modèle explicatif (importance des facteurs)

Responsable : commun (P4).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

MIN_GROUP = 30  # observations minimales pour calculer une normale ville × mois × heure

WIND_BINS = [0, 1, 2, 3, 5, 8, 30]
TEMP_BINS = [-15, 0, 5, 10, 15, 20, 25, 30, 45]
RAIN_BINS = [-0.01, 0.0, 0.5, 2.0, 100]
RAIN_LABELS = ["sec", "bruine (< 0,5 mm)", "pluie modérée (0,5-2 mm)", "forte pluie (> 2 mm)"]


def relative_index(df: pd.DataFrame, col: str, keys=("ville_id", "mois", "heure_locale"),
                   min_group: int = MIN_GROUP) -> pd.Series:
    """valeur / moyenne de la même ville × mois × heure locale (NaN si la normale repose sur trop peu de points)."""
    g = df.groupby(list(keys))[col]
    ref = g.transform("mean").where(g.transform("count") >= min_group)
    return df[col] / ref


def binned(df: pd.DataFrame, x: str, y: str, bins, labels=None) -> pd.DataFrame:
    d = df[[x, y]].dropna()
    cat = pd.cut(d[x], bins, labels=labels)
    return d.groupby(cat, observed=True)[y].agg(moyenne="mean", heures="size").round(3).reset_index()


# ---------------------------------------------------------------- Q1 météo ↔ pollution

def pollution_indices(df: pd.DataFrame) -> pd.DataFrame:
    d = df[df["a_pollution"]].copy()
    for p in ("pm25", "pm10", "no2", "o3"):
        d[f"i_{p}"] = relative_index(d, p)
    return d


def rain_washout(df: pd.DataFrame) -> pd.DataFrame:
    """PM et NO2 un jour sec vs un jour pluvieux (moyennes journalières en indice, par ville)."""
    d = pollution_indices(df)
    day = d.groupby(["ville_id", "date_locale"]).agg(pluie=("precipitation_mm", "sum"), vent=("vent_ms", "mean"),
                                                     i_pm25=("i_pm25", "mean"), i_pm10=("i_pm10", "mean"),
                                                     i_no2=("i_no2", "mean")).reset_index()
    day["type_jour"] = pd.cut(day["pluie"], [-0.01, 0.2, 5, 200], labels=["sec (< 0,2 mm)", "pluvieux (0,2-5 mm)",
                                                                          "très pluvieux (> 5 mm)"])
    return day.groupby("type_jour", observed=True)[["i_pm25", "i_pm10", "i_no2"]].mean().round(3)


def stagnation(df: pd.DataFrame) -> pd.DataFrame:
    """Hiver (déc.-fév.) : indice PM2.5 selon vent faible / fort × pression basse / haute (anticyclone)."""
    d = pollution_indices(df)
    w = d[d["mois"].isin([12, 1, 2])].copy()
    w["vent"] = np.where(w["vent_ms"] < 2, "vent < 2 m/s", "vent ≥ 2 m/s")
    w["pression"] = np.where(w["pression_hpa"] >= 1025, "haute pression (≥ 1025 hPa)", "pression < 1025 hPa")
    return w.pivot_table(index="pression", columns="vent", values="i_pm25", aggfunc="mean").round(2)


def spearman_indices(df: pd.DataFrame) -> pd.DataFrame:
    d = pollution_indices(df)
    meteo = ["temperature_c", "vent_ms", "precipitation_mm", "humidite_pct", "pression_hpa"]
    return d[meteo + ["i_pm25", "i_pm10", "i_no2", "i_o3"]].corr(method="spearman").loc[
        ["i_pm25", "i_pm10", "i_no2", "i_o3"], meteo].round(2)


# ---------------------------------------------------------------- Q2 météo ↔ vélo

VELO_KEYS = ("ville_id", "saison", "heure_locale", "est_weekend")


def velo_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Heures avec vélos, activité exprimée en indice (ville × SAISON × heure locale × jour ouvré / week-end).

    La normale est calculée par saison et non par mois : les vélos ne couvrent que 23 mois, et un groupe
    « mois × week-end » ne compterait que 8 à 17 heures — trop peu, les week-ends seraient perdus.
    """
    d = df[df["a_velos"] & df["a_meteo"]].copy()
    d["i_velo"] = relative_index(d, "activite_par_station", keys=VELO_KEYS, min_group=20)
    return d[d["i_velo"].notna()]


def rain_effect(v: pd.DataFrame, hours=range(7, 21)) -> pd.DataFrame:
    """Effet de la pluie sur l'activité vélo, en journée (7 h-21 h)."""
    d = v[v["heure_locale"].isin(list(hours))]
    return binned(d, "precipitation_mm", "i_velo", RAIN_BINS, RAIN_LABELS)


def dry_vs_rainy_days(v: pd.DataFrame) -> pd.DataFrame:
    day = v.groupby(["ville_id", "date_locale"]).agg(pluie=("precipitation_mm", "sum"), i_velo=("i_velo", "mean"),
                                                     weekend=("est_weekend", "first")).reset_index()
    day["type_jour"] = np.where(day["pluie"] < 0.2, "sec", np.where(day["pluie"] < 5, "pluvieux", "très pluvieux"))
    return day.groupby(["type_jour", "weekend"])["i_velo"].agg(["mean", "size"]).round(3)


def rain_effect_by_city(v: pd.DataFrame) -> pd.DataFrame:
    d = v[v["heure_locale"].between(7, 20)]
    dry = d[d["precipitation_mm"] == 0].groupby("ville_id")["i_velo"].mean()
    wet = d[d["precipitation_mm"] >= 0.5].groupby("ville_id")["i_velo"].mean()
    return pd.DataFrame({"sec": dry, "pluie_0_5mm_plus": wet,
                         "effet_pluie_pct": (100 * (wet / dry - 1))}).round(1).sort_values("effet_pluie_pct")


# ---------------------------------------------------------------- Q3 favorabilité

def favorability(df: pd.DataFrame) -> pd.Series:
    """Score 0-3 : +1 température 10-25 °C, +1 pas de pluie, +1 vent < 5 m/s (≈ 18 km/h)."""
    return ((df["temperature_c"].between(10, 25)).astype(int) + (df["precipitation_mm"] == 0).astype(int)
            + (df["vent_ms"] < 5).astype(int))


def favorability_validation(v: pd.DataFrame) -> pd.DataFrame:
    d = v[v["heure_locale"].between(7, 20)].assign(score=lambda x: favorability(x))
    return d.groupby("score")["i_velo"].agg(indice_activite="mean", heures="size").round(3)


def favorable_share(df: pd.DataFrame) -> pd.DataFrame:
    """Part des heures de journée (7 h-21 h) pleinement favorables (score 3), par ville, sur 5 ans de météo."""
    d = df[df["a_meteo"] & df["heure_locale"].between(7, 20)].assign(score=lambda x: favorability(x))
    out = d.groupby("ville_id")["score"].apply(lambda s: round(100 * (s == 3).mean(), 1))
    return out.rename("pct_heures_favorables").sort_values(ascending=False).to_frame()


# ---------------------------------------------------------------- Q4 interactions

def combined_days(df: pd.DataFrame, v: pd.DataFrame) -> pd.DataFrame:
    """Jours où pollution élevée (PM2.5 > 15 ou O3 élevé), météo défavorable et usage vélo atypique coïncident."""
    day_v = v.groupby(["ville_id", "date_locale"]).agg(i_velo=("i_velo", "mean")).reset_index()
    day = df[df["a_velos"]].groupby(["ville_id", "date_locale"]).agg(
        pm25=("pm25", "mean"), o3_max=("o3", "max"), t_max=("temperature_c", "max"),
        pluie=("precipitation_mm", "sum"), vent=("vent_ms", "mean")).reset_index().merge(day_v, on=["ville_id",
                                                                                                    "date_locale"])
    day["pollution_elevee"] = (day["pm25"] > 15) | (day["o3_max"] > 120)
    day["meteo_defavorable"] = (day["pluie"] >= 5) | (day["t_max"] >= 30) | (day["t_max"] <= 3) | (day["vent"] >= 6)
    day["velo_atypique"] = (day["i_velo"] < 0.75) | (day["i_velo"] > 1.25)
    return day


METEO_POLLUTION = ["temperature_c", "precipitation_mm", "vent_ms", "humidite_pct", "nebulosite_pct",
                   "pm25", "no2", "o3"]


def model_frame(v: pd.DataFrame, target: str = "activite_par_station") -> tuple[pd.DataFrame, pd.Series, list]:
    """target = « activite_par_station » (activité brute : ville, heure, météo, pollution)
    ou « i_velo » (écart à la normale : seulement météo et pollution — ce que le calendrier n'explique pas)."""
    d = v.dropna(subset=[target]).copy()
    if target == "i_velo":
        feats = METEO_POLLUTION
    else:
        d["ville_code"] = d["ville_id"].astype("category").cat.codes
        feats = METEO_POLLUTION + ["heure_locale", "jour_semaine", "mois", "ville_code"]
    return d[feats], d[target], feats


def explain_activity(v: pd.DataFrame, target: str = "activite_par_station", seed: int = 0) -> dict:
    """Modèle (gradient boosting) + importance par permutation, évalué sur une période TEST postérieure
    (les 20 % les plus récents) pour éviter la fuite d'information temporelle."""
    from sklearn.ensemble import HistGradientBoostingRegressor
    from sklearn.inspection import permutation_importance
    from sklearn.metrics import mean_absolute_error, r2_score

    X, y, feats = model_frame(v, target)
    order = v.loc[X.index, "heure_utc"]
    cut = order.quantile(0.8)
    tr, te = order < cut, order >= cut
    m = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.08, random_state=seed)
    m.fit(X[tr], y[tr])
    pred = m.predict(X[te])
    sample = X[te].sample(min(40_000, te.sum()), random_state=seed)
    imp = permutation_importance(m, sample, y.loc[sample.index], n_repeats=5, random_state=seed, n_jobs=1)
    importance = pd.Series(imp.importances_mean, index=feats).sort_values(ascending=False)
    return {"r2": round(r2_score(y[te], pred), 3), "mae": round(mean_absolute_error(y[te], pred), 3),
            "test_depuis": str(cut), "importance": importance.round(4), "model": m}
