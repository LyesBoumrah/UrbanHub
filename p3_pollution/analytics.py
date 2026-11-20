"""P3 — Fonctions analytiques répondant aux 4 questions métier sur la pollution urbaine (pandas pur).

Q1  Quelles zones présentent les niveaux de pollution les plus élevés ?
    -> `most_polluted_zones(df)` / `city_pollution_ranking(df)`
Q2  Existe-t-il une variation journalière de la pollution ?
    -> `diurnal_pollution_profile(df)`
Q3  Quels polluants sont les plus dominants dans la ville étudiée ?
    -> `dominant_pollutants_by_city(df)`
Q4  Peut-on identifier des épisodes de pollution anormale ?
    -> `detect_pollution_episodes(df)`

Responsable : P3.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from p3_pollution.quality import (
    EPISODE_THRESHOLDS_UGM3,
    WHO_THRESHOLDS_UGM3,
    normalize_pollutant,
    pollutant_label,
)


def ensure_enriched_pollution_df(df: pd.DataFrame) -> pd.DataFrame:
    """Garantit la présence des colonnes enrichies (pollutant_label, heure_utc, ratio_oms, depasse_oms...)."""
    if df.empty:
        return df.copy()
    out = df.copy()
    out["pollutant"] = out["pollutant"].map(lambda p: normalize_pollutant(p) or str(p))
    if "pollutant_label" not in out:
        out["pollutant_label"] = out["pollutant"].map(pollutant_label)
    if "timestamp" in out:
        out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    if "heure_utc" not in out and "timestamp" in out:
        out["heure_utc"] = out["timestamp"].dt.floor("h")
    else:
        out["heure_utc"] = pd.to_datetime(out["heure_utc"], utc=True, errors="coerce")
    if "ratio_oms" not in out:
        thresh = out["pollutant"].map(WHO_THRESHOLDS_UGM3)
        out["ratio_oms"] = (out["value"] / thresh).round(4)
    if "depasse_oms" not in out:
        thresh = out["pollutant"].map(WHO_THRESHOLDS_UGM3)
        out["depasse_oms"] = (out["value"] > thresh).astype("int8")
    if "depasse_episode" not in out:
        ep = out["pollutant"].map(EPISODE_THRESHOLDS_UGM3)
        out["depasse_episode"] = (out["value"] >= ep).astype("int8")
    if "location_name" not in out:
        out["location_name"] = "Site " + out.get("location_id", out["sensor_id"]).astype(str)
    if "ville_id" not in out:
        out["ville_id"] = "france"
    return out


# ---------------------------------------------------------------- Q1 : Zones les plus polluées

def most_polluted_zones(df: pd.DataFrame, pollutant: str | None = None, top_n: int = 30) -> pd.DataFrame:
    """Q1 — Classement des zones / stations de mesure par niveau de pollution.

    Si `pollutant` est spécifié, classe selon la concentration moyenne (µg/m³) et le P90 de ce polluant.
    Sinon, classe selon l'indice de dépassement OMS moyen (`ratio_oms`, où 1,0 = seuil OMS 2021).
    """
    if df.empty:
        return pd.DataFrame()
    work = ensure_enriched_pollution_df(df)
    if pollutant:
        pol = normalize_pollutant(pollutant) or pollutant
        work = work[work["pollutant"] == pol]
    if work.empty:
        return pd.DataFrame()

    grouped = (
        work.groupby(["ville_id", "location_id", "location_name", "pollutant", "pollutant_label"], as_index=False)
        .agg(
            latitude=("latitude", "first"),
            longitude=("longitude", "first"),
            concentration_moyenne=("value", "mean"),
            concentration_mediane=("value", "median"),
            concentration_p90=("value", lambda s: float(np.nanpercentile(s, 90))),
            concentration_max=("value", "max"),
            ratio_oms_moyen=("ratio_oms", "mean"),
            pct_depassement_oms=("depasse_oms", lambda s: float(s.mean() * 100.0)),
            pct_episodes=("depasse_episode", lambda s: float(s.mean() * 100.0)),
            nb_mesures=("value", "count"),
        )
    )
    num_cols = grouped.select_dtypes(include="number").columns
    grouped[num_cols] = grouped[num_cols].round(2)
    sort_col = "concentration_moyenne" if pollutant else "ratio_oms_moyen"
    return grouped.sort_values([sort_col, "pct_depassement_oms"], ascending=False).head(top_n).reset_index(drop=True)


def city_pollution_ranking(df: pd.DataFrame) -> pd.DataFrame:
    """Q1 (vue macro) — Classement des villes par indice OMS moyen et taux de dépassement."""
    if df.empty:
        return pd.DataFrame()
    work = ensure_enriched_pollution_df(df)
    by_city = (
        work.groupby("ville_id", as_index=False)
        .agg(
            nb_sites=("location_id", "nunique"),
            nb_capteurs=("sensor_id", "nunique"),
            ratio_oms_moyen=("ratio_oms", "mean"),
            ratio_oms_p90=("ratio_oms", lambda s: float(np.nanpercentile(s.dropna(), 90)) if s.notna().any() else 0.0),
            pct_depassement_oms=("depasse_oms", lambda s: float(s.mean() * 100.0)),
            pct_episodes=("depasse_episode", lambda s: float(s.mean() * 100.0)),
            nb_mesures=("value", "count"),
        )
    )
    num_cols = by_city.select_dtypes(include="number").columns
    by_city[num_cols] = by_city[num_cols].round(2)
    return by_city.sort_values("ratio_oms_moyen", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------- Q2 : Variation journalière

def diurnal_pollution_profile(df: pd.DataFrame, tz: str = "Europe/Paris") -> pd.DataFrame:
    """Q2 — Profil horaire journalier (0h–23h en heure locale) par polluant et type de jour (Semaine / Week-end)."""
    if df.empty:
        return pd.DataFrame()
    work = ensure_enriched_pollution_df(df)
    local_ts = work["timestamp"].dt.tz_convert(tz)
    work["heure_locale"] = local_ts.dt.hour
    work["type_jour"] = np.where(local_ts.dt.day_of_week >= 5, "Week-end", "Semaine")

    prof = (
        work.groupby(["ville_id", "pollutant", "pollutant_label", "type_jour", "heure_locale"], as_index=False)
        .agg(
            concentration_moyenne=("value", "mean"),
            concentration_mediane=("value", "median"),
            concentration_p90=("value", lambda s: float(np.nanpercentile(s, 90))),
            ratio_oms_moyen=("ratio_oms", "mean"),
            nb_mesures=("value", "count"),
        )
    )
    num_cols = prof.select_dtypes(include="number").columns
    prof[num_cols] = prof[num_cols].round(2)
    return prof.sort_values(["ville_id", "pollutant", "type_jour", "heure_locale"]).reset_index(drop=True)


# ---------------------------------------------------------------- Q3 : Polluants dominants

def dominant_pollutants_by_city(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Q3 — Polluants dominants par ville selon deux approches complémentaires :
    1) `par_polluant` : statistiques de chaque polluant par ville (moyenne µg/m³, ratio OMS moyen,
       fréquence où ce polluant est le plus critique de l'heure dans la ville).
    2) `synthese_ville` : le polluant n°1 de chaque ville en dangerosité relative (ratio OMS) et en masse brute.
    """
    if df.empty:
        return {"par_polluant": pd.DataFrame(), "synthese_ville": pd.DataFrame()}
    work = ensure_enriched_pollution_df(df)

    # Polluant ayant le ratio OMS le plus élevé pour chaque (ville_id, heure_utc)
    hourly_pol = (
        work.groupby(["ville_id", "heure_utc", "pollutant", "pollutant_label"], as_index=False)
        .agg(value=("value", "mean"), ratio_oms=("ratio_oms", "mean"))
    )
    idx_max = hourly_pol.groupby(["ville_id", "heure_utc"])["ratio_oms"].idxmax().dropna()
    winners = hourly_pol.loc[idx_max]
    win_counts = (
        winners.groupby(["ville_id", "pollutant"])
        .size()
        .rename("heures_dominant")
        .reset_index()
    )
    total_hours = winners.groupby("ville_id").size().rename("total_heures").reset_index()

    stats = (
        work.groupby(["ville_id", "pollutant", "pollutant_label"], as_index=False)
        .agg(
            concentration_moyenne=("value", "mean"),
            concentration_p90=("value", lambda s: float(np.nanpercentile(s, 90))),
            seuil_oms=("pollutant", lambda s: WHO_THRESHOLDS_UGM3.get(s.iloc[0], np.nan)),
            ratio_oms_moyen=("ratio_oms", "mean"),
            pct_depassement_oms=("depasse_oms", lambda s: float(s.mean() * 100.0)),
            nb_mesures=("value", "count"),
        )
    )
    stats = (
        stats.merge(win_counts, on=["ville_id", "pollutant"], how="left")
        .merge(total_hours, on="ville_id", how="left")
    )
    stats["heures_dominant"] = stats["heures_dominant"].fillna(0).astype(int)
    stats["pct_heures_dominant"] = np.where(
        stats["total_heures"] > 0,
        (stats["heures_dominant"] / stats["total_heures"] * 100.0).round(1),
        0.0,
    )
    stats = stats.drop(columns=["total_heures"])
    num_cols = stats.select_dtypes(include="number").columns
    stats[num_cols] = stats[num_cols].round(2)
    stats = (
        stats.sort_values(
            ["ville_id", "pct_heures_dominant", "ratio_oms_moyen"],
            ascending=[True, False, False],
        )
        .reset_index(drop=True)
    )

    # Synthèse 1 ligne / ville
    synth_rows = []
    for ville_id, sub in stats.groupby("ville_id"):
        top_oms = sub.sort_values(["pct_heures_dominant", "ratio_oms_moyen"], ascending=False).iloc[0]
        # Hors CO (qui se chiffre en centaines de µg/m³), quel est le polluant majeur en concentration ?
        sub_no_co = sub[sub["pollutant"] != "co"]
        cand = sub_no_co if not sub_no_co.empty else sub
        top_mass = cand.sort_values("concentration_moyenne", ascending=False).iloc[0]
        synth_rows.append({
            "ville_id": ville_id,
            "polluant_dominant_oms": top_oms["pollutant_label"],
            "pct_heures_dominant": top_oms["pct_heures_dominant"],
            "ratio_oms_dominant": top_oms["ratio_oms_moyen"],
            "polluant_dominant_masse_hors_co": top_mass["pollutant_label"],
            "concentration_moyenne_ugm3": top_mass["concentration_moyenne"],
        })
    return {"par_polluant": stats, "synthese_ville": pd.DataFrame(synth_rows)}


# ---------------------------------------------------------------- Q4 : Épisodes de pollution anormale

def detect_pollution_episodes(
    df: pd.DataFrame,
    z_threshold: float = 2.0,
    min_consecutive_hours: int = 3,
) -> pd.DataFrame:
    """Q4 — Détecte les épisodes de pollution anormale par `(ville_id, pollutant)`.

    Une heure est qualifiée d'anormale si :
      - sa concentration dépasse le seuil d'épisode ATMO (`EPISODE_THRESHOLDS_UGM3`), OU
      - son z-score local (par ville × polluant) dépasse `z_threshold` ET le seuil OMS est franchi.
    Les heures consécutives (écart <= 2 h) sont fusionnées en épisodes continus d'au moins
    `min_consecutive_hours` heures.
    """
    if df.empty:
        return pd.DataFrame()
    work = ensure_enriched_pollution_df(df)

    hourly = (
        work.groupby(["ville_id", "pollutant", "pollutant_label", "heure_utc"], as_index=False)
        .agg(
            concentration_moyenne=("value", "mean"),
            concentration_max=("value", "max"),
            ratio_oms=("ratio_oms", "mean"),
            depasse_oms=("depasse_oms", "max"),
            depasse_episode=("depasse_episode", "max"),
        )
        .sort_values(["ville_id", "pollutant", "heure_utc"])
    )

    episodes: list[dict[str, object]] = []
    for (ville_id, pol, pol_label), sub in hourly.groupby(["ville_id", "pollutant", "pollutant_label"]):
        sub = sub.sort_values("heure_utc").copy()
        mean_v = float(sub["concentration_moyenne"].mean())
        std_v = float(sub["concentration_moyenne"].std(ddof=0))
        sub["z_score"] = (sub["concentration_moyenne"] - mean_v) / std_v if std_v > 0 else 0.0

        is_anom = (sub["depasse_episode"] == 1) | ((sub["z_score"] >= z_threshold) & (sub["depasse_oms"] == 1))
        anom = sub[is_anom].copy()
        if anom.empty:
            continue

        gap_hours = anom["heure_utc"].diff().dt.total_seconds().div(3600.0).fillna(0)
        anom["episode_id"] = (gap_hours > 2.0).cumsum()

        for _, ep in anom.groupby("episode_id"):
            duree_h = int(round((ep["heure_utc"].max() - ep["heure_utc"].min()).total_seconds() / 3600.0)) + 1
            if len(ep) < min_consecutive_hours and duree_h < min_consecutive_hours:
                continue
            episodes.append({
                "ville_id": ville_id,
                "pollutant": pol,
                "pollutant_label": pol_label,
                "debut_utc": ep["heure_utc"].min(),
                "fin_utc": ep["heure_utc"].max(),
                "duree_heures": duree_h,
                "nb_heures_mesurees": len(ep),
                "concentration_moyenne": round(float(ep["concentration_moyenne"].mean()), 2),
                "concentration_pic": round(float(ep["concentration_max"].max()), 2),
                "ratio_oms_pic": round(float(ep["ratio_oms"].max()), 2),
                "z_score_pic": round(float(ep["z_score"].max()), 2),
                "severite": "ALERTE" if float(ep["ratio_oms"].max()) >= 2.0 else "MODERE",
            })

    res = pd.DataFrame(episodes)
    if res.empty:
        return res
    return res.sort_values(["ratio_oms_pic", "duree_heures"], ascending=False).reset_index(drop=True)
