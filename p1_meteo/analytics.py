"""P1 — Analyses météo à partir du silver (fonctions pures, réutilisées par le notebook et le dashboard).

Questions du sujet (adaptées : la visibilité est absente de l'archive Open-Meteo) :
  Q1  Périodes météorologiques anormales  → anomalie de température / normale saisonnière, z-score, épisodes
  Q2  Corrélations entre variables météo  → matrices Pearson et Spearman (remplace « météo / visibilité »)
  Q3  Évolution saisonnière de la température → moyennes mensuelles par ville, heatmap mois × année
  Q4  Jours extrêmes (chaleur, froid, vent, pluie) → percentiles 1 % / 99 % par ville sur 5 ans

Toutes les agrégations journalières sont faites en DATE LOCALE (Europe/Paris).

Responsable : P1.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

VARS = ["temperature_c", "humidite_pct", "precipitation_mm", "pression_hpa", "nebulosite_pct", "vent_ms"]


def daily(df: pd.DataFrame) -> pd.DataFrame:
    """Silver horaire → 1 ligne par ville × jour local."""
    d = df[df["is_valid"]].assign(date=df["heure_utc"].dt.tz_convert("Europe/Paris").dt.normalize()
                                  .dt.tz_localize(None))
    g = d.groupby(["ville_id", "date"]).agg(
        t_moy=("temperature_c", "mean"), t_min=("temperature_c", "min"), t_max=("temperature_c", "max"),
        pluie_mm=("precipitation_mm", "sum"), vent_max=("vent_ms", "max"), vent_moy=("vent_ms", "mean"),
        humidite=("humidite_pct", "mean"), nebulosite=("nebulosite_pct", "mean"), heures=("temperature_c", "size"))
    return g[g["heures"] >= 20].reset_index()  # jours quasi complets uniquement (bords de période exclus)


# ---------------------------------------------------------------- Q1 périodes anormales

def seasonal_normal(day: pd.DataFrame, window: int = 15) -> pd.DataFrame:
    """Normale par ville × jour de l'année : moyenne des 5 ans, lissée sur ±7 jours (fenêtre circulaire)."""
    d = day.assign(doy=day["date"].dt.dayofyear.clip(upper=365))
    clim = d.groupby(["ville_id", "doy"])["t_moy"].mean().unstack("ville_id").reindex(range(1, 366))
    padded = pd.concat([clim.iloc[-window:], clim, clim.iloc[:window]])
    smooth = padded.rolling(window, center=True, min_periods=1).mean().iloc[window:-window]
    return smooth.stack().rename("t_normale").reset_index()


def anomalies(day: pd.DataFrame) -> pd.DataFrame:
    """Anomalie journalière (°C) et z-score (écart-type de l'anomalie par ville × mois)."""
    d = day.assign(doy=day["date"].dt.dayofyear.clip(upper=365))
    d = d.merge(seasonal_normal(day), on=["ville_id", "doy"], how="left")
    d["anomalie"] = d["t_moy"] - d["t_normale"]
    sd = d.groupby(["ville_id", d["date"].dt.month])["anomalie"].transform("std")
    d["z"] = d["anomalie"] / sd
    return d


def episodes(anom: pd.DataFrame, z_seuil: float = 1.5, min_jours: int = 3) -> pd.DataFrame:
    """Épisodes = au moins `min_jours` jours consécutifs de même signe avec |z| ≥ z_seuil."""
    rows = []
    for ville, g in anom.sort_values("date").groupby("ville_id"):
        sign = np.where(g["z"] >= z_seuil, 1, np.where(g["z"] <= -z_seuil, -1, 0))
        brk = (pd.Series(sign).diff().ne(0) | g["date"].diff().dt.days.ne(1).values).cumsum().values
        for _, run in g.assign(sign=sign, run=brk).groupby("run"):
            if run["sign"].iloc[0] != 0 and len(run) >= min_jours:
                rows.append({"ville_id": ville, "debut": run["date"].min(), "fin": run["date"].max(),
                             "jours": len(run), "type": "chaud" if run["sign"].iloc[0] > 0 else "froid",
                             "anomalie_moy_c": round(run["anomalie"].mean(), 1),
                             "z_max": round(run["z"].abs().max(), 1)})
    return pd.DataFrame(rows).sort_values(["jours", "z_max"], ascending=False).reset_index(drop=True)


def national_episodes(ep: pd.DataFrame, min_villes: int = 6) -> pd.DataFrame:
    """Épisodes « nationaux » : périodes où au moins `min_villes` villes sont simultanément en épisode."""
    days = [(d, r["type"], r["ville_id"]) for _, r in ep.iterrows()
            for d in pd.date_range(r["debut"], r["fin"])]
    if not days:
        return pd.DataFrame()
    x = pd.DataFrame(days, columns=["date", "type", "ville_id"])
    n = x.groupby(["date", "type"])["ville_id"].nunique().reset_index(name="villes")
    n = n[n["villes"] >= min_villes].sort_values("date")
    rows = []
    for typ, g in n.groupby("type"):
        run = g["date"].diff().dt.days.ne(1).cumsum()
        for _, r in g.groupby(run):
            rows.append({"type": typ, "debut": r["date"].min(), "fin": r["date"].max(), "jours": len(r),
                         "villes_max": int(r["villes"].max())})
    return pd.DataFrame(rows).sort_values("jours", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------- Q2 corrélations

def correlations(df: pd.DataFrame) -> dict:
    v = df.loc[df["is_valid"], VARS]
    return {"pearson": v.corr(method="pearson").round(2), "spearman": v.corr(method="spearman").round(2)}


# ---------------------------------------------------------------- Q3 saisonnalité

def monthly(day: pd.DataFrame) -> pd.DataFrame:
    m = day.assign(annee=day["date"].dt.year, mois=day["date"].dt.month)
    return m.groupby(["ville_id", "annee", "mois"]).agg(t_moy=("t_moy", "mean"), pluie_mm=("pluie_mm", "sum"),
                                                        jours=("date", "size")).reset_index()


def study_years(day: pd.DataFrame) -> pd.DataFrame:
    """Moyennes par « année d'étude » complète (novembre → octobre).

    Les années civiles sont incomplètes aux bords (2021 = nov.-déc., 2026 = jan.-oct.) et donc trompeuses ;
    une année d'étude couvre toujours 12 mois, toutes saisons comprises.
    """
    d = day.assign(annee_etude=np.where(day["date"].dt.month >= 11, day["date"].dt.year, day["date"].dt.year - 1))
    d["periode"] = d["annee_etude"].astype(str) + "-" + (d["annee_etude"] + 1).astype(str).str[-2:]
    g = d.groupby(["periode", "ville_id"]).agg(t_moy=("t_moy", "mean"), pluie_mm=("pluie_mm", "sum"),
                                               jours=("date", "size")).reset_index()
    return g[g["jours"] >= 360]


def climatology(day: pd.DataFrame) -> pd.DataFrame:
    """Température moyenne par ville × mois calendaire (5 ans), + amplitude annuelle."""
    c = day.groupby(["ville_id", day["date"].dt.month])["t_moy"].mean().unstack()
    c.columns.name = "mois"
    c["amplitude"] = c.max(axis=1) - c.min(axis=1)
    return c.round(1)


# ---------------------------------------------------------------- Q4 jours extrêmes

EXTREMES = {  # nom : (colonne, sens, percentile)
    "chaleur": ("t_max", "haut", 0.99), "froid": ("t_min", "bas", 0.01),
    "vent": ("vent_max", "haut", 0.99), "pluie": ("pluie_mm", "haut", 0.99),
}


def extreme_days(day: pd.DataFrame) -> pd.DataFrame:
    """Jours au-delà du percentile 99 (ou en deçà du 1) de LEUR ville : seuils relatifs au climat local."""
    rows = []
    for nom, (col, sens, q) in EXTREMES.items():
        seuil = day.groupby("ville_id")[col].transform(lambda s: s.quantile(q))
        mask = day[col] >= seuil if sens == "haut" else day[col] <= seuil
        rows.append(day.loc[mask, ["ville_id", "date", col]].assign(type=nom, valeur=day.loc[mask, col],
                                                                    seuil=seuil[mask]).drop(columns=col))
    out = pd.concat(rows, ignore_index=True)
    out["valeur"] = out["valeur"].round(1)
    out["seuil"] = out["seuil"].round(1)
    return out


def top_extremes(day: pd.DataFrame, n: int = 5) -> dict:
    """Records absolus (toutes villes) par type d'extrême."""
    out = {}
    for nom, (col, sens, _) in EXTREMES.items():
        s = day.sort_values(col, ascending=(sens == "bas")).head(n)
        out[nom] = s[["ville_id", "date", col]].rename(columns={col: "valeur"}).reset_index(drop=True)
    return out
