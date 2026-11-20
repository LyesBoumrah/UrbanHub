"""P1 — Fonctions d'analyse météo répondant aux 4 questions métier (pandas pur, testables).

Q1  Périodes météorologiques anormales  -> daily_anomalies / anomalous_periods
Q2  Corrélation météo ↔ visibilité      -> visibility_correlations / visibility_by_class
Q3  Évolution saisonnière de la température (France + grandes villes) -> seasonal_temperature
Q4  Jours aux conditions extrêmes       -> extreme_days

Entrée standard : agrégat ville × heure (`silver/meteo/meteo_obs.parquet` agrégé par ville)
avec les colonnes ville_id, heure_utc, temperature, pressure, wind_speed, precipitation, visibility, relative_humidity.

Responsable : P1.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# Seuils « extrêmes » (références Météo-France / vigilance)
SEUIL_CANICULE_TMAX = 35.0       # °C
SEUIL_FORTE_CHALEUR_TMIN = 20.0  # °C (nuit tropicale)
SEUIL_GEL_FORT_TMIN = -5.0       # °C
SEUIL_VENT_FORT = 15.0           # m/s de vent moyen ≈ 54 km/h (rafales bien supérieures)
SEUIL_PLUIE_FORTE = 30.0         # mm/jour
SEUIL_BROUILLARD = 1000.0        # m (définition OMM du brouillard)
SEUIL_DEPRESSION = 980.0         # hPa (dépression creuse / tempête)

SAISONS = {12: "Hiver", 1: "Hiver", 2: "Hiver", 3: "Printemps", 4: "Printemps", 5: "Printemps",
           6: "Été", 7: "Été", 8: "Été", 9: "Automne", 10: "Automne", 11: "Automne"}


def to_local_day(ts: pd.Series, tz: str = "Europe/Paris") -> pd.Series:
    return pd.to_datetime(ts, utc=True).dt.tz_convert(tz).dt.normalize().dt.tz_localize(None)


def daily_aggregates(hourly: pd.DataFrame) -> pd.DataFrame:
    """Agrégats journaliers (jour civil local) par ville."""
    if hourly.empty:
        return pd.DataFrame()
    df = hourly.copy()
    df["jour"] = to_local_day(df["heure_utc"])
    g = df.groupby(["ville_id", "jour"])
    daily = g.agg(
        tmin=("temperature", "min"),
        tmax=("temperature", "max"),
        tmoy=("temperature", "mean"),
        pression_min=("pressure", "min"),
        pression_moy=("pressure", "mean"),
        vent_moy=("wind_speed", "mean"),
        vent_max=("wind_speed", "max"),
        visibilite_min=("visibility", "min"),
        visibilite_moy=("visibility", "mean"),
        humidite_moy=("relative_humidity", "mean"),
        precip_taux_moy=("precipitation", "mean"),
        nb_heures=("temperature", "count"),
    ).reset_index()
    # Taux moyen (mm/h) × 24 h ≈ cumul journalier (les cumuls ISD sont ramenés en mm/h au nettoyage)
    daily["precip_jour"] = (daily.pop("precip_taux_moy") * 24).round(1)
    num_cols = daily.select_dtypes(include="number").columns
    daily[num_cols] = daily[num_cols].round(2)
    return daily


# ---------------------------------------------------------------- Q1 anomalies

def daily_anomalies(daily: pd.DataFrame, window_days: int = 15) -> pd.DataFrame:
    """Anomalies standardisées (z-scores) vs climatologie du même moment de l'année.

    Climatologie : moyenne et écart-type par ville et jour de l'année, lissés par une fenêtre
    circulaire de ±window_days jours (robuste avec seulement 5 années d'historique).
    """
    if daily.empty:
        return daily
    df = daily.copy()
    df["doy"] = pd.to_datetime(df["jour"]).dt.day_of_year.clip(upper=365)
    out = []
    for ville, sub in df.groupby("ville_id"):
        sub = sub.copy()
        for var in ("tmoy", "pression_moy", "vent_moy", "precip_jour"):
            clim = sub.groupby("doy")[var].agg(["sum", "count"]).reindex(range(1, 366), fill_value=0)
            sq = sub.assign(_sq=sub[var] ** 2).groupby("doy")["_sq"].sum().reindex(range(1, 366), fill_value=0)
            k = 2 * window_days + 1
            ext = lambda s: pd.concat([s.iloc[-window_days:], s, s.iloc[:window_days]])  # noqa: E731
            s_sum = ext(clim["sum"]).rolling(k, center=True).sum().iloc[window_days:-window_days]
            s_cnt = ext(clim["count"]).rolling(k, center=True).sum().iloc[window_days:-window_days]
            s_sq = ext(sq).rolling(k, center=True).sum().iloc[window_days:-window_days]
            mean = s_sum / s_cnt
            std = np.sqrt((s_sq / s_cnt - mean ** 2).clip(lower=0))
            mean.index = std.index = range(1, 366)
            sub[f"{var}_clim"] = sub["doy"].map(mean)
            sub[f"z_{var}"] = (sub[var] - sub[f"{var}_clim"]) / sub["doy"].map(std).replace(0, np.nan)
        out.append(sub)
    res = pd.concat(out, ignore_index=True)
    num_cols = res.select_dtypes(include="number").columns
    res[num_cols] = res[num_cols].round(3)
    return res


def anomalous_periods(anom: pd.DataFrame, var: str = "tmoy", z_threshold: float = 1.5,
                      min_days: int = 3) -> pd.DataFrame:
    """Épisodes de ≥ min_days jours consécutifs où |z| dépasse le seuil (vagues de chaleur/froid…)."""
    col = f"z_{var}"
    if anom.empty or col not in anom:
        return pd.DataFrame()
    rows = []
    for ville, sub in anom.sort_values("jour").groupby("ville_id"):
        sub = sub.dropna(subset=[col])
        for sign, label in ((1, "chaud/haut"), (-1, "froid/bas")):
            hit = (sign * sub[col]) >= z_threshold
            run_id = (hit != hit.shift()).cumsum()
            for _, run in sub[hit].groupby(run_id[hit]):
                if len(run) >= min_days:
                    rows.append({
                        "ville_id": ville, "variable": var, "type": label,
                        "debut": run["jour"].min(), "fin": run["jour"].max(), "nb_jours": len(run),
                        "z_moyen": round(run[col].mean(), 2), "z_max": round((sign * run[col]).max() * sign, 2),
                        "valeur_moyenne": round(run[var].mean(), 2),
                        "normale_moyenne": round(run[f"{var}_clim"].mean(), 2),
                    })
    res = pd.DataFrame(rows)
    return res.sort_values(["z_moyen"], key=lambda s: -s.abs()).reset_index(drop=True) if not res.empty else res


def national_anomaly_series(anom: pd.DataFrame) -> pd.DataFrame:
    """Anomalie de température moyenne « France » (moyenne des villes) par jour."""
    if anom.empty:
        return anom
    res = (
        anom.groupby("jour", as_index=False)
        .agg(z_tmoy=("z_tmoy", "mean"), tmoy=("tmoy", "mean"), tmoy_clim=("tmoy_clim", "mean"))
    )
    for col in ("z_tmoy", "tmoy", "tmoy_clim"):
        res[col] = res[col].round(3)
    return res


# ---------------------------------------------------------------- Q2 visibilité

VIS_DRIVERS = ["relative_humidity", "precipitation", "wind_speed", "temperature", "pressure", "dew_point"]


def visibility_correlations(hourly: pd.DataFrame, method: str = "spearman") -> pd.DataFrame:
    """Corrélation (Spearman par défaut, via rangs sans scipy) visibilité ↔ variables."""
    cols = [c for c in VIS_DRIVERS if c in hourly]
    df = hourly[["visibility"] + cols].dropna(subset=["visibility"])
    rows = []
    for c in cols:
        pair = df[["visibility", c]].dropna()
        if len(pair) >= 10:
            if method == "spearman":
                corr_val = pair["visibility"].rank().corr(pair[c].rank(), method="pearson")
            else:
                corr_val = pair["visibility"].corr(pair[c], method=method)
            rows.append({"variable": c, "correlation": round(float(corr_val), 3), "n": len(pair)})
    return pd.DataFrame(rows).sort_values("correlation", key=lambda s: -s.abs()).reset_index(drop=True)


def visibility_by_class(hourly: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Visibilité médiane et fréquence de brouillard par classes d'humidité, de pluie et de vent."""
    df = hourly.dropna(subset=["visibility"]).copy()
    df["brouillard"] = df["visibility"] < SEUIL_BROUILLARD
    classes = {
        "humidite": pd.cut(df["relative_humidity"], [0, 60, 75, 85, 90, 95, 98, 100.1],
                           labels=["<60", "60-75", "75-85", "85-90", "90-95", "95-98", "98-100"]),
        "pluie": pd.cut(df["precipitation"], [-0.01, 0.0, 0.5, 2, 5, 1000],
                        labels=["sec", "0-0,5 mm/h", "0,5-2", "2-5", ">5"]),
        "vent": pd.cut(
            df["wind_speed"],
            [-0.1, 1, 3, 6, 10, 100],
            labels=["calme <1", "1-3", "3-6", "6-10", ">10 m/s"],
        ),
    }
    out = {}
    for name, cls in classes.items():
        out[name] = (df.groupby(cls, observed=True)
                     .agg(visibilite_mediane=("visibility", "median"),
                          pct_brouillard=("brouillard", "mean"), n=("visibility", "size"))
                     .reset_index().rename(columns={"index": name}))
        out[name]["pct_brouillard"] = (out[name]["pct_brouillard"] * 100).round(2)
    return out


# ---------------------------------------------------------------- Q3 saisonnalité

def seasonal_temperature(hourly: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Températures mensuelles par ville et pour la France (moyenne des villes), + saisons par année."""
    df = hourly.dropna(subset=["temperature"]).copy()
    local = pd.to_datetime(df["heure_utc"], utc=True).dt.tz_convert("Europe/Paris")
    df["annee"], df["mois"] = local.dt.year, local.dt.month
    df["saison"] = df["mois"].map(SAISONS)
    df["annee_saison"] = np.where(df["mois"] == 12, df["annee"] + 1, df["annee"])  # hiver = DJF
    monthly_city = df.groupby(["ville_id", "annee", "mois"])["temperature"].mean().round(2).reset_index()
    clim_city = df.groupby(["ville_id", "mois"])["temperature"].agg(["mean", "min", "max"]).round(2).reset_index()
    france_month = monthly_city.groupby(["annee", "mois"])["temperature"].mean().round(2).reset_index()
    seasons = (df.groupby(["ville_id", "annee_saison", "saison"])["temperature"].mean().round(2)
               .reset_index().rename(columns={"annee_saison": "annee"}))
    amplitude = (clim_city.groupby("ville_id")["mean"].agg(lambda s: s.max() - s.min())
                 .round(2).rename("amplitude_annuelle").reset_index())
    return {"mensuel_ville": monthly_city, "climatologie_ville": clim_city,
            "mensuel_france": france_month, "saisons": seasons, "amplitude": amplitude}


# ---------------------------------------------------------------- Q4 extrêmes

def extreme_days(daily: pd.DataFrame, pct: float = 0.99) -> pd.DataFrame:
    """Jours extrêmes : seuils absolus (vigilance) OU dépassement des quantiles locaux (p99 / p1)."""
    if daily.empty:
        return daily
    df = daily.copy()
    q = df.groupby("ville_id").agg(
        q_tmax=("tmax", lambda s: s.quantile(pct)), q_tmin=("tmin", lambda s: s.quantile(1 - pct)),
        q_vent=("vent_max", lambda s: s.quantile(pct)), q_pluie=("precip_jour", lambda s: s.quantile(pct)),
    ).reset_index()
    df = df.merge(q, on="ville_id", how="left")
    crit = {
        "canicule": df["tmax"] >= SEUIL_CANICULE_TMAX,
        "nuit_tropicale": df["tmin"] >= SEUIL_FORTE_CHALEUR_TMIN,
        "gel_fort": df["tmin"] <= SEUIL_GEL_FORT_TMIN,
        "vent_fort": df["vent_max"] >= SEUIL_VENT_FORT,
        "pluie_forte": df["precip_jour"] >= SEUIL_PLUIE_FORTE,
        "brouillard_dense": df["visibilite_min"] < 200,
        "depression": df["pression_min"] < SEUIL_DEPRESSION,
        "chaleur_record_local": df["tmax"] >= df["q_tmax"],
        "froid_record_local": df["tmin"] <= df["q_tmin"],
        "vent_record_local": df["vent_max"] >= df["q_vent"],
        "pluie_record_local": (df["precip_jour"] >= df["q_pluie"]) & (df["precip_jour"] > 5),
    }
    flags = pd.DataFrame({k: v.fillna(False) for k, v in crit.items()})
    df["criteres"] = flags.apply(lambda r: ", ".join(flags.columns[r.values]), axis=1)
    df["score_extreme"] = flags.sum(axis=1)
    out = df[df["score_extreme"] > 0].drop(columns=["q_tmax", "q_tmin", "q_vent", "q_pluie"])
    return out.sort_values(["score_extreme", "jour"], ascending=[False, True]).reset_index(drop=True)
