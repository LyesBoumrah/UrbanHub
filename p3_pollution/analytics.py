"""P3 — Analyses de la pollution de l'air à partir du silver (3,46 M mesures station × polluant × heure).

Questions du sujet :
  Q1  Zones les plus polluées                    → moyenne et percentile 95 par station et par ville
  Q2  Variation journalière de la pollution      → profil horaire (heure locale), semaine vs week-end
  Q3  Polluants dominants par ville              → rapport aux valeurs guides de l'OMS (2021)
  Q4  Épisodes de pollution anormaux             → jours de dépassement de la valeur guide 24 h, épisodes nationaux

Référence : lignes directrices OMS 2021 sur la qualité de l'air (µg/m³).
Seules les mesures `is_valid` sont utilisées ; les jours sont en DATE LOCALE et ne sont retenus que
s'ils comptent au moins 18 heures de mesure (règle usuelle de complétude de 75 %).

Responsable : P3.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

POLLUTANTS = ["pm25", "pm10", "no2", "o3", "co"]
LABELS = {"pm25": "PM2.5", "pm10": "PM10", "no2": "NO2", "o3": "O3", "co": "CO"}

# OMS 2021 — valeurs guides
WHO_ANNUAL = {"pm25": 5.0, "pm10": 15.0, "no2": 10.0}
WHO_DAILY = {"pm25": 15.0, "pm10": 45.0, "no2": 25.0, "co": 4000.0}   # moyenne 24 h
WHO_O3_8H = 100.0                                                     # max journalier de la moyenne glissante 8 h
MIN_HOURS_DAY = 18

TRAFFIC_WORDS = r"proxi|trafic|périph|peripher|boulevard|bld|a7|a35|autoroute|quai|rocade"


def valid(df: pd.DataFrame) -> pd.DataFrame:
    d = df[df["is_valid"]].copy()
    local = d["heure_utc"].dt.tz_convert("Europe/Paris")
    d["date"] = local.dt.normalize().dt.tz_localize(None)
    d["h"] = local.dt.hour
    d["wd"] = local.dt.dayofweek
    return d


def station_type(names: pd.Series) -> pd.Series:
    """« trafic » si le nom évoque un axe routier, sinon « fond urbain » (approximation par le nom)."""
    return np.where(names.str.contains(TRAFFIC_WORDS, case=False, regex=True), "trafic", "fond urbain")


# ---------------------------------------------------------------- Q1 zones les plus polluées

def station_stats(d: pd.DataFrame, stations: pd.DataFrame, min_hours: int = 2000) -> pd.DataFrame:
    g = d.groupby(["ville_id", "station_id", "polluant"])["valeur"].agg(
        moyenne="mean", p95=lambda x: x.quantile(0.95), heures="size").reset_index()
    g = g[g["heures"] >= min_hours]
    g = g.merge(stations[["station_id", "station_nom", "latitude", "longitude"]], on="station_id", how="left")
    g["type"] = station_type(g["station_nom"].fillna(""))
    return g.round({"moyenne": 1, "p95": 1})


def city_means(st: pd.DataFrame) -> pd.DataFrame:
    """Moyenne des stations de la ville, par polluant (chaque station pèse autant)."""
    return st.pivot_table(index="ville_id", columns="polluant", values="moyenne", aggfunc="mean").round(1)


# ---------------------------------------------------------------- Q2 variation journalière

def hourly_profile(d: pd.DataFrame, weekend: bool | None = None) -> pd.DataFrame:
    x = d if weekend is None else d[(d["wd"] >= 5) == weekend]
    return x.groupby(["polluant", "h"])["valeur"].mean().unstack("polluant")


def monthly_profile(d: pd.DataFrame) -> pd.DataFrame:
    return d.groupby(["polluant", d["date"].dt.month])["valeur"].mean().unstack("polluant")


# ---------------------------------------------------------------- jours

def daily(d: pd.DataFrame) -> pd.DataFrame:
    """Moyenne journalière par station × polluant (jours complets à ≥ 18 h) + O3 : max 8 h glissantes."""
    g = d.groupby(["ville_id", "station_id", "polluant", "date"])["valeur"].agg(["mean", "size"]).reset_index()
    g = g[g["size"] >= MIN_HOURS_DAY].rename(columns={"mean": "valeur"}).drop(columns="size")
    o3 = d[d["polluant"] == "o3"].sort_values(["station_id", "heure_utc"])
    if len(o3):
        roll = (o3.set_index("heure_utc").groupby("station_id")["valeur"]
                  .rolling("8h", min_periods=6).mean().reset_index(name="o3_8h"))
        roll["date"] = roll["heure_utc"].dt.tz_convert("Europe/Paris").dt.normalize().dt.tz_localize(None)
        mx = roll.groupby(["station_id", "date"])["o3_8h"].max().reset_index()
        g = g.merge(mx.assign(polluant="o3"), on=["station_id", "polluant", "date"], how="left")
    else:
        g["o3_8h"] = np.nan
    return g


def city_daily(day: pd.DataFrame) -> pd.DataFrame:
    """Par ville × polluant × jour : moyenne des stations et max des stations (pour l'O3 : max 8 h)."""
    return day.groupby(["ville_id", "polluant", "date"]).agg(
        moyenne=("valeur", "mean"), max_stations=("valeur", "max"), o3_8h=("o3_8h", "max"),
        stations=("station_id", "nunique")).reset_index()


# ---------------------------------------------------------------- Q3 OMS

def who_ratio(st: pd.DataFrame) -> pd.DataFrame:
    """Moyenne de la ville / valeur guide annuelle OMS (1 = au niveau de la valeur guide)."""
    cm = city_means(st)
    return pd.DataFrame({p: (cm[p] / v).round(2) for p, v in WHO_ANNUAL.items() if p in cm})


def exceedance_days(cday: pd.DataFrame) -> pd.DataFrame:
    """% des jours où AU MOINS UNE station de la ville dépasse la valeur guide journalière OMS."""
    rows = []
    for (v, p), g in cday.groupby(["ville_id", "polluant"]):
        if p == "o3":
            ok = g["o3_8h"].notna()
            n, k = ok.sum(), (g.loc[ok, "o3_8h"] > WHO_O3_8H).sum()
        elif p in WHO_DAILY:
            n, k = len(g), (g["max_stations"] > WHO_DAILY[p]).sum()
        else:
            continue
        rows.append({"ville_id": v, "polluant": p, "jours": int(n), "jours_depassement": int(k),
                     "pct_jours": round(100 * k / n, 1) if n else np.nan})
    return pd.DataFrame(rows)


def dominant_pollutant(ratio: pd.DataFrame) -> pd.Series:
    """Polluant le plus au-dessus de sa valeur guide annuelle, par ville."""
    return ratio.idxmax(axis=1).map(LABELS)


# ---------------------------------------------------------------- Q4 épisodes

def episodes(cday: pd.DataFrame, polluant: str, min_villes: int = 4) -> pd.DataFrame:
    """Jours où au moins `min_villes` villes dépassent la valeur guide 24 h ; regroupés en épisodes consécutifs."""
    g = cday[cday["polluant"] == polluant]
    if polluant == "o3":
        over = g[g["o3_8h"] > WHO_O3_8H]
    else:
        over = g[g["moyenne"] > WHO_DAILY[polluant]]
    n = over.groupby("date").agg(villes=("ville_id", "nunique"), max_moyenne=("moyenne", "max")).reset_index()
    n = n[n["villes"] >= min_villes].sort_values("date")
    if n.empty:
        return pd.DataFrame(columns=["debut", "fin", "jours", "villes_max", "pic_moyenne"])
    run = n["date"].diff().dt.days.ne(1).cumsum()
    out = n.groupby(run).agg(debut=("date", "min"), fin=("date", "max"), jours=("date", "size"),
                             villes_max=("villes", "max"), pic_moyenne=("max_moyenne", "max"))
    return out.sort_values(["villes_max", "jours"], ascending=False).reset_index(drop=True).round(1)


def anomalies(cday: pd.DataFrame, polluant: str, window: int = 30, z: float = 3.0) -> pd.DataFrame:
    """Pics anormaux : moyenne journalière de la ville > moyenne glissante 30 j + z × écart-type glissant."""
    g = cday[cday["polluant"] == polluant].sort_values("date").copy()
    roll = g.groupby("ville_id")["moyenne"]
    g["ref"] = roll.transform(lambda s: s.shift(1).rolling(window, min_periods=20).mean())
    g["sd"] = roll.transform(lambda s: s.shift(1).rolling(window, min_periods=20).std())
    g["z"] = (g["moyenne"] - g["ref"]) / g["sd"]
    return g[g["z"] > z][["ville_id", "date", "moyenne", "ref", "z"]].round(1).sort_values("z", ascending=False)
