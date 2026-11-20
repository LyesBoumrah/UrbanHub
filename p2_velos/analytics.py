"""P2 — Analyses vélos à partir du silver (48,6 M lignes station × heure).

Le silver est trop volumineux pour être chargé d'un bloc : chaque fichier (ville × mois) est réduit à des
SOMMES et des COMPTAGES (`aggregate_file`), puis les agrégats sont combinés (`combine`) — le principe
« map → reduce ». Les questions sont ensuite calculées sur ces agrégats.

Questions du sujet :
  Q1  Stations au plus fort taux d'utilisation      → activité moyenne par jour (mouvements = emprunts + retours)
  Q2  Zones où l'offre de vélos est insuffisante    → % du temps où les stations sont vides, grille ~500 m
  Q3  Pics d'utilisation journaliers                → activité par heure locale × jour de semaine
  Q4  Déséquilibres géographiques                   → remplissage matin vs soir, centre vs périphérie
  Q5  Stations critiques à rééquilibrer             → % des heures de pointe vides / pleines → score

Heures de pointe (jours ouvrés, heure locale) : 7 h-10 h et 17 h-20 h.

Responsable : P2.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

COLUMNS = ["ville_id", "station_id", "heure_utc", "activite", "taux_remplissage", "est_vide", "est_pleine",
           "etat_observe", "is_valid"]
MORNING, EVENING = (7, 8, 9), (17, 18, 19)
CELL_M = 500  # taille de maille des zones (m)


# ---------------------------------------------------------------- map : un fichier → agrégats

def prepare(df: pd.DataFrame) -> pd.DataFrame:
    d = df[df["is_valid"]].copy()
    local = d["heure_utc"].dt.tz_convert("Europe/Paris")
    d["h"] = local.dt.hour.astype("int8")
    d["wd"] = local.dt.dayofweek.astype("int8")
    d["ouvre"] = d["wd"] < 5
    d["pointe"] = d["ouvre"] & d["h"].isin(MORNING + EVENING)
    d["matin"] = d["ouvre"] & d["h"].isin(MORNING)
    d["soir"] = d["ouvre"] & d["h"].isin(EVENING)
    for c in ("est_vide", "est_pleine"):
        d[c] = d[c].astype("int32")
    return d


def aggregate_file(df: pd.DataFrame) -> dict:
    """Réduit un fichier silver à des sommes / comptages additifs."""
    d = prepare(df)
    station = d.groupby(["ville_id", "station_id"]).agg(
        heures=("activite", "size"), activite=("activite", "sum"), vide=("est_vide", "sum"),
        pleine=("est_pleine", "sum"), remplissage=("taux_remplissage", "sum"))
    peak = d[d["pointe"]].groupby(["ville_id", "station_id"]).agg(
        h_pointe=("activite", "size"), vide_pointe=("est_vide", "sum"), pleine_pointe=("est_pleine", "sum"))
    mat = d[d["matin"]].groupby(["ville_id", "station_id"]).agg(
        h_matin=("activite", "size"), rempl_matin=("taux_remplissage", "sum"))
    soi = d[d["soir"]].groupby(["ville_id", "station_id"]).agg(
        h_soir=("activite", "size"), rempl_soir=("taux_remplissage", "sum"))
    station = station.join(peak, how="left").join(mat, how="left").join(soi, how="left").fillna(0)
    week = d.groupby(["ville_id", "wd", "h"]).agg(heures=("activite", "size"), activite=("activite", "sum"))
    return {"station": station, "week": week}


def combine(parts: list[dict]) -> dict:
    """Somme des agrégats de tous les fichiers (reduce)."""
    return {k: pd.concat([p[k] for p in parts]).groupby(level=list(range(parts[0][k].index.nlevels))).sum()
            for k in parts[0]}


def load_aggregates(storage, read_table) -> dict:
    keys = sorted(k for k in storage.list("silver/velos/") if k.endswith("velos_heure.parquet"))
    return combine([aggregate_file(read_table(storage, k, COLUMNS)) for k in keys])


# ---------------------------------------------------------------- indicateurs par station

def haversine_m(lat1, lon1, lat2, lon2):
    r = 6_371_000
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dp, dl = p2 - p1, np.radians(lon2 - lon1)
    a = np.sin(dp / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * r * np.arcsin(np.sqrt(a))


def station_indicators(agg: dict, stations: pd.DataFrame, villes: list[dict], min_jours: int = 30) -> pd.DataFrame:
    """Indicateurs par station (seules les stations observées au moins `min_jours` jours sont gardées)."""
    s = agg["station"].reset_index()
    s["jours"] = s["heures"] / 24
    s = s[s["jours"] >= min_jours].copy()
    s["activite_jour"] = s["activite"] / s["jours"]
    s["pct_vide"] = 100 * s["vide"] / s["heures"]
    s["pct_pleine"] = 100 * s["pleine"] / s["heures"]
    s["remplissage_moy"] = s["remplissage"] / s["heures"]
    s["pct_vide_pointe"] = 100 * s["vide_pointe"] / s["h_pointe"].replace(0, np.nan)
    s["pct_pleine_pointe"] = 100 * s["pleine_pointe"] / s["h_pointe"].replace(0, np.nan)
    s["rempl_matin"] = s["rempl_matin"] / s["h_matin"].replace(0, np.nan)
    s["rempl_soir"] = s["rempl_soir"] / s["h_soir"].replace(0, np.nan)
    s["delta_soir_matin"] = s["rempl_soir"] - s["rempl_matin"]
    s = s.merge(stations[["ville_id", "station_id", "station_nom", "latitude", "longitude", "capacite_declaree"]],
                on=["ville_id", "station_id"], how="left")
    centre = pd.DataFrame(villes)[["ville_id", "lat", "lon"]]
    s = s.merge(centre, on="ville_id", how="left")
    s["distance_centre_km"] = haversine_m(s["lat"], s["lon"], s["latitude"], s["longitude"]) / 1000
    s["statut_station"] = station_status(s)
    return s.drop(columns=["lat", "lon"])


MAX_ROTATION = 50  # mouvements / jour / place : au-delà, la « station » n'est pas une borne réelle (q99 ≈ 15)


def station_status(s: pd.DataFrame) -> pd.Series:
    """technique (nom vide / TEST) · aberrante (rotation impossible) · inactive (< 1 mouvement/jour) · exploitable."""
    nom = s["station_nom"].fillna("").str.strip()
    # rotation calculée seulement si la capacité déclarée est crédible (≥ 5 places) : une capacité saisie à 1
    # ferait passer une station normale pour aberrante
    rotation = s["activite_jour"] / s["capacite_declaree"].where(s["capacite_declaree"] >= 5)
    technique = nom.eq("") | nom.str.contains(r"\btest\b|atelier", case=False, regex=True)
    return pd.Series(np.select(
        [technique, rotation > MAX_ROTATION, s["activite_jour"] < 1],
        ["technique", "aberrante", "inactive"], default="exploitable"), index=s.index)


def usable(ind: pd.DataFrame) -> pd.DataFrame:
    return ind[ind["statut_station"] == "exploitable"]


# ---------------------------------------------------------------- Q1 utilisation

def top_stations(ind: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    return ind.nlargest(n, "activite_jour")[["ville_id", "station_nom", "activite_jour", "capacite_declaree",
                                            "distance_centre_km"]].reset_index(drop=True)


def concentration(ind: pd.DataFrame) -> pd.DataFrame:
    """Part de l'activité totale portée par les 10 % de stations les plus actives, par ville."""
    rows = []
    for v, g in ind.groupby("ville_id"):
        s = g["activite_jour"].sort_values(ascending=False)
        k = max(1, int(round(len(s) * 0.10)))
        rows.append({"ville_id": v, "stations": len(s), "activite_jour_station": round(s.mean(), 1),
                     "part_top10pct": round(100 * s.iloc[:k].sum() / s.sum(), 1)})
    return pd.DataFrame(rows).sort_values("activite_jour_station", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------- Q2 sous-offre

def grid_cells(ind: pd.DataFrame, cell_m: int = CELL_M) -> pd.DataFrame:
    """Zones carrées d'environ `cell_m` mètres : % du temps vide (pondéré par les heures)."""
    lat_step = cell_m / 111_320
    d = ind.copy()
    d["lon_step"] = lat_step / np.cos(np.radians(d["latitude"]))
    d["cell_lat"] = (d["latitude"] / lat_step).round().astype(int)
    d["cell_lon"] = (d["longitude"] / d["lon_step"]).round().astype(int)
    g = d.groupby(["ville_id", "cell_lat", "cell_lon"]).agg(
        stations=("station_id", "size"), heures=("heures", "sum"), vide=("vide", "sum"),
        latitude=("latitude", "mean"), longitude=("longitude", "mean"),
        activite_jour=("activite_jour", "sum"))
    g["pct_vide"] = 100 * g["vide"] / g["heures"]
    return g.reset_index().drop(columns=["heures", "vide"])


# ---------------------------------------------------------------- Q3 pics

def weekly_profile(agg: dict) -> pd.DataFrame:
    """Activité moyenne par station et par heure : ville × jour de semaine × heure locale."""
    w = agg["week"].reset_index()
    w["activite_station_h"] = w["activite"] / w["heures"]
    return w


def peak_hours(week: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for v, g in week.groupby("ville_id"):
        wd = g[g["wd"] < 5].groupby("h")["activite_station_h"].mean()
        we = g[g["wd"] >= 5].groupby("h")["activite_station_h"].mean()
        rows.append({"ville_id": v, "pic_matin_h": int(wd.loc[5:11].idxmax()),
                     "pic_soir_h": int(wd.loc[14:21].idxmax()), "pic_weekend_h": int(we.idxmax()),
                     "ratio_pic_creux": round(wd.max() / wd.loc[2:4].mean(), 1),
                     "ratio_semaine_weekend": round(wd.mean() / we.mean(), 2)})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- Q4 déséquilibres

def imbalance_by_distance(ind: pd.DataFrame, bands=(0, 1, 2, 4, 8, 50)) -> pd.DataFrame:
    d = ind.dropna(subset=["rempl_matin", "rempl_soir"]).copy()
    d["zone"] = pd.cut(d["distance_centre_km"], bands, right=False,
                       labels=[f"{a}-{b} km" for a, b in zip(bands[:-1], bands[1:])])
    return d.groupby("zone", observed=True).agg(stations=("station_id", "size"),
                                                rempl_matin=("rempl_matin", "mean"),
                                                rempl_soir=("rempl_soir", "mean")).round(3).reset_index()


# ---------------------------------------------------------------- Q5 criticité

def critical_stations(ind: pd.DataFrame, n: int = 15) -> pd.DataFrame:
    """Score = % des heures de pointe où la station est vide ou pleine (inutilisable pour emprunter / rendre)."""
    d = ind.dropna(subset=["pct_vide_pointe", "pct_pleine_pointe"]).copy()
    d["score_criticite"] = (d["pct_vide_pointe"] + d["pct_pleine_pointe"]).round(1)
    d["action"] = np.where(d["pct_vide_pointe"] >= d["pct_pleine_pointe"], "réapprovisionner", "désengorger")
    cols = ["ville_id", "station_nom", "score_criticite", "pct_vide_pointe", "pct_pleine_pointe", "action",
            "activite_jour"]
    return d.nlargest(n, "score_criticite")[cols].round(1).reset_index(drop=True)


def critical_share(ind: pd.DataFrame, seuil: float = 30.0) -> pd.DataFrame:
    """Par ville : part des stations critiques (vides ou pleines ≥ `seuil` % des heures de pointe)."""
    d = ind.dropna(subset=["pct_vide_pointe"]).assign(
        critique=lambda x: (x["pct_vide_pointe"] + x["pct_pleine_pointe"]) >= seuil)
    return (d.groupby("ville_id").agg(stations=("station_id", "size"), critiques=("critique", "sum"))
             .assign(pct_critiques=lambda x: (100 * x["critiques"] / x["stations"]).round(1))
             .sort_values("pct_critiques", ascending=False).reset_index())
