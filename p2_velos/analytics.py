"""P2 — Fonctions analytiques répondant aux 5 questions métier sur la mobilité urbaine.

Partagées entre le dashboard Streamlit (dashboard/pages/2_Velos.py) et le notebook
d'analyse exploratoire (notebooks/02_velos.ipynb) pour éviter toute duplication.

Questions métier couvertes :
1. Quelles stations présentent le plus fort taux d'utilisation ?
2. Peut-on identifier des zones où l'offre de vélos est insuffisante ?
3. Quels sont les pics d'utilisation journaliers du réseau de vélos ?
4. Existe-t-il des déséquilibres géographiques dans la disponibilité des vélos ?
5. Peut-on détecter des stations critiques nécessitant un rééquilibrage des vélos ?

Responsable : P2.
"""

from __future__ import annotations

import math
from typing import Any, Mapping

import pandas as pd


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance grand-cercle en kilomètres entre deux coordonnées WGS84."""
    r = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    return 2.0 * r * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def ensure_enriched_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """S'assure que le DataFrame dispose des colonnes dérivées et timestamps UTC."""
    if df.empty:
        return df.copy()
    out = df.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True)

    if "capacity" not in out.columns:
        out["capacity"] = out["bikes_available"] + out["free_slots"]
    safe_cap = out["capacity"].replace(0, pd.NA)
    if "taux_occupation" not in out.columns:
        out["taux_occupation"] = (out["bikes_available"] / safe_cap).fillna(0.0).astype(float)
    if "taux_utilisation" not in out.columns:
        out["taux_utilisation"] = (out["free_slots"] / safe_cap).fillna(0.0).astype(float)
    if "est_vide" not in out.columns:
        out["est_vide"] = (out["bikes_available"] == 0).astype(int)
    if "est_pleine" not in out.columns:
        out["est_pleine"] = (out["free_slots"] == 0).astype(int)
    if "est_critique" not in out.columns:
        out["est_critique"] = (
            (out["taux_occupation"] <= 0.10) | (out["taux_occupation"] >= 0.90)
        ).astype(int)
    return out


# ============================================================================
# Question 1 : Quelles stations présentent le plus fort taux d'utilisation ?
# ============================================================================
def top_utilisation_stations(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """Classe les stations par taux d'utilisation moyen, volatilité et rotation."""
    data = ensure_enriched_dataframe(df)
    if data.empty:
        return pd.DataFrame()

    data = data.sort_values(["ville_id", "station_id", "timestamp"])
    data["abs_delta_bikes"] = (
        data.groupby(["ville_id", "station_id"])["bikes_available"].diff().abs().fillna(0.0)
    )

    agg = (
        data.groupby(["ville_id", "station_id"], as_index=False)
        .agg(
            station_name=("station_name", "last"),
            latitude=("latitude", "last"),
            longitude=("longitude", "last"),
            capacite_moyenne=("capacity", "mean"),
            velos_dispo_moyens=("bikes_available", "mean"),
            taux_utilisation_moyen=("taux_utilisation", "mean"),
            taux_occupation_moyen=("taux_occupation", "mean"),
            volatilite_velos=("bikes_available", "std"),
            min_velos=("bikes_available", "min"),
            max_velos=("bikes_available", "max"),
            rotation_totale=("abs_delta_bikes", "sum"),
            nb_releves=("timestamp", "count"),
        )
    )
    agg["volatilite_velos"] = agg["volatilite_velos"].fillna(0.0).round(2)
    agg["amplitude_velos"] = agg["max_velos"] - agg["min_velos"]
    agg["taux_utilisation_pct"] = (agg["taux_utilisation_moyen"] * 100.0).round(1)
    agg["taux_occupation_pct"] = (agg["taux_occupation_moyen"] * 100.0).round(1)
    agg["capacite_moyenne"] = agg["capacite_moyenne"].round(1)
    agg["velos_dispo_moyens"] = agg["velos_dispo_moyens"].round(1)

    return agg.sort_values(
        by=["taux_utilisation_moyen", "rotation_totale", "capacite_moyenne"],
        ascending=[False, False, False],
    ).head(top_n)


# ============================================================================
# Question 2 : Zones où l'offre de vélos est insuffisante
# ============================================================================
def detect_undersupplied_zones(
    df: pd.DataFrame,
    grid_precision: int = 2,
    low_supply_threshold: float = 0.15,
    min_stations: int = 1,
) -> pd.DataFrame:
    """Identifie les zones géographiques souffrant d'une offre de vélos insuffisante.

    Regroupe les stations par cellule spatiale (grid_precision=2 ~ 1.1 km, 3 ~ 110 m)
    et calcule la fréquence de pénurie (<= 15 % de vélos disponibles) et de vacuité (0 vélo).
    """
    data = ensure_enriched_dataframe(df)
    if data.empty:
        return pd.DataFrame()

    data["zone_lat"] = data["latitude"].round(grid_precision)
    data["zone_lon"] = data["longitude"].round(grid_precision)
    data["en_sous_offre"] = (data["taux_occupation"] <= low_supply_threshold).astype(int)

    zones = (
        data.groupby(["ville_id", "zone_lat", "zone_lon"], as_index=False)
        .agg(
            nb_stations=("station_id", "nunique"),
            exemple_station=("station_name", "first"),
            capacite_totale=("capacity", lambda s: round(float(s.mean()) * data.loc[s.index, "station_id"].nunique())),
            velos_dispo_moyens=("bikes_available", "mean"),
            taux_occupation_moyen=("taux_occupation", "mean"),
            pct_temps_vide=("est_vide", "mean"),
            pct_temps_sous_offre=("en_sous_offre", "mean"),
            nb_releves=("timestamp", "count"),
        )
    )
    zones = zones[zones["nb_stations"] >= min_stations].copy()
    zones["velos_dispo_moyens"] = zones["velos_dispo_moyens"].round(2)
    zones["taux_occupation_pct"] = (zones["taux_occupation_moyen"] * 100.0).round(1)
    zones["pct_temps_vide"] = (zones["pct_temps_vide"] * 100.0).round(1)
    zones["pct_temps_sous_offre"] = (zones["pct_temps_sous_offre"] * 100.0).round(1)

    return zones.sort_values(
        by=["pct_temps_sous_offre", "pct_temps_vide", "taux_occupation_moyen"],
        ascending=[False, False, True],
    )


# ============================================================================
# Question 3 : Pics d'utilisation journaliers du réseau
# ============================================================================
def daily_usage_peaks(df: pd.DataFrame, tz: str = "Europe/Paris") -> pd.DataFrame:
    """Calcule le profil horaire (0h-23h heure locale) distinguant semaine et week-end."""
    data = ensure_enriched_dataframe(df)
    if data.empty:
        return pd.DataFrame()

    local_ts = data["timestamp"].dt.tz_convert(tz)
    data["heure_locale"] = local_ts.dt.hour
    data["type_jour"] = local_ts.dt.day_of_week.map(lambda d: "Week-end" if d >= 5 else "Semaine")

    peaks = (
        data.groupby(["ville_id", "type_jour", "heure_locale"], as_index=False)
        .agg(
            taux_utilisation_moyen=("taux_utilisation", "mean"),
            taux_occupation_moyen=("taux_occupation", "mean"),
            velos_dispo_moyens=("bikes_available", "mean"),
            pct_stations_vides=("est_vide", "mean"),
            pct_stations_pleines=("est_pleine", "mean"),
            nb_releves=("timestamp", "count"),
        )
    )
    peaks["taux_utilisation_pct"] = (peaks["taux_utilisation_moyen"] * 100.0).round(1)
    peaks["taux_occupation_pct"] = (peaks["taux_occupation_moyen"] * 100.0).round(1)
    peaks["pct_stations_vides"] = (peaks["pct_stations_vides"] * 100.0).round(1)
    peaks["pct_stations_pleines"] = (peaks["pct_stations_pleines"] * 100.0).round(1)
    return peaks.sort_values(["ville_id", "type_jour", "heure_locale"])


# ============================================================================
# Question 4 : Déséquilibres géographiques dans la disponibilité des vélos
# ============================================================================
def geographic_imbalance(
    df: pd.DataFrame,
    city_centers: Mapping[str, tuple[float, float]],
    center_radius_km: float = 1.5,
) -> pd.DataFrame:
    """Mesure les écarts de disponibilité entre le centre-ville et les quadrants N/S/E/O."""
    data = ensure_enriched_dataframe(df)
    if data.empty:
        return pd.DataFrame()

    def _classify_sector(row: Any) -> tuple[str, float]:
        center = city_centers.get(row["ville_id"])
        if center is None:
            return ("Inconnu", 0.0)
        clat, clon = center
        dist = haversine_km(row["latitude"], row["longitude"], clat, clon)
        if dist <= center_radius_km:
            return (f"Centre (<{center_radius_km:.1f} km)", dist)
        ns = "Nord" if row["latitude"] >= clat else "Sud"
        ew = "Est" if row["longitude"] >= clon else "Ouest"
        return (f"{ns}-{ew}", dist)

    sectors = data.apply(_classify_sector, axis=1)
    data["secteur_geo"] = [s[0] for s in sectors]
    data["distance_centre_km"] = [s[1] for s in sectors]

    summary = (
        data.groupby(["ville_id", "secteur_geo"], as_index=False)
        .agg(
            nb_stations=("station_id", "nunique"),
            distance_moyenne_km=("distance_centre_km", "mean"),
            velos_dispo_moyens=("bikes_available", "mean"),
            taux_occupation_moyen=("taux_occupation", "mean"),
            taux_utilisation_moyen=("taux_utilisation", "mean"),
            pct_temps_vide=("est_vide", "mean"),
            pct_temps_pleine=("est_pleine", "mean"),
            ecart_type_occupation=("taux_occupation", "std"),
        )
    )
    summary["distance_moyenne_km"] = summary["distance_moyenne_km"].round(2)
    summary["velos_dispo_moyens"] = summary["velos_dispo_moyens"].round(1)
    summary["taux_occupation_pct"] = (summary["taux_occupation_moyen"] * 100.0).round(1)
    summary["taux_utilisation_pct"] = (summary["taux_utilisation_moyen"] * 100.0).round(1)
    summary["pct_temps_vide"] = (summary["pct_temps_vide"] * 100.0).round(1)
    summary["pct_temps_pleine"] = (summary["pct_temps_pleine"] * 100.0).round(1)
    summary["ecart_type_occupation"] = summary["ecart_type_occupation"].fillna(0.0).round(3)

    # Écart par rapport à la moyenne de la ville (indice de déséquilibre spatial)
    city_avg = summary.groupby("ville_id")["taux_occupation_pct"].transform("mean")
    summary["ecart_moyenne_ville_pts"] = (summary["taux_occupation_pct"] - city_avg).round(1)
    return summary.sort_values(["ville_id", "ecart_moyenne_ville_pts"])


# ============================================================================
# Question 5 : Détection des stations critiques nécessitant un rééquilibrage
# ============================================================================
def detect_critical_stations(
    df: pd.DataFrame,
    low_threshold: float = 0.10,
    high_threshold: float = 0.90,
) -> pd.DataFrame:
    """Détecte les stations critiques (pénurie <= 10 % ou saturation >= 90 %) et propose une action."""
    data = ensure_enriched_dataframe(df)
    if data.empty:
        return pd.DataFrame()

    data = data.sort_values(["ville_id", "station_id", "timestamp"])

    persistence = (
        data.groupby(["ville_id", "station_id"], as_index=False)
        .agg(
            pct_temps_critique=("est_critique", "mean"),
            pct_temps_vide=("est_vide", "mean"),
            pct_temps_pleine=("est_pleine", "mean"),
            nb_releves=("timestamp", "count"),
        )
    )

    latest = data.groupby(["ville_id", "station_id"], as_index=False).last()
    merged = latest.merge(persistence, on=["ville_id", "station_id"], how="left")

    def _action(occ: float) -> str:
        if occ <= low_threshold:
            return "REAPPROVISIONNER"
        if occ >= high_threshold:
            return "DESENGORGER"
        return "AUCUNE"

    def _severity(row: Any) -> str:
        occ = row["taux_occupation"]
        if row["bikes_available"] == 0:
            return "CRITIQUE_VIDE"
        if row["free_slots"] == 0:
            return "CRITIQUE_PLEINE"
        if occ <= low_threshold:
            return "PENURIE_CRITIQUE"
        if occ >= high_threshold:
            return "SATURATION_CRITIQUE"
        return "NORMAL"

    merged["action_reequilibrage"] = merged["taux_occupation"].map(_action)
    merged["statut_criticite"] = merged.apply(_severity, axis=1)
    merged["taux_occupation_pct"] = (merged["taux_occupation"] * 100.0).round(1)
    merged["pct_temps_critique"] = (merged["pct_temps_critique"] * 100.0).round(1)
    merged["delta_velos_equilibre"] = (
        (merged["capacity"] * 0.5).round().astype(int) - merged["bikes_available"].astype(int)
    )

    # Score de priorité (combinaison sévérité immédiate, persistance et taille de la station)
    merged["score_priorite"] = (
        (merged["taux_occupation"] - 0.5).abs() * 2.0 * 50.0
        + merged["pct_temps_critique"] * 0.35
        + merged["capacity"].clip(upper=50) * 0.30
    ).round(1)

    critical_only = merged[merged["action_reequilibrage"] != "AUCUNE"].copy()
    cols = [
        "ville_id",
        "station_id",
        "station_name",
        "latitude",
        "longitude",
        "timestamp",
        "bikes_available",
        "free_slots",
        "capacity",
        "taux_occupation_pct",
        "statut_criticite",
        "action_reequilibrage",
        "delta_velos_equilibre",
        "pct_temps_critique",
        "score_priorite",
    ]
    return critical_only[cols].sort_values("score_priorite", ascending=False)
