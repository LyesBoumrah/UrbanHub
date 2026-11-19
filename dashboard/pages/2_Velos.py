"""Dashboard — P2 Vélos (mobilité urbaine temps réel + historique 5 ans CityBikes).

Exécute les agrégations analytiques directement dans ClickHouse (SQL pushdown, sans
troncature même sur plusieurs millions de lignes sur 5 ans), avec repli automatique
sur les fichiers silver locaux si ClickHouse n'est pas joignable.

Répond visuellement aux 5 questions métier :
1. Stations au plus fort taux d'utilisation
2. Zones où l'offre de vélos est insuffisante
3. Pics d'utilisation journaliers (+ tendance mensuelle multi-annuelle jusqu'à 5 ans)
4. Déséquilibres géographiques dans la disponibilité
5. Détection des stations critiques nécessitant un rééquilibrage

Responsable : P2.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from common.config import get_settings, load_ref_ville
from common.warehouse import get_warehouse
from p2_velos.analytics import (
    daily_usage_peaks,
    detect_critical_stations,
    detect_undersupplied_zones,
    ensure_enriched_dataframe,
    geographic_imbalance,
    top_utilisation_stations,
)
from p2_velos.cleaning import clean_bronze_payload, get_bbox_map, save_silver_local
from p2_velos.ingestion import ingest_all_cities

st.set_page_config(
    page_title="UrbanHub — P2 Mobilité Vélos",
    page_icon="🚲",
    layout="wide",
)

st.title("🚲 P2 — Mobilité urbaine (CityBikes : Streaming + Historique 5 ans)")
st.caption(
    "Flux streaming toutes les 2-3 min (`api.citybik.es/v2`) + "
    "backfill historique officiel (`data.citybik.es`, fenêtre jusqu'à 5 ans)"
)

REF_VILLES = load_ref_ville()
VILLE_NOM_MAP = {v["ville_id"]: v["nom"] for v in REF_VILLES}
CITY_CENTERS = {v["ville_id"]: (float(v["lat"]), float(v["lon"])) for v in REF_VILLES}

TIME_WINDOWS: dict[str, tuple[str, int]] = {
    "24 dernières heures": ("24 HOUR", 24),
    "7 derniers jours": ("7 DAY", 24 * 7),
    "30 derniers jours": ("30 DAY", 24 * 30),
    "6 derniers mois": ("6 MONTH", 24 * 183),
    "1 an": ("1 YEAR", 24 * 365),
    "2 ans": ("2 YEAR", 24 * 365 * 2),
    "5 ans (tout l'historique)": ("5 YEAR", 24 * 365 * 5),
}


def _where_clause(interval_sql: str, selected_cities: tuple[str, ...], alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    clause = f"{prefix}timestamp >= now() - INTERVAL {interval_sql}"
    if selected_cities:
        quoted = ", ".join(f"'{c}'" for c in selected_cities)
        clause += f" AND {prefix}ville_id IN ({quoted})"
    return clause


@st.cache_data(ttl=120)
def ch_overview(interval_sql: str, selected_cities: tuple[str, ...]) -> dict[str, object]:
    wh = get_warehouse()
    where = _where_clause(interval_sql, selected_cities)
    meta = wh.query_df(
        f"""
        SELECT
            count() AS total_releves,
            min(timestamp) AS min_ts,
            max(timestamp) AS max_ts
        FROM urbanhub.velo_releve
        WHERE {where}
        """
    )
    if meta.empty or int(meta.iloc[0]["total_releves"]) == 0:
        return {"total_releves": 0}
    latest = wh.query_df(
        f"""
        SELECT
            count() AS nb_stations,
            sum(bikes_available) AS total_bikes,
            sum(free_slots) AS total_slots,
            round(avg(taux_utilisation) * 100, 1) AS avg_util_pct
        FROM (
            SELECT
                ville_id,
                station_id,
                argMax(bikes_available, timestamp) AS bikes_available,
                argMax(free_slots, timestamp) AS free_slots,
                argMax(taux_utilisation, timestamp) AS taux_utilisation
            FROM urbanhub.velo_releve
            WHERE {where}
            GROUP BY ville_id, station_id
        )
        """
    )
    return {
        "total_releves": int(meta.iloc[0]["total_releves"]),
        "min_ts": pd.to_datetime(meta.iloc[0]["min_ts"], utc=True),
        "max_ts": pd.to_datetime(meta.iloc[0]["max_ts"], utc=True),
        "nb_stations": int(latest.iloc[0]["nb_stations"]),
        "total_bikes": int(latest.iloc[0]["total_bikes"]),
        "total_slots": int(latest.iloc[0]["total_slots"]),
        "avg_util_pct": float(latest.iloc[0]["avg_util_pct"]),
    }


@st.cache_data(ttl=120)
def ch_q1_top_stations(interval_sql: str, selected_cities: tuple[str, ...], top_n: int) -> pd.DataFrame:
    wh = get_warehouse()
    where = _where_clause(interval_sql, selected_cities)
    return wh.query_df(
        f"""
        SELECT
            ville_id,
            station_id,
            anyLast(station_name) AS station_name,
            round(avg(capacity), 1) AS capacite_moyenne,
            round(avg(bikes_available), 1) AS velos_dispo_moyens,
            round(avg(taux_utilisation) * 100, 1) AS taux_utilisation_pct,
            round(stddevPop(bikes_available), 2) AS volatilite_velos,
            max(bikes_available) - min(bikes_available) AS amplitude_velos,
            count() AS nb_releves
        FROM urbanhub.velo_releve
        WHERE {where}
        GROUP BY ville_id, station_id
        ORDER BY taux_utilisation_pct DESC, volatilite_velos DESC, capacite_moyenne DESC
        LIMIT {int(top_n)}
        """
    )


@st.cache_data(ttl=120)
def ch_q2_zones(interval_sql: str, selected_cities: tuple[str, ...], grid_prec: int) -> pd.DataFrame:
    wh = get_warehouse()
    where = _where_clause(interval_sql, selected_cities)
    return wh.query_df(
        f"""
        SELECT
            ville_id,
            round(latitude, {int(grid_prec)}) AS zone_lat,
            round(longitude, {int(grid_prec)}) AS zone_lon,
            uniqExact(station_id) AS nb_stations,
            any(station_name) AS exemple_station,
            round(avg(bikes_available), 2) AS velos_dispo_moyens,
            round(countIf(est_vide = 1) / count() * 100, 1) AS pct_temps_vide,
            round(countIf(taux_occupation <= 0.15) / count() * 100, 1) AS pct_temps_sous_offre,
            count() AS nb_releves
        FROM urbanhub.velo_releve
        WHERE {where}
        GROUP BY ville_id, zone_lat, zone_lon
        ORDER BY pct_temps_sous_offre DESC, pct_temps_vide DESC, nb_stations DESC
        LIMIT 150
        """
    )


@st.cache_data(ttl=120)
def ch_q3_peaks_and_monthly(
    interval_sql: str, selected_cities: tuple[str, ...]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    wh = get_warehouse()
    where = _where_clause(interval_sql, selected_cities)
    peaks = wh.query_df(
        f"""
        SELECT
            ville_id,
            if(toDayOfWeek(toTimezone(timestamp, 'Europe/Paris')) IN (6, 7), 'Week-end', 'Semaine') AS type_jour,
            toHour(toTimezone(timestamp, 'Europe/Paris')) AS heure_locale,
            round(avg(taux_utilisation) * 100, 1) AS taux_utilisation_pct,
            round(avg(taux_occupation) * 100, 1) AS taux_occupation_pct,
            round(countIf(est_vide = 1) / count() * 100, 1) AS pct_stations_vides,
            round(countIf(est_pleine = 1) / count() * 100, 1) AS pct_stations_pleines,
            count() AS nb_releves
        FROM urbanhub.velo_releve
        WHERE {where}
        GROUP BY ville_id, type_jour, heure_locale
        ORDER BY ville_id, type_jour, heure_locale
        """
    )
    monthly = wh.query_df(
        f"""
        SELECT
            ville_id,
            formatDateTime(toStartOfMonth(timestamp), '%Y-%m') AS mois,
            round(avg(taux_utilisation) * 100, 1) AS taux_utilisation_pct,
            round(countIf(est_critique = 1) / count() * 100, 1) AS pct_critiques,
            count() AS nb_releves
        FROM urbanhub.velo_releve
        WHERE {where}
        GROUP BY ville_id, mois
        ORDER BY mois, ville_id
        """
    )
    return peaks, monthly


@st.cache_data(ttl=120)
def ch_q4_imbalance(interval_sql: str, selected_cities: tuple[str, ...]) -> pd.DataFrame:
    wh = get_warehouse()
    where = _where_clause(interval_sql, selected_cities, alias="r")
    df = wh.query_df(
        f"""
        SELECT
            r.ville_id AS ville_id,
            multiIf(
                geoDistance(r.longitude, r.latitude, v.longitude, v.latitude) <= 1500, 'Centre (<1.5 km)',
                r.latitude >= v.latitude AND r.longitude >= v.longitude, 'Nord-Est',
                r.latitude >= v.latitude AND r.longitude < v.longitude, 'Nord-Ouest',
                r.latitude < v.latitude AND r.longitude >= v.longitude, 'Sud-Est',
                'Sud-Ouest'
            ) AS secteur_geo,
            uniqExact(r.station_id) AS nb_stations,
            round(avg(r.taux_occupation) * 100, 1) AS taux_occupation_pct,
            round(countIf(r.est_vide = 1) / count() * 100, 1) AS pct_temps_vide,
            round(countIf(r.est_pleine = 1) / count() * 100, 1) AS pct_temps_pleine,
            round(avg(geoDistance(r.longitude, r.latitude, v.longitude, v.latitude)) / 1000.0, 2) AS distance_moyenne_km
        FROM urbanhub.velo_releve AS r
        LEFT JOIN urbanhub.ref_ville AS v ON r.ville_id = v.ville_id
        WHERE {where}
        GROUP BY r.ville_id, secteur_geo
        """
    )
    if not df.empty:
        city_avg = df.groupby("ville_id")["taux_occupation_pct"].transform("mean")
        df["ecart_moyenne_ville_pts"] = (df["taux_occupation_pct"] - city_avg).round(1)
        df = df.sort_values(["ville_id", "ecart_moyenne_ville_pts"])
    return df


@st.cache_data(ttl=120)
def ch_q5_critical(
    interval_sql: str,
    selected_cities: tuple[str, ...],
    low_thresh: float,
    high_thresh: float,
) -> pd.DataFrame:
    wh = get_warehouse()
    where = _where_clause(interval_sql, selected_cities)
    return wh.query_df(
        f"""
        SELECT
            ville_id,
            station_id,
            station_name,
            latitude,
            longitude,
            dernier_timestamp AS timestamp,
            bikes_available,
            free_slots,
            capacity,
            round(dernier_taux_occupation * 100, 1) AS taux_occupation_pct,
            multiIf(
                dernier_taux_occupation <= {float(low_thresh)}, 'REAPPROVISIONNER',
                dernier_taux_occupation >= {float(high_thresh)}, 'DESENGORGER',
                'AUCUNE'
            ) AS action_reequilibrage,
            toInt32(round(capacity * 0.5) - bikes_available) AS delta_velos_equilibre,
            round(pct_temps_critique * 100, 1) AS pct_temps_critique,
            round(
                abs(dernier_taux_occupation - 0.5) * 100.0
                + pct_temps_critique * 35.0
                + least(capacity, 50) * 0.30,
                1
            ) AS score_priorite
        FROM (
            SELECT
                ville_id,
                station_id,
                argMax(station_name, timestamp) AS station_name,
                argMax(latitude, timestamp) AS latitude,
                argMax(longitude, timestamp) AS longitude,
                max(timestamp) AS dernier_timestamp,
                argMax(bikes_available, timestamp) AS bikes_available,
                argMax(free_slots, timestamp) AS free_slots,
                argMax(capacity, timestamp) AS capacity,
                argMax(taux_occupation, timestamp) AS dernier_taux_occupation,
                countIf(est_critique = 1) / count() AS pct_temps_critique
            FROM urbanhub.velo_releve
            WHERE {where}
            GROUP BY ville_id, station_id
        )
        WHERE dernier_taux_occupation <= {float(low_thresh)}
           OR dernier_taux_occupation >= {float(high_thresh)}
        ORDER BY score_priorite DESC
        """
    )


def load_local_silver_data(
    selected_cities: list[str] | None = None,
    hours_back: int = 24 * 365 * 5,
) -> pd.DataFrame:
    """Repli local : charge data/silver/velos/ et data/silver/velos_historique/."""
    base_silver = Path(get_settings().local_data_dir) / "silver"
    frames: list[pd.DataFrame] = []

    stream_dir = base_silver / "velos"
    if stream_dir.exists():
        for p in sorted(stream_dir.glob("velo_releve_*.parquet")):
            frames.append(pd.read_parquet(p))
        for p in sorted(stream_dir.glob("velo_releve_*.jsonl")):
            frames.append(pd.read_json(p, lines=True))

    hist_dir = base_silver / "velos_historique"
    if hist_dir.exists():
        for p in sorted(hist_dir.rglob("velo_releve_*.parquet")):
            if selected_cities and p.parent.name not in selected_cities:
                continue
            frames.append(pd.read_parquet(p))

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    if selected_cities:
        df = df[df["ville_id"].isin(selected_cities)]
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours_back)
    df = df[df["timestamp"] >= cutoff]
    return ensure_enriched_dataframe(
        df.drop_duplicates(subset=["ville_id", "station_id", "timestamp"], keep="last")
    )


def fetch_live_snapshot(selected_cities: list[str]) -> pd.DataFrame:
    """Collecte un snapshot temps réel CityBikes, le nettoie et le sauve en silver local."""
    results = ingest_all_cities(cities=selected_cities, use_local_storage=True, pause_seconds=0.1)
    bbox_map = get_bbox_map()
    now_utc = datetime.now(timezone.utc)
    all_stations, all_releves = [], []
    for res in results:
        if res.ok and res.payload is not None:
            st_rows, rel_rows, _ = clean_bronze_payload(
                res.payload,
                bbox=bbox_map.get(res.ville_id),
                now_utc=now_utc,
                ville_id=res.ville_id,
                network_id=res.network_id,
            )
            all_stations.extend(st_rows)
            all_releves.extend(rel_rows)
    if all_releves:
        save_silver_local(all_stations, all_releves)
    return pd.DataFrame(all_releves)


# ============================================================================
# Barre latérale
# ============================================================================
with st.sidebar:
    st.header("Paramètres")
    all_city_ids = [v["ville_id"] for v in REF_VILLES]
    selected_cities = st.multiselect(
        "Villes françaises",
        options=all_city_ids,
        default=all_city_ids[:5],
        format_func=lambda cid: VILLE_NOM_MAP.get(cid, cid),
    )
    window_label = st.selectbox(
        "Fenêtre historique",
        options=list(TIME_WINDOWS.keys()),
        index=len(TIME_WINDOWS) - 1,
    )
    interval_sql, hours_back = TIME_WINDOWS[window_label]

    low_thresh = st.slider("Seuil pénurie critique (%)", min_value=5, max_value=25, value=10) / 100.0
    high_thresh = st.slider("Seuil saturation critique (%)", min_value=75, max_value=95, value=90) / 100.0

    st.divider()
    if st.button("🔄 Collecter un snapshot temps réel", use_container_width=True):
        with st.spinner("Interrogation de l'API CityBikes..."):
            live_df = fetch_live_snapshot(selected_cities or all_city_ids[:5])
            if not live_df.empty:
                st.success(f"{len(live_df)} relevés collectés !")
                st.cache_data.clear()
            else:
                st.error("Aucune donnée récupérée depuis CityBikes.")

    st.caption(
        "Backfill historique 5 ans (`data.citybik.es`) :\n"
        "`python -m p2_velos.backfill --years 5`"
    )

# ============================================================================
# Sélection du backend : ClickHouse (SQL pushdown complet) ou fichiers locaux
# ============================================================================
cities_tuple = tuple(selected_cities)
use_clickhouse = False
local_df = pd.DataFrame()
overview: dict[str, object] = {}

if get_warehouse().ping():
    overview = ch_overview(interval_sql, cities_tuple)
    if int(overview.get("total_releves", 0)) > 0:
        use_clickhouse = True

if not use_clickhouse:
    local_df = load_local_silver_data(selected_cities, hours_back=hours_back)
    if local_df.empty:
        st.warning(
            "Aucune donnée disponible pour les villes sélectionnées. "
            "Lancez `python -m p2_velos.backfill --years 5` ou `python -m p2_velos.flow --once --local`."
        )
        st.stop()
    latest_local = local_df.sort_values("timestamp").groupby(["ville_id", "station_id"], as_index=False).last()
    overview = {
        "total_releves": len(local_df),
        "min_ts": local_df["timestamp"].min(),
        "max_ts": local_df["timestamp"].max(),
        "nb_stations": len(latest_local),
        "total_bikes": int(latest_local["bikes_available"].sum()),
        "total_slots": int(latest_local["free_slots"].sum()),
        "avg_util_pct": round(float(latest_local["taux_utilisation"].mean()) * 100.0, 1),
    }

source_label = "ClickHouse (SQL pushdown intégral)" if use_clickhouse else "Fichiers locaux (data/silver)"
min_ts = overview["min_ts"]
max_ts = overview["max_ts"]
st.caption(
    f"Source active : **{source_label}** | "
    f"{int(overview['total_releves']):,} relevés | "
    f"Période couverte (UTC) : **{min_ts.strftime('%Y-%m-%d %H:%M')} -> {max_ts.strftime('%Y-%m-%d %H:%M')}**"
)
if window_label.startswith("5 ans"):
    st.info(
        "L'archive publique officielle CityBikes (`data.citybik.es`) débute en **novembre 2024**. "
        "La fenêtre 5 ans couvre l'intégralité de l'historique réel disponible depuis cette date "
        "complété par le flux streaming temps réel."
    )

crit_df = (
    ch_q5_critical(interval_sql, cities_tuple, low_thresh, high_thresh)
    if use_clickhouse
    else detect_critical_stations(local_df, low_threshold=low_thresh, high_threshold=high_thresh)
)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Stations actives", f"{int(overview['nb_stations']):,}")
k2.metric("Vélos disponibles", f"{int(overview['total_bikes']):,}")
k3.metric("Bornes libres", f"{int(overview['total_slots']):,}")
k4.metric("Taux d'utilisation moyen", f"{float(overview['avg_util_pct']):.1f} %")
k5.metric(
    "Stations critiques",
    f"{len(crit_df):,} ({len(crit_df) / max(int(overview['nb_stations']), 1) * 100:.1f} %)",
)

# ============================================================================
# 5 onglets correspondant aux 5 questions métier
# ============================================================================
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    [
        "1. Taux d'utilisation",
        "2. Zones sous-dotées",
        "3. Pics & Historique",
        "4. Déséquilibres géo",
        "5. Stations critiques",
    ]
)

with tab1:
    st.subheader("Q1 — Quelles stations présentent le plus fort taux d'utilisation ?")
    top_n = st.slider("Nombre de stations à afficher", 10, 50, 20, key="q1_top_n")
    top_df = (
        ch_q1_top_stations(interval_sql, cities_tuple, top_n)
        if use_clickhouse
        else top_utilisation_stations(local_df, top_n=top_n)
    )
    if not top_df.empty:
        fig_q1 = px.bar(
            top_df.sort_values("taux_utilisation_pct"),
            x="taux_utilisation_pct",
            y="station_name",
            color="ville_id",
            orientation="h",
            hover_data=["capacite_moyenne", "velos_dispo_moyens", "amplitude_velos"],
            labels={
                "taux_utilisation_pct": "Taux d'utilisation moyen (%)",
                "station_name": "Station",
                "ville_id": "Ville",
            },
            title=f"Top {top_n} stations les plus sollicitées",
            height=max(420, top_n * 24),
        )
        st.plotly_chart(fig_q1, use_container_width=True)
        st.dataframe(top_df, use_container_width=True, hide_index=True)

with tab2:
    st.subheader("Q2 — Zones où l'offre de vélos est insuffisante")
    grid_prec = st.radio(
        "Résolution de la grille spatiale",
        options=[2, 3],
        format_func=lambda p: "Quartier (~1 km)" if p == 2 else "Micro-zone (~110 m)",
        horizontal=True,
    )
    zones_df = (
        ch_q2_zones(interval_sql, cities_tuple, grid_prec)
        if use_clickhouse
        else detect_undersupplied_zones(local_df, grid_precision=grid_prec)
    )
    if not zones_df.empty:
        fig_q2 = px.scatter_map(
            zones_df.head(150),
            lat="zone_lat",
            lon="zone_lon",
            size="nb_stations",
            color="pct_temps_sous_offre",
            color_continuous_scale="YlOrRd",
            hover_name="exemple_station",
            hover_data=["ville_id", "nb_stations", "velos_dispo_moyens", "pct_temps_vide", "pct_temps_sous_offre"],
            map_style="carto-positron",
            zoom=5 if len(zones_df["ville_id"].unique()) > 1 else 12,
            height=520,
            title="Zones les plus fréquemment en sous-offre (<= 15 % de vélos disponibles)",
        )
        st.plotly_chart(fig_q2, use_container_width=True)
        st.dataframe(zones_df.head(40), use_container_width=True, hide_index=True)

with tab3:
    st.subheader("Q3 — Quels sont les pics d'utilisation journaliers du réseau ?")
    if use_clickhouse:
        peaks_df, monthly_df = ch_q3_peaks_and_monthly(interval_sql, cities_tuple)
    else:
        peaks_df = daily_usage_peaks(local_df)
        tmp_m = local_df.copy()
        tmp_m["mois"] = tmp_m["timestamp"].dt.tz_localize(None).dt.to_period("M").astype(str)
        monthly_df = (
            tmp_m.groupby(["ville_id", "mois"], as_index=False)
            .agg(
                taux_utilisation_pct=("taux_utilisation", lambda s: round(float(s.mean()) * 100, 1)),
                pct_critiques=("est_critique", lambda s: round(float(s.mean()) * 100, 1)),
                nb_releves=("timestamp", "count"),
            )
            .sort_values("mois")
        )

    if not peaks_df.empty:
        if peaks_df["heure_locale"].nunique() < 3:
            st.info(
                "Peu d'heures distinctes collectées : lancez le backfill historique "
                "`python -m p2_velos.backfill --years 5` pour obtenir le profil complet 0h-23h."
            )
        fig_q3 = px.line(
            peaks_df,
            x="heure_locale",
            y="taux_utilisation_pct",
            color="ville_id",
            line_dash="type_jour",
            markers=True,
            labels={
                "heure_locale": "Heure locale (Europe/Paris)",
                "taux_utilisation_pct": "Taux d'utilisation moyen (%)",
                "ville_id": "Ville",
                "type_jour": "Jour",
            },
            title="Profil horaire du taux d'utilisation (Semaine vs Week-end)",
        )
        fig_q3.update_xaxes(dtick=1, range=[0, 23])
        st.plotly_chart(fig_q3, use_container_width=True)

        if not monthly_df.empty and monthly_df["mois"].nunique() > 1:
            fig_month = px.line(
                monthly_df,
                x="mois",
                y="taux_utilisation_pct",
                color="ville_id",
                markers=True,
                title="Évolution mensuelle du taux d'utilisation (historique multi-annuel)",
                labels={"mois": "Mois", "taux_utilisation_pct": "Taux d'utilisation moyen (%)", "ville_id": "Ville"},
            )
            st.plotly_chart(fig_month, use_container_width=True)

        st.dataframe(peaks_df, use_container_width=True, hide_index=True)

with tab4:
    st.subheader("Q4 — Existe-t-il des déséquilibres géographiques dans la disponibilité ?")
    imb_df = (
        ch_q4_imbalance(interval_sql, cities_tuple)
        if use_clickhouse
        else geographic_imbalance(local_df, city_centers=CITY_CENTERS)
    )
    if not imb_df.empty:
        fig_q4 = px.bar(
            imb_df,
            x="secteur_geo",
            y="ecart_moyenne_ville_pts",
            color="ville_id",
            barmode="group",
            hover_data=["nb_stations", "taux_occupation_pct", "pct_temps_vide", "pct_temps_pleine"],
            labels={
                "secteur_geo": "Secteur géographique",
                "ecart_moyenne_ville_pts": "Écart d'occupation vs moyenne ville (pts %)",
                "ville_id": "Ville",
            },
            title="Déséquilibre de disponibilité entre Centre-ville et quadrants périphériques",
        )
        fig_q4.add_hline(y=0, line_dash="dash", line_color="gray")
        st.plotly_chart(fig_q4, use_container_width=True)
        st.dataframe(imb_df, use_container_width=True, hide_index=True)

with tab5:
    st.subheader("Q5 — Stations critiques nécessitant un rééquilibrage")
    if crit_df.empty:
        st.success("Aucune station critique détectée selon les seuils actuels.")
    else:
        c1, c2 = st.columns(2)
        nb_reappro = int((crit_df["action_reequilibrage"] == "REAPPROVISIONNER").sum())
        nb_deseng = int((crit_df["action_reequilibrage"] == "DESENGORGER").sum())
        c1.metric("Stations à RÉAPPROVISIONNER (pénurie)", nb_reappro)
        c2.metric("Stations à DÉSENGORGER (saturation)", nb_deseng)

        fig_q5 = px.scatter_map(
            crit_df,
            lat="latitude",
            lon="longitude",
            color="action_reequilibrage",
            size="capacity",
            color_discrete_map={
                "REAPPROVISIONNER": "#d62728",
                "DESENGORGER": "#1f77b4",
            },
            hover_name="station_name",
            hover_data=[
                "ville_id",
                "bikes_available",
                "free_slots",
                "taux_occupation_pct",
                "delta_velos_equilibre",
                "score_priorite",
            ],
            map_style="carto-positron",
            zoom=5 if len(crit_df["ville_id"].unique()) > 1 else 12,
            height=520,
            title="Carte opérationnelle des stations critiques à rééquilibrer",
        )
        st.plotly_chart(fig_q5, use_container_width=True)

        st.dataframe(crit_df, use_container_width=True, hide_index=True)
        csv_bytes = crit_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Exporter le plan de rééquilibrage (CSV)",
            data=csv_bytes,
            file_name=f"plan_reequilibrage_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
        )
