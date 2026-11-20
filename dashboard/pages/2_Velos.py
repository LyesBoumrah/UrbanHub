"""Dashboard — P2 Vélos (mobilité urbaine temps réel + historique 5 ans CityBikes).

Lit strictement les fichiers Parquet de la couche Silver (`silver/velos/` et
`silver/velos_historique/`) depuis MinIO S3 ou le disque local `data/silver/`.

Répond visuellement aux 5 questions métier :
1. Stations au plus fort taux d'utilisation
2. Zones où l'offre de vélos est insuffisante
3. Pics d'utilisation journaliers (+ tendance mensuelle multi-annuelle jusqu'à 5 ans)
4. Déséquilibres géographiques dans la disponibilité
5. Détection des stations critiques nécessitant un rééquilibrage

Responsable : P2.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from common.config import get_settings, load_ref_ville
from common.period import UI_DURATION_UNITS
from common.storage import get_storage
from p2_velos.backfill import run_backfill
from p2_velos.analytics import (
    daily_usage_peaks,
    detect_critical_stations,
    detect_undersupplied_zones,
    ensure_enriched_dataframe,
    geographic_imbalance,
    top_utilisation_stations,
)
from p2_velos.cleaning import (
    build_silver_streaming_key,
    clean_bronze_payload,
    get_bbox_map,
    save_silver_local,
    save_silver_parquet_to_storage,
)
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

TIME_WINDOWS: dict[str, int] = {
    "24 dernières heures": 24,
    "7 derniers jours": 24 * 7,
    "30 derniers jours": 24 * 30,
    "6 derniers mois": 24 * 183,
    "1 an": 24 * 365,
    "2 ans": 24 * 365 * 2,
    "5 ans (tout l'historique)": 24 * 365 * 5,
}


def _prefer_local() -> bool:
    return os.getenv("STORAGE", "local").lower() == "local"


@st.cache_data(ttl=120)
def load_silver_velos_data(
    selected_cities: tuple[str, ...],
    hours_back: int = 24 * 365 * 5,
) -> tuple[pd.DataFrame, str, int]:
    """Charge les Parquet Silver vélos depuis MinIO S3 ou le disque local `data/silver/`."""
    frames: list[pd.DataFrame] = []
    source_label = "Aucune donnée disponible"

    def _read_minio() -> tuple[list[pd.DataFrame], str]:
        out_frames: list[pd.DataFrame] = []
        try:
            st_minio = get_storage(use_local=False)
            keys = [
                k
                for prefix in ("silver/velos/", "silver/velos_historique/")
                for k in st_minio.list(prefix)
                if "velo_releve_" in k and k.endswith(".parquet")
            ]
            for key in keys:
                if "velos_historique/" in key and selected_cities:
                    parts = key.split("/")
                    if len(parts) >= 3 and parts[2] not in selected_cities:
                        continue
                out_frames.append(st_minio.get_parquet(key))
            if out_frames:
                return out_frames, "Parquet Silver MinIO S3 (s3://silver/velos*)"
        except Exception:  # noqa: BLE001
            pass
        return [], ""

    def _read_local() -> tuple[list[pd.DataFrame], str]:
        out_frames: list[pd.DataFrame] = []
        base_silver = Path(get_settings().local_data_dir) / "silver"
        stream_dir = base_silver / "velos"
        if stream_dir.exists():
            for p in sorted(stream_dir.glob("velo_releve_*.parquet")):
                out_frames.append(pd.read_parquet(p))
        hist_dir = base_silver / "velos_historique"
        if hist_dir.exists():
            for p in sorted(hist_dir.rglob("velo_releve_*.parquet")):
                if selected_cities and p.parent.name not in selected_cities:
                    continue
                out_frames.append(pd.read_parquet(p))
        if out_frames:
            return out_frames, "Parquet Silver local (data/silver/velos*)"
        return [], ""

    readers = (_read_local, _read_minio) if _prefer_local() else (_read_minio, _read_local)
    for reader in readers:
        frames, source_label = reader()
        if frames:
            break

    if not frames:
        return pd.DataFrame(), "Aucune donnée disponible", 0

    df = pd.concat(frames, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    if selected_cities:
        df = df[df["ville_id"].isin(selected_cities)]
    df = df.drop_duplicates(subset=["ville_id", "station_id", "timestamp"], keep="last")
    total_before_window = len(df)
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours_back)
    df = df[df["timestamp"] >= cutoff]
    enriched = ensure_enriched_dataframe(df)
    return enriched, source_label, total_before_window


def fetch_live_snapshot(selected_cities: list[str], use_local: bool | None = None) -> pd.DataFrame:
    """Collecte un snapshot temps réel CityBikes -> Bronze -> nettoyage -> Silver Parquet (MinIO ou local)."""
    target_local = _prefer_local() if use_local is None else use_local
    results = ingest_all_cities(cities=selected_cities, use_local_storage=target_local, pause_seconds=0.1)
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
        if target_local:
            save_silver_local(all_stations, all_releves)
        else:
            save_silver_parquet_to_storage(
                get_storage(use_local=False),
                all_stations,
                all_releves,
                releve_key=build_silver_streaming_key(now_utc=now_utc),
            )
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
        default=all_city_ids,
        format_func=lambda cid: VILLE_NOM_MAP.get(cid, cid),
    )
    window_label = st.selectbox(
        "Fenêtre historique",
        options=list(TIME_WINDOWS.keys()),
        index=len(TIME_WINDOWS) - 1,
    )
    hours_back = TIME_WINDOWS[window_label]

    low_thresh = st.slider("Seuil pénurie critique (%)", min_value=5, max_value=25, value=10) / 100.0
    high_thresh = st.slider("Seuil saturation critique (%)", min_value=75, max_value=95, value=90) / 100.0

    st.divider()
    target_mode = "local" if _prefer_local() else "MinIO"
    st.subheader("Streaming temps réel")
    if st.button(f"🔄 Collecter un snapshot temps réel ({target_mode})", use_container_width=True):
        with st.spinner(f"Interrogation de l'API CityBikes ({target_mode})..."):
            live_df = fetch_live_snapshot(selected_cities or all_city_ids, use_local=_prefer_local())
            if not live_df.empty:
                st.success(f"{len(live_df)} relevés collectés et écrits en Silver Parquet ({target_mode}) !")
                st.cache_data.clear()
                st.rerun()
            else:
                st.error("Aucune donnée récupérée depuis CityBikes.")

    st.divider()
    st.subheader("Backfill historique (data.citybik.es)")
    c_qty, c_unit = st.columns([1, 2])
    fetch_qty = int(c_qty.number_input("Durée", min_value=1, max_value=60, value=1, step=1, key="p2_qty"))
    fetch_unit = c_unit.selectbox("Unité", options=UI_DURATION_UNITS, index=1, key="p2_unit")
    fetch_duration = f"{fetch_qty} {fetch_unit}"

    if st.button(f"⬇️ Backfill CityBikes ({fetch_duration} · {target_mode})", use_container_width=True):
        with st.spinner(f"Backfill parallèle CityBikes sur {fetch_duration} ({target_mode})..."):
            bf_res = run_backfill(
                duration=fetch_duration,
                cities=selected_cities or all_city_ids,
                use_local=_prefer_local(),
            )
            if bf_res.get("dumps_total", 0) == 0:
                latest_ym = bf_res.get("latest_available_ym") or "inconnu"
                latest_fmt = f"{latest_ym[:4]}-{latest_ym[4:]}" if len(str(latest_ym)) == 6 else latest_ym
                st.warning(
                    f"Aucun dump mensuel publié sur `data.citybik.es` pour les derniers `{fetch_duration}` "
                    f"(dernier mois disponible : **{latest_fmt}**, latence de publication ~1–2 mois). "
                    "Les données récentes sont alimentées par le flux **Streaming temps réel**, "
                    "ou augmentez la durée (ex. `3 mois` ou `6 mois`) pour inclure les archives."
                )
            else:
                st.success(
                    f"{bf_res['dumps_ok']} dump(s) ingéré(s) ({bf_res['total_silver_rows']:,} relevés) "
                    f"sur `{bf_res['earliest_ym']} → {bf_res['latest_ym']}` !"
                )
                st.cache_data.clear()
                st.rerun()

    st.caption(
        "CLI durée libre :\n"
        f'`python -m p2_velos.backfill --duration "{fetch_duration}"`'
    )

# ============================================================================
# Chargement strict depuis la couche Silver Parquet
# ============================================================================
local_df, source_label, total_all_rows = load_silver_velos_data(tuple(selected_cities), hours_back=hours_back)
if local_df.empty:
    st.warning(
        "Aucune donnée Silver Parquet disponible pour les villes/fenêtre sélectionnées. "
        "Essayez d'élargir **Fenêtre historique** ou cliquez sur **Collecter un snapshot temps réel**."
    )
    st.stop()

latest_local = local_df.sort_values("timestamp").groupby(["ville_id", "station_id"], as_index=False).last()
min_ts = local_df["timestamp"].min()
max_ts = local_df["timestamp"].max()
nb_stations = len(latest_local)
total_bikes = int(latest_local["bikes_available"].sum())
total_slots = int(latest_local["free_slots"].sum())
avg_util_pct = round(float(latest_local["taux_utilisation"].mean()) * 100.0, 1)

st.caption(
    f"Source active : **{source_label}** | "
    f"{len(local_df):,} relevés affichés (sur {total_all_rows:,} en Silver) | "
    f"Période couverte (UTC) : **{min_ts.strftime('%Y-%m-%d %H:%M')} -> {max_ts.strftime('%Y-%m-%d %H:%M')}**"
)
if total_all_rows > len(local_df):
    st.info(
        f"**{total_all_rows - len(local_df):,} relevés historiques supplémentaires** existent dans Silver "
        "hors de la fenêtre sélectionnée (les archives mensuelles `data.citybik.es` ont ~1 à 2 mois de latence). "
        "Choisissez **6 derniers mois** ou **5 ans (tout l'historique)** dans la barre latérale pour les afficher."
    )
elif window_label.startswith("5 ans"):
    st.info(
        "L'archive publique officielle CityBikes (`data.citybik.es`) débute en **novembre 2024**. "
        "La fenêtre 5 ans couvre l'intégralité de l'historique réel disponible depuis cette date "
        "complété par le flux streaming temps réel."
    )

crit_df = detect_critical_stations(local_df, low_threshold=low_thresh, high_threshold=high_thresh)
crit_pct = len(crit_df) / max(nb_stations, 1) * 100.0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Stations actives", f"{nb_stations:,}")
k2.metric("Vélos disponibles", f"{total_bikes:,}")
k3.metric("Bornes libres", f"{total_slots:,}")
k4.metric("Taux d'utilisation moyen", f"{avg_util_pct:.1f} %")
k5.metric("Stations critiques", f"{len(crit_df):,}", delta=f"{crit_pct:.1f} % du réseau", delta_color="off")

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
    top_df = top_utilisation_stations(local_df, top_n=top_n)
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
    zones_df = detect_undersupplied_zones(local_df, grid_precision=grid_prec)
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
                "`python -m p2_velos.backfill --years 5 --local` pour obtenir le profil complet 0h-23h."
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
    imb_df = geographic_imbalance(local_df, city_centers=CITY_CENTERS)
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
