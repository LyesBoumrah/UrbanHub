"""Dashboard — P3 Pollution urbaine (flux IoT capteurs OpenAQ v3).

Lit les fichiers Parquet Silver (`silver/pollution/air_mesure.parquet`) depuis MinIO ou le disque local.

Répond visuellement aux 4 questions métier :
  1. Quelles zones présentent les niveaux de pollution les plus élevés ?
  2. Existe-t-il une variation journalière de la pollution ?
  3. Quels polluants sont les plus dominants dans la ville étudiée ?
  4. Peut-on identifier des épisodes de pollution anormale ?

Responsable : P3.
"""

from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import streamlit as st

from common.config import load_ref_ville
from common.period import UI_DURATION_UNITS
from p3_pollution.analytics import (
    city_pollution_ranking,
    detect_pollution_episodes,
    diurnal_pollution_profile,
    dominant_pollutants_by_city,
    ensure_enriched_pollution_df,
    most_polluted_zones,
)
from p3_pollution.cleaning import MESURE_SILVER_KEY, clean_all
from p3_pollution.flow import p3_pollution_flow
from p3_pollution.quality import POLLUTANT_LABELS, WHO_THRESHOLDS_UGM3
from p3_pollution.storage import storage_from_env

st.set_page_config(
    page_title="UrbanHub — P3 Pollution IoT",
    page_icon="🌫️",
    layout="wide",
)

st.title("🌫️ P3 — Pollution atmosphérique urbaine (Capteurs IoT OpenAQ v3)")
st.caption(
    "Flux IoT horaire (`/locations/{id}/latest` & `/sensors/{id}/hours`) — "
    "Polluants suivis : PM2.5, PM10, NO2, O3, CO (unités harmonisées en µg/m³)"
)

REF_VILLES = load_ref_ville()
VILLE_NOM_MAP = {v["ville_id"]: v["nom"] for v in REF_VILLES}


def _prefer_local() -> bool:
    return os.getenv("STORAGE", "local").lower() == "local"


@st.cache_data(ttl=180)
def load_pollution_data() -> tuple[pd.DataFrame, str]:
    """Charge les mesures silver de pollution depuis ClickHouse Lakehouse ou Parquet Silver (MinIO / local)."""
    if not _prefer_local():
        try:
            from common.warehouse import try_get_warehouse

            wh = try_get_warehouse()
            if wh is not None:
                df = wh.query_df("SELECT * FROM urbanhub.air_mesure FINAL WHERE is_valid = 1")
                if not df.empty:
                    return ensure_enriched_pollution_df(df), "ClickHouse Lakehouse (urbanhub.air_mesure)"
        except Exception:  # noqa: BLE001
            pass

    order = (True, False) if _prefer_local() else (False, True)
    for use_local in order:
        try:
            st_backend = storage_from_env(use_local=use_local)
            if st_backend.exists(MESURE_SILVER_KEY):
                df = st_backend.read_parquet(MESURE_SILVER_KEY)
                if not df.empty:
                    label = "Parquet Silver local" if use_local else "Parquet Silver MinIO S3 (s3://silver)"
                    return ensure_enriched_pollution_df(df), f"{label} ({MESURE_SILVER_KEY})"
        except Exception:  # noqa: BLE001
            continue

    return pd.DataFrame(), "Aucune donnée disponible"


df_all, backend_label = load_pollution_data()

with st.sidebar:
    st.header("Paramètres P3 Pollution")
    all_city_ids = [v["ville_id"] for v in REF_VILLES]
    avail_cities = sorted(df_all["ville_id"].unique()) if not df_all.empty else all_city_ids
    selected_cities = st.multiselect(
        "Villes françaises",
        options=avail_cities,
        default=avail_cities,
        format_func=lambda cid: VILLE_NOM_MAP.get(cid, cid),
    )

    avail_polls = list(POLLUTANT_LABELS.keys())
    selected_polls = st.multiselect(
        "Polluants",
        options=avail_polls,
        default=avail_polls,
        format_func=lambda p: POLLUTANT_LABELS.get(p, p.upper()),
    )

    z_ep_thresh = st.slider("Seuil z-score épisodes anormaux (Q4)", min_value=1.2, max_value=3.0, value=1.8, step=0.1)
    min_ep_hours = st.slider("Durée min épisode (heures)", min_value=2, max_value=12, value=3)

    st.divider()
    st.subheader("Durée de récupération")
    c_qty, c_unit = st.columns([1, 2])
    fetch_qty = int(c_qty.number_input("Durée", min_value=1, max_value=60, value=1, step=1, key="p3_qty"))
    fetch_unit = c_unit.selectbox("Unité", options=UI_DURATION_UNITS, index=1, key="p3_unit")
    fetch_duration = f"{fetch_qty} {fetch_unit}"

    target_mode = "local" if _prefer_local() else "MinIO"
    if st.button(f"🔄 Collecter flux IoT temps réel ({target_mode})", use_container_width=True):
        with st.spinner(f"Exécution du pipeline IoT P3 (latest · {target_mode})..."):
            p3_pollution_flow(mode="latest", use_local=_prefer_local())
            clean_all(use_local=_prefer_local())
            st.cache_data.clear()
            st.rerun()

    if st.button(f"⬇️ Historique OpenAQ ({fetch_duration} · {target_mode})", use_container_width=True):
        with st.spinner(f"Backfill parallèle OpenAQ sur {fetch_duration} ({target_mode})..."):
            p3_pollution_flow(mode="backfill", duration=fetch_duration, use_local=_prefer_local())
            st.cache_data.clear()
            st.rerun()

    st.caption(
        f'CLI : `python -m p3_pollution.flow --backfill --duration "{fetch_duration}"`\n\n'
        "Seuils journaliers OMS 2021 :\n"
        "- PM2.5 : 15 µg/m³ | PM10 : 45 µg/m³\n"
        "- NO2 : 25 µg/m³ | O3 : 100 µg/m³\n"
        "- CO : 4 000 µg/m³"
    )

if df_all.empty:
    st.warning(
        "Aucune mesure de pollution Silver n'est disponible. Lancez `python -m p3_pollution.flow --local` "
        "ou cliquez sur le bouton dans la barre latérale."
    )
    st.stop()

df = df_all.copy()
if selected_cities:
    df = df[df["ville_id"].isin(selected_cities)]
if selected_polls:
    df = df[df["pollutant"].isin(selected_polls)]

if df.empty:
    st.info("Aucune mesure pour les filtres sélectionnés.")
    st.stop()

df["Ville"] = df["ville_id"].map(lambda c: VILLE_NOM_MAP.get(c, c))

# ============================================================================
# Bandeau KPIs
# ============================================================================
st.caption(f"Source active : **{backend_label}**")
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Mesures capteurs IoT", f"{len(df):,}".replace(",", " "))
k2.metric("Capteurs actifs", df["sensor_id"].nunique())
k3.metric("Sites de mesure", df["location_id"].nunique())
k4.metric("Ratio OMS moyen", f"{df['ratio_oms'].mean():.2f}x")
k5.metric("Taux dépassement OMS", f"{df['depasse_oms'].mean() * 100:.1f} %")

tab_q1, tab_q2, tab_q3, tab_q4 = st.tabs([
    "📍 Q1 — Zones les plus polluées",
    "🕒 Q2 — Variation journalière",
    "🧪 Q3 — Polluants dominants",
    "🚨 Q4 — Épisodes anormaux",
])

# ============================================================================
# Q1 : Zones présentant les niveaux de pollution les plus élevés
# ============================================================================
with tab_q1:
    st.subheader("Q1 — Identification des zones et stations aux niveaux de pollution les plus élevés")
    pol_filter_q1 = st.selectbox(
        "Filtrer le classement spatial par polluant",
        options=["tous"] + list(POLLUTANT_LABELS.keys()),
        format_func=lambda p: (
            "Tous polluants (indice normalisé OMS)"
            if p == "tous"
            else f"{POLLUTANT_LABELS[p]} (seuil OMS : {WHO_THRESHOLDS_UGM3[p]} µg/m³)"
        ),
    )
    zones = most_polluted_zones(df, pollutant=None if pol_filter_q1 == "tous" else pol_filter_q1, top_n=50)
    city_rank = city_pollution_ranking(df)

    c_map, c_bar = st.columns([3, 2])
    with c_map:
        if not zones.empty:
            zones["Ville"] = zones["ville_id"].map(lambda c: VILLE_NOM_MAP.get(c, c))
            fig_map = px.scatter_mapbox(
                zones,
                lat="latitude",
                lon="longitude",
                color="ratio_oms_moyen",
                size="concentration_p90",
                hover_name="location_name",
                hover_data=["Ville", "pollutant_label", "concentration_moyenne", "pct_depassement_oms"],
                color_continuous_scale="YlOrRd",
                zoom=5,
                center={"lat": 46.6, "lon": 2.4},
                mapbox_style="carto-positron",
                title="Carte des stations IoT selon leur ratio de dépassement OMS",
            )
            st.plotly_chart(fig_map, use_container_width=True)
    with c_bar:
        if not city_rank.empty:
            city_rank["Ville"] = city_rank["ville_id"].map(lambda c: VILLE_NOM_MAP.get(c, c))
            fig_city = px.bar(
                city_rank.sort_values("ratio_oms_moyen", ascending=True),
                x="ratio_oms_moyen",
                y="Ville",
                orientation="h",
                color="pct_depassement_oms",
                color_continuous_scale="Reds",
                title="Classement des villes par ratio OMS moyen",
                labels={"ratio_oms_moyen": "Ratio moyen / seuil OMS", "pct_depassement_oms": "% dépassement OMS"},
            )
            fig_city.add_vline(x=1.0, line_dash="dash", line_color="red")
            st.plotly_chart(fig_city, use_container_width=True)

    if not zones.empty:
        st.dataframe(
            zones[[
                "Ville", "location_name", "pollutant_label", "concentration_moyenne",
                "concentration_p90", "concentration_max", "ratio_oms_moyen", "pct_depassement_oms", "nb_mesures",
            ]],
            use_container_width=True,
            hide_index=True,
        )

# ============================================================================
# Q2 : Variation journalière de la pollution
# ============================================================================
with tab_q2:
    st.subheader("Q2 — Variation journalière (cycle diurne 0h–23h, Semaine vs Week-end)")
    prof = diurnal_pollution_profile(df)
    if prof.empty:
        st.info("Données insuffisantes pour le profil horaire.")
    else:
        c_p2, c_v2 = st.columns(2)
        pol_q2 = c_p2.selectbox(
            "Polluant à visualiser (Q2)",
            options=sorted(prof["pollutant"].unique()),
            format_func=lambda p: POLLUTANT_LABELS.get(p, p.upper()),
        )
        city_q2 = c_v2.selectbox(
            "Périmètre géographique (Q2)",
            options=["toutes"] + sorted(prof["ville_id"].unique()),
            format_func=lambda c: "🇫🇷 Toutes les villes sélectionnées" if c == "toutes" else VILLE_NOM_MAP.get(c, c),
        )

        sub_p2 = prof[prof["pollutant"] == pol_q2]
        if city_q2 != "toutes":
            sub_p2 = sub_p2[sub_p2["ville_id"] == city_q2]
        sub_agg = (
            sub_p2.groupby(["type_jour", "heure_locale"], as_index=False)
            .agg(
                concentration_moyenne=("concentration_moyenne", "mean"),
                concentration_p90=("concentration_p90", "mean"),
            )
        )

        pol_title = POLLUTANT_LABELS.get(pol_q2, pol_q2)
        fig_diurne = px.line(
            sub_agg,
            x="heure_locale",
            y="concentration_moyenne",
            color="type_jour",
            markers=True,
            title=f"Profil journalier horaire de {pol_title} (µg/m³, heure locale Europe/Paris)",
            labels={
                "heure_locale": "Heure locale (0h–23h)",
                "concentration_moyenne": "Concentration moyenne (µg/m³)",
                "type_jour": "Type de jour",
            },
        )
        fig_diurne.add_hline(
            y=WHO_THRESHOLDS_UGM3.get(pol_q2, 25.0),
            line_dash="dot",
            line_color="red",
            annotation_text=f"Seuil OMS ({WHO_THRESHOLDS_UGM3.get(pol_q2)} µg/m³)",
        )
        st.plotly_chart(fig_diurne, use_container_width=True)

        st.info(
            "**Interprétation métier (Q2)** : On observe deux signatures horaires opposées :\n"
            "- **Polluants primaires liés au trafic et au chauffage (NO2, PM2.5, PM10, CO)** : "
            "**double pic journalier** aux heures de pointe (07h–09h et 18h–21h), "
            "nettement plus prononcé en semaine que le week-end.\n"
            "- **Ozone (O3, polluant secondaire photochimique)** : creux matinal (titration par le NO du trafic) "
            "et **pic unique l'après-midi entre 14h et 17h** sous l'effet du rayonnement solaire."
        )

# ============================================================================
# Q3 : Polluants dominants dans chaque ville
# ============================================================================
with tab_q3:
    st.subheader("Q3 — Polluants dominants dans chaque ville étudiée")
    dom = dominant_pollutants_by_city(df)
    par_pol = dom["par_polluant"]
    synth = dom["synthese_ville"]

    if not par_pol.empty:
        par_pol["Ville"] = par_pol["ville_id"].map(lambda c: VILLE_NOM_MAP.get(c, c))
        c_left, c_right = st.columns(2)
        with c_left:
            fig_dom = px.bar(
                par_pol,
                x="Ville",
                y="pct_heures_dominant",
                color="pollutant_label",
                barmode="stack",
                title="Répartition (%) des heures où chaque polluant est le plus critique (ratio OMS max)",
                labels={"pct_heures_dominant": "% des heures dominant", "pollutant_label": "Polluant"},
            )
            st.plotly_chart(fig_dom, use_container_width=True)
        with c_right:
            fig_ratio = px.bar(
                par_pol,
                x="Ville",
                y="ratio_oms_moyen",
                color="pollutant_label",
                barmode="group",
                title="Ratio moyen par rapport au seuil sanitaire OMS 2021 (1.0 = seuil OMS)",
                labels={"ratio_oms_moyen": "Ratio Concentration / Seuil OMS", "pollutant_label": "Polluant"},
            )
            fig_ratio.add_hline(y=1.0, line_dash="dash", line_color="red")
            st.plotly_chart(fig_ratio, use_container_width=True)

    if not synth.empty:
        synth["Ville"] = synth["ville_id"].map(lambda c: VILLE_NOM_MAP.get(c, c))
        st.markdown("**Synthèse du polluant dominant par métropole** :")
        st.dataframe(
            synth[[
                "Ville", "polluant_dominant_oms", "pct_heures_dominant",
                "ratio_oms_dominant", "polluant_dominant_masse_hors_co", "concentration_moyenne_ugm3",
            ]],
            use_container_width=True,
            hide_index=True,
        )

# ============================================================================
# Q4 : Épisodes de pollution anormale
# ============================================================================
with tab_q4:
    st.subheader("Q4 — Détection des épisodes de pollution anormale")
    episodes = detect_pollution_episodes(df, z_threshold=z_ep_thresh, min_consecutive_hours=min_ep_hours)
    if episodes.empty:
        st.info("Aucun épisode de pollution anormale détecté avec les paramètres actuels.")
    else:
        episodes["Ville"] = episodes["ville_id"].map(lambda c: VILLE_NOM_MAP.get(c, c))
        fig_ep = px.scatter(
            episodes,
            x="debut_utc",
            y="ratio_oms_pic",
            color="pollutant_label",
            size="duree_heures",
            symbol="severite",
            hover_data=[
                "Ville", "duree_heures", "concentration_moyenne", "concentration_pic", "z_score_pic",
            ],
            title="Chronologie des épisodes de pollution anormale (taille = durée en heures)",
            labels={
                "debut_utc": "Début de l'épisode (UTC)",
                "ratio_oms_pic": "Ratio OMS au pic",
                "pollutant_label": "Polluant",
            },
        )
        fig_ep.add_hline(y=1.0, line_dash="dot", line_color="orange")
        fig_ep.add_hline(y=2.0, line_dash="dash", line_color="red")
        st.plotly_chart(fig_ep, use_container_width=True)

        st.markdown(f"**{len(episodes)} épisodes continus de pollution anormale détectés** :")
        cols_ep = [
            "Ville", "pollutant_label", "severite", "debut_utc", "fin_utc",
            "duree_heures", "concentration_moyenne", "concentration_pic", "ratio_oms_pic", "z_score_pic",
        ]
        st.dataframe(episodes[cols_ep], use_container_width=True, hide_index=True)
        st.download_button(
            "📥 Exporter les épisodes de pollution (CSV)",
            data=episodes[cols_ep].to_csv(index=False).encode("utf-8"),
            file_name="urbanhub_p3_episodes_pollution.csv",
            mime="text/csv",
        )
