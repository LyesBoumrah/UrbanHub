"""Dashboard — P1 Météo (flux batch Open-Meteo : 5 ans + données récentes).

Lit les fichiers Parquet Silver (`silver/meteo/meteo_obs.parquet`) depuis MinIO ou le disque local.

Répond visuellement aux 4 questions métier :
  1. Peut-on identifier des périodes météorologiques anormales ?
  2. Existe-t-il une corrélation entre conditions météorologiques et visibilité ?
  3. Quelle est l'évolution saisonnière de la température en France et dans les principales villes ?
  4. Quels sont les jours présentant des conditions météorologiques extrêmes ?

Responsable : P1.
"""

from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import streamlit as st

from common.config import load_ref_ville
from common.period import UI_DURATION_UNITS
from common.storage import get_storage
from p1_meteo.analytics import (
    anomalous_periods,
    daily_aggregates,
    daily_anomalies,
    extreme_days,
    national_anomaly_series,
    seasonal_temperature,
    visibility_by_class,
    visibility_correlations,
)
from p1_meteo.cleaning import (
    SILVER_KEY,
    STATION_SILVER_KEY,
    aggregate_ville_heure,
    clean_all,
    load_silver,
)
from p1_meteo.ingestion import ingest

st.set_page_config(
    page_title="UrbanHub — P1 Météo Urbaine",
    page_icon="🌦️",
    layout="wide",
)

st.title("🌦️ P1 — Analyse météorologique urbaine (Open-Meteo : 5 ans + données récentes)")
st.caption(
    "Flux Batch : téléchargement parallèle des stations météo françaises sur 5 ans jusqu'à aujourd'hui "
    "(`archive-api.open-meteo.com` + `api.open-meteo.com/v1/forecast` pour les données récentes)"
)

REF_VILLES = load_ref_ville()
VILLE_NOM_MAP = {v["ville_id"]: v["nom"] for v in REF_VILLES}
MOIS_NOMS = {
    1: "Jan", 2: "Fév", 3: "Mar", 4: "Avr", 5: "Mai", 6: "Juin",
    7: "Juil", 8: "Août", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Déc",
}
VAR_LABELS = {
    "tmoy": "Température moyenne (°C)",
    "precip_jour": "Précipitations journalières (mm/j)",
    "vent_moy": "Vitesse du vent moyenne (m/s)",
    "pression_moy": "Pression atmosphérique (hPa)",
}


def _prefer_local() -> bool:
    return os.getenv("STORAGE", "local").lower() == "local"


@st.cache_data(ttl=300)
def load_meteo_data() -> tuple[pd.DataFrame, str]:
    """Charge l'agrégat horaire par ville depuis les fichiers Parquet Silver (MinIO ou local)."""
    order = (True, False) if _prefer_local() else (False, True)
    for use_local in order:
        try:
            storage = get_storage(use_local=use_local)
            obs = load_silver(storage=storage, use_local=use_local)
            if not obs.empty:
                st_df = storage.get_parquet(STATION_SILVER_KEY) if storage.exists(STATION_SILVER_KEY) else None
                agg = aggregate_ville_heure(obs, st_df)
                if not agg.empty:
                    agg["heure_utc"] = pd.to_datetime(agg["heure_utc"], utc=True)
                    label = "Parquet Silver local" if use_local else "Parquet Silver MinIO S3 (s3://silver)"
                    return agg, f"{label} ({SILVER_KEY})"
        except Exception:  # noqa: BLE001
            continue

    return pd.DataFrame(), "Aucune donnée disponible"


df_all, backend_label = load_meteo_data()

with st.sidebar:
    st.header("Paramètres P1 Météo")
    all_city_ids = [v["ville_id"] for v in REF_VILLES]
    available_cities = sorted(df_all["ville_id"].unique()) if not df_all.empty else all_city_ids
    selected_cities = st.multiselect(
        "Villes françaises",
        options=available_cities,
        default=available_cities,
        format_func=lambda cid: VILLE_NOM_MAP.get(cid, cid),
    )

    if not df_all.empty:
        years_avail = sorted(pd.to_datetime(df_all["heure_utc"], utc=True).dt.year.unique())
        selected_years = st.multiselect("Années", options=years_avail, default=years_avail)
    else:
        selected_years = []

    z_thresh = st.slider("Seuil d'anomalie |z-score| (Q1)", min_value=1.0, max_value=3.0, value=1.5, step=0.1)
    min_ep_days = st.slider("Durée minimale d'un épisode (jours)", min_value=2, max_value=7, value=3)

    st.divider()
    st.subheader("Durée de récupération")
    c_qty, c_unit = st.columns([1, 2])
    fetch_qty = int(c_qty.number_input("Durée", min_value=1, max_value=60, value=1, step=1, key="p1_qty"))
    fetch_unit = c_unit.selectbox("Unité", options=UI_DURATION_UNITS, index=1, key="p1_unit")
    fetch_duration = f"{fetch_qty} {fetch_unit}"

    target_mode = "local" if _prefer_local() else "MinIO"
    if st.button(f"⬇️ Récupérer Open-Meteo ({fetch_duration} · {target_mode})", use_container_width=True):
        with st.spinner(f"Téléchargement parallèle Open-Meteo sur {fetch_duration} ({target_mode})..."):
            ingest(duration=fetch_duration, max_workers=6, use_local=_prefer_local(), skip_existing=False)
            clean_all(use_local=_prefer_local())
            st.cache_data.clear()
            st.rerun()

    st.caption(
        "CLI durée libre :\n"
        f'`python -m p1_meteo.flow --duration "{fetch_duration}"`'
    )

if df_all.empty:
    st.warning(
        "Aucune donnée météo Silver n'est encore présente. Cliquez sur **Télécharger & nettoyer Open-Meteo (local)** "
        "dans la barre latérale ou exécutez `python -m p1_meteo.flow --local`."
    )
    st.stop()

df = df_all.copy()
if selected_cities:
    df = df[df["ville_id"].isin(selected_cities)]
if selected_years:
    df = df[pd.to_datetime(df["heure_utc"], utc=True).dt.year.isin(selected_years)]

if df.empty:
    st.info("Aucune observation pour les filtres sélectionnés.")
    st.stop()

df["ville_nom"] = df["ville_id"].map(lambda c: VILLE_NOM_MAP.get(c, c))
daily = daily_aggregates(df)
anom = daily_anomalies(daily)

# ============================================================================
# Bandeau KPIs
# ============================================================================
st.caption(f"Source active : **{backend_label}**")
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Relevés horaires (ville×h)", f"{len(df):,}".replace(",", " "))
k2.metric("Villes analysées", df["ville_id"].nunique())
k3.metric("Température moyenne", f"{df['temperature'].mean():.1f} °C")
k4.metric("Vent maximal horaire", f"{df['wind_speed'].max():.1f} m/s")
pct_fog = float((df["visibility"] < 1000).mean() * 100) if df["visibility"].notna().any() else 0.0
k5.metric("Fréquence de brouillard (<1 km)", f"{pct_fog:.2f} %")

tab_q1, tab_q2, tab_q3, tab_q4 = st.tabs([
    "🌡️ Q1 — Périodes anormales",
    "🌫️ Q2 — Météo & Visibilité",
    "📅 Q3 — Évolution saisonnière",
    "⚡ Q4 — Jours extrêmes",
])

# ============================================================================
# Q1 : Périodes météorologiques anormales
# ============================================================================
with tab_q1:
    st.subheader("Q1 — Identification des périodes météorologiques anormales")
    st.markdown(
        "Les anomalies journalières sont mesurées par leur **z-score standardisé** par rapport à la "
        "normale saisonnière locale (±15 jours autour du jour de l'année). Un épisode anormal correspond "
        f"à au moins **{min_ep_days} jours consécutifs** avec $|z| \\ge {z_thresh}$."
    )

    c_var, c_city = st.columns([1, 1])
    target_var = c_var.selectbox(
        "Variable analysée", options=list(VAR_LABELS.keys()), format_func=lambda k: VAR_LABELS[k]
    )
    city_q1 = c_city.selectbox(
        "Série chronologique",
        options=["france_moyenne"] + sorted(df["ville_id"].unique()),
        format_func=lambda c: (
            "🇫🇷 Moyenne France (toutes villes)" if c == "france_moyenne" else VILLE_NOM_MAP.get(c, c)
        ),
    )

    if city_q1 == "france_moyenne" and target_var == "tmoy":
        nat = national_anomaly_series(anom)
        nat["Type"] = nat["z_tmoy"].apply(
            lambda z: "Anomalie chaude (z > 0)" if z >= 0 else "Anomalie froide (z < 0)"
        )
        fig_anom = px.bar(
            nat,
            x="jour",
            y="z_tmoy",
            color="Type",
            color_discrete_map={"Anomalie chaude (z > 0)": "#d95f02", "Anomalie froide (z < 0)": "#1f78b4"},
            title="Anomalie thermique journalière standardisée (z-score moyen France)",
            labels={"jour": "Date", "z_tmoy": "Z-score de température"},
        )
        fig_anom.add_hline(y=z_thresh, line_dash="dash", line_color="#d95f02")
        fig_anom.add_hline(y=-z_thresh, line_dash="dash", line_color="#1f78b4")
        st.plotly_chart(fig_anom, use_container_width=True)
    else:
        sub_city = anom if city_q1 == "france_moyenne" else anom[anom["ville_id"] == city_q1]
        z_col = f"z_{target_var}"
        sub_plot = sub_city.groupby("jour", as_index=False)[[target_var, f"{target_var}_clim", z_col]].mean()
        fig_anom = px.line(
            sub_plot,
            x="jour",
            y=[target_var, f"{target_var}_clim"],
            title=f"{VAR_LABELS[target_var]} vs normale saisonnière ({VILLE_NOM_MAP.get(city_q1, 'France')})",
            labels={"jour": "Date", "value": VAR_LABELS[target_var], "variable": "Série"},
        )
        st.plotly_chart(fig_anom, use_container_width=True)

    periods = anomalous_periods(anom, var=target_var, z_threshold=z_thresh, min_days=min_ep_days)
    if not periods.empty:
        periods["Ville"] = periods["ville_id"].map(lambda c: VILLE_NOM_MAP.get(c, c))
        st.markdown(f"**{len(periods)} épisodes anormaux détectés** sur la période :")
        st.dataframe(
            periods[[
                "Ville", "type", "debut", "fin", "nb_jours",
                "valeur_moyenne", "normale_moyenne", "z_moyen", "z_max",
            ]],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("Aucun épisode anormal détecté avec ces seuils.")

# ============================================================================
# Q2 : Corrélation entre conditions météo et visibilité
# ============================================================================
with tab_q2:
    st.subheader("Q2 — Relation entre conditions météorologiques et visibilité horizontale")
    corrs = visibility_correlations(df, method="spearman")
    by_cls = visibility_by_class(df)

    col_left, col_right = st.columns(2)
    with col_left:
        if not corrs.empty:
            fig_corr = px.bar(
                corrs,
                x="correlation",
                y="variable",
                orientation="h",
                color="correlation",
                color_continuous_scale="RdBu",
                range_color=[-1, 1],
                title="Corrélation de rangs de Spearman avec la visibilité (m)",
                labels={"correlation": "Coefficient de Spearman (ρ)", "variable": "Variable météo"},
            )
            st.plotly_chart(fig_corr, use_container_width=True)

    with col_right:
        hum_df = by_cls.get("humidite", pd.DataFrame())
        if not hum_df.empty:
            fig_hum = px.bar(
                hum_df,
                x="relative_humidity",
                y="pct_brouillard",
                text="pct_brouillard",
                color="visibilite_mediane",
                color_continuous_scale="Viridis_r",
                title="Fréquence de brouillard (<1000 m) par classe d'humidité relative (%)",
                labels={
                    "relative_humidity": "Classe d'humidité relative (%)",
                    "pct_brouillard": "% d'heures avec brouillard (<1 km)",
                    "visibilite_mediane": "Visibilité médiane (m)",
                },
            )
            st.plotly_chart(fig_hum, use_container_width=True)

    c_pluie, c_vent = st.columns(2)
    with c_pluie:
        pluie_df = by_cls.get("pluie", pd.DataFrame())
        if not pluie_df.empty:
            fig_pluie = px.bar(
                pluie_df,
                x="precipitation",
                y="visibilite_mediane",
                color="pct_brouillard",
                title="Visibilité médiane (m) selon l'intensité des précipitations",
                labels={"precipitation": "Intensité de pluie (mm/h)", "visibilite_mediane": "Visibilité médiane (m)"},
            )
            st.plotly_chart(fig_pluie, use_container_width=True)
    with c_vent:
        vent_df = by_cls.get("vent", pd.DataFrame())
        if not vent_df.empty:
            fig_vent = px.bar(
                vent_df,
                x="wind_speed",
                y="pct_brouillard",
                color="visibilite_mediane",
                title="Dispersion du brouillard selon la vitesse du vent",
                labels={"wind_speed": "Classe de vent (m/s)", "pct_brouillard": "% d'heures avec brouillard (<1 km)"},
            )
            st.plotly_chart(fig_vent, use_container_width=True)

    st.info(
        "**Interprétation métier (Q2)** : L'humidité relative est le déterminant majeur de la visibilité "
        "(chute brutale dès que HR > 95 %, où la condensation forme brume et brouillard radiatif). "
        "Les précipitations réduisent la visibilité médiane par extinction optique des gouttes, tandis "
        "qu'un vent modéré (> 3 m/s) brasse la couche limite et divise fortement le risque de brouillard."
    )

# ============================================================================
# Q3 : Évolution saisonnière de la température
# ============================================================================
with tab_q3:
    st.subheader("Q3 — Évolution saisonnière de la température en France et par métropole")
    seas = seasonal_temperature(df)
    clim_ville = seas["climatologie_ville"].copy()
    clim_ville["Ville"] = clim_ville["ville_id"].map(lambda c: VILLE_NOM_MAP.get(c, c))
    clim_ville["Mois"] = clim_ville["mois"].map(MOIS_NOMS)

    fig_clim = px.line(
        clim_ville,
        x="Mois",
        y="mean",
        color="Ville",
        markers=True,
        category_orders={"Mois": list(MOIS_NOMS.values())},
        title="Cycle annuel moyen de la température (°C) dans les principales villes de France",
        labels={"mean": "Température moyenne (°C)", "Mois": "Mois de l'année"},
    )
    st.plotly_chart(fig_clim, use_container_width=True)

    c_fr, c_amp = st.columns([3, 2])
    with c_fr:
        fr_m = seas["mensuel_france"].copy()
        fr_m["Mois"] = fr_m["mois"].map(MOIS_NOMS)
        fr_m["Année"] = fr_m["annee"].astype(str)
        fig_fr = px.line(
            fr_m,
            x="Mois",
            y="temperature",
            color="Année",
            markers=True,
            category_orders={"Mois": list(MOIS_NOMS.values())},
            title="Évolution mensuelle de la température moyenne en France par année",
            labels={"temperature": "Température moyenne France (°C)"},
        )
        st.plotly_chart(fig_fr, use_container_width=True)
    with c_amp:
        amp = seas["amplitude"].copy()
        amp["Ville"] = amp["ville_id"].map(lambda c: VILLE_NOM_MAP.get(c, c))
        amp = amp.sort_values("amplitude_annuelle", ascending=True)
        fig_amp = px.bar(
            amp,
            x="amplitude_annuelle",
            y="Ville",
            orientation="h",
            color="amplitude_annuelle",
            color_continuous_scale="YlOrRd",
            title="Amplitude thermique annuelle (mois le plus chaud - mois le plus froid, °C)",
            labels={"amplitude_annuelle": "Amplitude annuelle (°C)"},
        )
        st.plotly_chart(fig_amp, use_container_width=True)

    st.info(
        "**Interprétation métier (Q3)** : Les profils thermiques distinguent trois régimes climatiques : "
        "**continental** (Strasbourg, Lyon, Clermont-Ferrand : forte amplitude > 16 °C), "
        "**océanique** (Nantes, Rennes, Rouen, Lille : amplitude modérée 11–14 °C), et "
        "**méditerranéen** (Marseille, Montpellier : hivers doux ~8 °C et étés très chauds > 24 °C)."
    )

# ============================================================================
# Q4 : Jours aux conditions météorologiques extrêmes
# ============================================================================
with tab_q4:
    st.subheader("Q4 — Identification des jours aux conditions météorologiques extrêmes")
    ext = extreme_days(daily)
    if ext.empty:
        st.info("Aucun jour extrême détecté sur la sélection.")
    else:
        ext["Ville"] = ext["ville_id"].map(lambda c: VILLE_NOM_MAP.get(c, c))
        fig_ext = px.scatter(
            ext,
            x="jour",
            y="tmax",
            color="Ville",
            size="score_extreme",
            hover_data=["tmin", "vent_max", "precip_jour", "visibilite_min", "criteres"],
            title="Calendrier des journées météorologiques extrêmes (taille = nombre de critères franchis)",
            labels={"jour": "Date", "tmax": "Température maximale journalière (°C)"},
        )
        st.plotly_chart(fig_ext, use_container_width=True)

        st.markdown(
            f"**{len(ext)} journées extrêmes identifiées** "
            "(seuils de vigilance Météo-France ou quantiles p99/p01 locaux) :"
        )
        display_cols = [
            "Ville", "jour", "score_extreme", "criteres",
            "tmin", "tmax", "vent_max", "precip_jour", "visibilite_min", "pression_min",
        ]
        st.dataframe(ext[display_cols].head(200), use_container_width=True, hide_index=True)
        st.download_button(
            "📥 Télécharger les jours extrêmes (CSV)",
            data=ext[display_cols].to_csv(index=False).encode("utf-8"),
            file_name="urbanhub_p1_jours_extremes.csv",
            mime="text/csv",
        )
