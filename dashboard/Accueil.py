"""Dashboard Streamlit — page d'accueil (indicateurs clés, état des pipelines).

Lancement : streamlit run dashboard/Accueil.py
Responsable : commun.
"""

from __future__ import annotations

import streamlit as st

from common.config import load_ref_ville
from common.warehouse import get_warehouse

st.set_page_config(
    page_title="UrbanHub — Jumeau numérique urbain",
    page_icon="🏙️",
    layout="wide",
)

st.title("🏙️ UrbanHub — Jumeau numérique urbain")
st.markdown(
    "Plateforme Big Data de suivi urbain (**Météo P1**, **Mobilité Vélos P2**, "
    "**Pollution P3**, **Analyse croisée P4**)."
)

villes = load_ref_ville()
warehouse = get_warehouse()
ch_ok = warehouse.ping()

col1, col2, col3 = st.columns(3)
col1.metric("Villes configurées", len(villes))
col2.metric("Villes cœur UrbanHub", sum(1 for v in villes if v.get("core_urbanhub")))
col3.metric("Entrepôt ClickHouse", "Connecté" if ch_ok else "Mode local / hors ligne")

st.subheader("Réseaux couverts")
st.dataframe(
    [
        {
            "Ville": v["nom"],
            "ID": v["ville_id"],
            "Cœur UrbanHub": "Oui" if v.get("core_urbanhub") else "Extension P2",
            "Réseau CityBikes": (v.get("citybikes") or {}).get("network_name", "—"),
            "Network ID": (v.get("citybikes") or {}).get("network_id", "—"),
        }
        for v in villes
    ],
    use_container_width=True,
    hide_index=True,
)

if ch_ok:
    st.subheader("Dernières alertes pipeline")
    try:
        df_alerts = warehouse.query_df(
            "SELECT ts_utc, pipeline, ville_id, niveau, message "
            "FROM urbanhub.pipeline_alertes ORDER BY ts_utc DESC LIMIT 20"
        )
        if df_alerts.empty:
            st.success("Aucune alerte récente enregistrée.")
        else:
            st.dataframe(df_alerts, use_container_width=True, hide_index=True)
    except Exception as exc:
        st.info(f"Table `pipeline_alertes` non encore initialisée : {exc}")
else:
    st.info(
        "ClickHouse n'est pas joignable localement. Vous pouvez tout de même explorer "
        "la page **2_Velos** en mode local (fichiers Parquet) ou via une collecte temps réel."
    )
