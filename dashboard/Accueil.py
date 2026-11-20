"""Dashboard Streamlit — page d'accueil (indicateurs clés, état du Data Lake MinIO / local).

Architecture 100 % Data Lake :
  - Bronze (`bronze/`) : données brutes issues des APIs
  - Silver (`silver/`) : fichiers Parquet nettoyés issus de la couche bronze
  - Gold (`gold/`)     : fichiers JSON métier répondant aux questions P1 à P4

Lancement : streamlit run dashboard/Accueil.py
Responsable : commun.
"""

from __future__ import annotations

import json
import os

import pandas as pd
import streamlit as st

from common.config import load_ref_ville
from common.storage import get_storage
from p4_croise.build_gold import GOLD_SYNTHESE_KEY

st.set_page_config(
    page_title="UrbanHub — Jumeau numérique urbain",
    page_icon="🏙️",
    layout="wide",
)

st.title("🏙️ UrbanHub — Jumeau numérique urbain")
st.markdown(
    "Plateforme Big Data **100 % Data Lake (Bronze brut → Silver Parquet → Gold JSON métier)** "
    "croisant **3 types de flux** : **Batch Météo Open-Meteo (P1)**, **Streaming Vélos CityBikes (P2)**, "
    "**IoT Pollution OpenAQ (P3)** et **Analyse croisée Gold (P4)**."
)

villes = load_ref_ville()


def _detect_datalake() -> tuple[str, bool, int, int, int, dict | None]:
    """Détecte le backend actif (MinIO S3 ou disque local) et lit la synthèse Gold."""
    prefer_local = os.getenv("STORAGE", "local").lower() == "local"
    order = (
        ((True, "Disque local (data/)"), (False, "MinIO S3"))
        if prefer_local
        else ((False, "MinIO S3"), (True, "Disque local (data/)"))
    )
    for use_local, label in order:
        try:
            st_backend = get_storage(use_local=use_local)
            bronze = st_backend.list("bronze/")
            silver = [k for k in st_backend.list("silver/") if k.endswith(".parquet")]
            gold = [k for k in st_backend.list("gold/") if k.endswith(".json")]
            synthese = st_backend.get_json(GOLD_SYNTHESE_KEY) if st_backend.exists(GOLD_SYNTHESE_KEY) else None
            if bronze or silver or gold or not prefer_local:
                return label, use_local, len(bronze), len(silver), len(gold), synthese
        except Exception:  # noqa: BLE001
            continue
    return "Disque local (data/)", True, 0, 0, 0, None


lake_mode, _, nb_bronze, nb_silver, nb_gold, synthese = _detect_datalake()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Villes couvertes", len(villes))
col2.metric("Data Lake actif", lake_mode)
col3.metric("Bucket silver (Parquet)", nb_silver)
col4.metric("Bucket gold (JSON métier)", nb_gold)

st.caption(
    f"Bucket **bronze** : **{nb_bronze}** fichiers bruts archivés · "
    "Architecture 3 buckets MinIO : `s3://bronze` → `s3://silver` → `s3://gold`."
)

if synthese and isinstance(synthese, dict):
    kpis = synthese.get("kpis_globaux_france") or {}
    cov = synthese.get("couverture_flux") or {}
    st.subheader("📊 Indicateurs urbains consolidés — Couche Gold (Aide à la décision publique)")
    if cov:
        def _fmt_range(r: dict | None) -> str:
            if not r or not r.get("debut_utc"):
                return "—"
            return f"{str(r['debut_utc'])[:16]} → {str(r['fin_utc'])[:16]}"

        st.caption(
            f"Période observée Gold : `{cov.get('periode_debut_utc', '—')}` → `{cov.get('periode_fin_utc', '—')}` | "
            f"Météo P1 : **{cov.get('p1_meteo_heures', 0):,}** h ({_fmt_range(cov.get('p1_meteo_periode'))}) · "
            f"Vélos P2 : **{cov.get('p2_velos_releves_silver', 0):,}** relevés "
            f"({_fmt_range(cov.get('p2_velos_periode'))}) · "
            f"Pollution P3 : **{cov.get('p3_pollution_mesures_silver', 0):,}** mesures "
            f"({_fmt_range(cov.get('p3_pollution_periode'))}) · "
            f"Croisement 3 flux : **{cov.get('croisement_complet_3_flux_heures', 0):,}** h"
        )
    if kpis:
        g1, g2, g3, g4, g5 = st.columns(5)
        t_fr = kpis.get("temperature_moy_france_c")
        u_fr = kpis.get("taux_utilisation_velo_france_pct")
        p_fr = kpis.get("indice_pollution_oms_france")
        s_fr = kpis.get("score_mobilite_douce_france")
        f_fr = kpis.get("pct_creneaux_favorables_france")
        g1.metric("Température moy. France", f"{t_fr} °C" if t_fr is not None else "N/A")
        g2.metric("Utilisation vélos", f"{u_fr} %" if u_fr is not None else "N/A")
        g3.metric("Indice pollution OMS", f"{p_fr}x" if p_fr is not None else "N/A")
        g4.metric("Score mobilité douce", f"{s_fr} / 100" if s_fr is not None else "N/A")
        g5.metric("Créneaux favorables", f"{f_fr} %" if f_fr is not None else "N/A")

    ind_villes = synthese.get("indicateurs_par_ville") or []
    if ind_villes:
        st.markdown("**Tableau de bord consolidé par métropole (extrait de `gold/synthese_indicateurs_urbains.json`)**")
        st.dataframe(pd.DataFrame(ind_villes), use_container_width=True, hide_index=True)

    q_list = synthese.get("questions_metier_couvertes") or []
    if q_list:
        with st.expander(f"✅ Couverture des questions métier en couche Gold ({len(q_list)} livrables JSON)"):
            st.dataframe(pd.DataFrame(q_list), use_container_width=True, hide_index=True)
            st.download_button(
                "📥 Télécharger gold/synthese_indicateurs_urbains.json",
                data=json.dumps(synthese, indent=2, ensure_ascii=False).encode("utf-8"),
                file_name="synthese_indicateurs_urbains.json",
                mime="application/json",
            )

st.subheader("Référentiel multi-sources des 12 métropoles françaises")
st.dataframe(
    [
        {
            "Ville": v["nom"],
            "ID": v["ville_id"],
            "Cœur UrbanHub": "Oui" if v.get("core_urbanhub") else "Extension",
            "Station Météo (P1)": v.get("noaa_station_id") or f"FR-{v['ville_id'].upper()}",
            "Réseau CityBikes (P2)": (v.get("citybikes") or {}).get("network_name", "—"),
            "Network ID (P2)": (v.get("citybikes") or {}).get("network_id", "—"),
            "Sites OpenAQ (P3)": len(v.get("openaq_location_ids") or []),
        }
        for v in villes
    ],
    use_container_width=True,
    hide_index=True,
)
