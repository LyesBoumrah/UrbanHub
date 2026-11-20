"""Dashboard — P4 Analyse croisée (Météo × Mobilité Vélos × Pollution).

Lit les fichiers JSON métier de la couche Gold (`gold/p4_croise/gold_ville_heure.json`) depuis
MinIO ou le disque local, ou reconstruit la couche Gold depuis les Parquet Silver.

Répond clairement et visuellement aux 4 questions métier de la Partie 4 :
  1. Existe-t-il une relation entre conditions météorologiques et pollution ?
  2. Les conditions météo influencent-elles l'utilisation des vélos urbains ?
  3. Peut-on identifier des conditions météorologiques favorables à la mobilité douce ?
  4. Existe-t-il des périodes où mobilité, pollution et météo interagissent fortement ?

Responsable : commun / P4.
"""

from __future__ import annotations

import json
import os

import pandas as pd
import plotly.express as px
import streamlit as st

from common.config import load_ref_ville
from common.storage import get_storage
from p4_croise.analytics import (
    favorable_cycling_conditions,
    pollution_by_weather_regime,
    strong_tripartite_interactions,
    weather_bike_usage_impact,
    weather_pollution_correlations,
)
from p4_croise.build_gold import build_gold, enrich_gold_indicators, load_gold_df

st.set_page_config(
    page_title="UrbanHub — P4 Analyse Croisée",
    page_icon="🔗",
    layout="wide",
)

st.title("🔗 P4 — Analyse croisée urbaine (Météo × Mobilité × Pollution)")
st.caption(
    "Couche Gold JSON unifiée au grain `(ville_id, heure_utc)` croisant Open-Meteo (P1), "
    "CityBikes (P2) et OpenAQ (P3) pour produire des indicateurs d'aide à la décision publique"
)

REF_VILLES = load_ref_ville()
VILLE_NOM_MAP = {v["ville_id"]: v["nom"] for v in REF_VILLES}
MOIS_NOMS = {
    1: "Jan", 2: "Fév", 3: "Mar", 4: "Avr", 5: "Mai", 6: "Juin",
    7: "Juil", 8: "Août", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Déc",
}
METEO_LABELS = {
    "temperature": "Température (°C)",
    "wind_speed": "Vent (m/s)",
    "precipitation": "Précipitations (mm/h)",
    "pressure": "Pression (hPa)",
    "relative_humidity": "Humidité relative (%)",
    "visibility": "Visibilité (m)",
}
POLLUTANT_LABELS = {
    "pm25": "PM2.5",
    "pm10": "PM10",
    "no2": "NO₂",
    "o3": "O₃",
    "co": "CO",
    "indice_pollution_oms": "Indice OMS",
}
REGIME_LABELS = {
    "exposition_cyclistes_pointe": "Exposition cyclistes (pointe + stagnation + pollution)",
    "episode_photochimique_chaleur": "Épisode photochimique (chaleur + O₃)",
    "intemperies_report_modal": "Intempéries (pluie/vent fort → report modal)",
    "dispersion_favorable": "Dispersion favorable (air sain + météo douce)",
    "normal": "Régime urbain normal",
}


def _prefer_local() -> bool:
    return os.getenv("STORAGE", "local").lower() == "local"


@st.cache_data(ttl=120)
def load_gold_data() -> tuple[pd.DataFrame, str, bool]:
    """Charge la table croisée Gold depuis ClickHouse Lakehouse ou JSON Gold (MinIO / local)."""
    if not _prefer_local():
        try:
            from common.warehouse import try_get_warehouse

            wh = try_get_warehouse()
            if wh is not None:
                df = wh.query_df("SELECT * FROM urbanhub.gold_ville_heure FINAL ORDER BY ville_id, heure_utc")
                if not df.empty:
                    df["heure_utc"] = pd.to_datetime(df["heure_utc"], utc=True)
                    return enrich_gold_indicators(df), "ClickHouse Lakehouse (urbanhub.gold_ville_heure)", False
        except Exception:  # noqa: BLE001
            pass

    order = (True, False) if _prefer_local() else (False, True)
    for use_local in order:
        try:
            df = load_gold_df(use_local=use_local)
            if not df.empty:
                df["heure_utc"] = pd.to_datetime(df["heure_utc"], utc=True)
                label = "JSON Gold local" if use_local else "JSON Gold MinIO"
                return enrich_gold_indicators(df), f"{label} (gold/p4_croise/gold_ville_heure.json)", use_local
        except Exception:  # noqa: BLE001
            continue

    # Tentative de construction automatique depuis Silver Parquet (MinIO puis local)
    for use_local in order:
        try:
            build_gold(use_local=use_local)
            df = load_gold_df(use_local=use_local)
            if not df.empty:
                df["heure_utc"] = pd.to_datetime(df["heure_utc"], utc=True)
                label = "JSON Gold local" if use_local else "JSON Gold MinIO"
                return enrich_gold_indicators(df), f"{label} (reconstruit depuis Silver Parquet)", use_local
        except Exception:  # noqa: BLE001
            continue

    return pd.DataFrame(), "Aucune donnée disponible", _prefer_local()


def _load_gold_json_preview(key: str, use_local: bool) -> dict | None:
    try:
        st_backend = get_storage(use_local=use_local)
        if st_backend.exists(key):
            return st_backend.get_json(key)
    except Exception:  # noqa: BLE001
        pass
    return None


gold_all, backend_label, active_use_local = load_gold_data()

with st.sidebar:
    st.header("Paramètres P4 — Analyse croisée")
    all_city_ids = [v["ville_id"] for v in REF_VILLES]
    avail_cities = sorted(gold_all["ville_id"].unique()) if not gold_all.empty else all_city_ids
    selected_cities = st.multiselect(
        "Villes françaises",
        options=avail_cities,
        default=avail_cities,
        format_func=lambda cid: VILLE_NOM_MAP.get(cid, cid),
    )

    min_inter_score = st.slider(
        "Seuil d'interaction tripartite forte (Q4, /100)",
        min_value=35.0,
        max_value=85.0,
        value=55.0,
        step=2.5,
    )

    st.divider()
    target_mode = "local" if _prefer_local() else "MinIO"
    if st.button(f"🔄 Recalculer la couche Gold JSON ({target_mode})", use_container_width=True):
        with st.spinner("Jointure (ville_id, heure_utc) P1 × P2 × P3 -> gold/*.json..."):
            try:
                build_gold(use_local=_prefer_local())
            except Exception:  # noqa: BLE001
                build_gold(use_local=True)
            st.cache_data.clear()
            st.rerun()

    st.caption(
        "CLI : `python -m p4_croise.flow` (MinIO) ou `--local`"
    )

if gold_all.empty:
    st.warning(
        "La couche Gold JSON (`gold/p4_croise/gold_ville_heure.json`) est encore vide. "
        "Lancez d'abord les flux d'ingestion (`docker compose up -d` ou "
        "`python scripts/run_all_parallel.py --local`) puis cliquez sur **Recalculer la couche Gold JSON**."
    )
    st.stop()

gold = gold_all.copy()
if selected_cities:
    gold = gold[gold["ville_id"].isin(selected_cities)]

if gold.empty:
    st.info("Aucune ligne Gold pour les villes sélectionnées.")
    st.stop()

gold["Ville"] = gold["ville_id"].map(lambda c: VILLE_NOM_MAP.get(c, c))

# Diagnostic de couverture croisée des 3 flux
n_meteo = int(gold["temperature"].notna().sum())
n_velos = int(gold["taux_utilisation_velo"].notna().sum())
n_air = int(gold["indice_pollution_oms"].notna().sum())
n_mp = int((gold["temperature"].notna() & gold["indice_pollution_oms"].notna()).sum())
n_mv = int((gold["temperature"].notna() & gold["taux_utilisation_velo"].notna()).sum())
n_all3 = int(
    (
        gold["temperature"].notna()
        & gold["taux_utilisation_velo"].notna()
        & gold["indice_pollution_oms"].notna()
    ).sum()
)

# ============================================================================
# Bandeau KPIs croisés & couverture des 3 flux
# ============================================================================
st.caption(
    f"Source active : **{backend_label}** | "
    f"Couverture des flux — Météo (P1) : **{n_meteo:,}** h | "
    f"Vélos (P2) : **{n_velos:,}** h | "
    f"Pollution (P3) : **{n_air:,}** h | "
    f"Croisement complet 3 flux : **{n_all3:,}** h"
)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Heures Gold (ville×h)", f"{len(gold):,}".replace(",", " "))
k2.metric("Score mobilité douce moyen", f"{gold['score_mobilite_douce'].mean():.1f} / 100")
k3.metric("Créneaux favorables vélo", f"{gold['condition_favorable_velo'].mean() * 100:.1f} %")
util_pct = float(gold["taux_utilisation_velo"].mean() * 100) if n_velos > 0 else 0.0
k4.metric("Taux utilisation vélos", f"{util_pct:.1f} %" if n_velos > 0 else "N/A")
oms_avg = float(gold["indice_pollution_oms"].mean()) if n_air > 0 else 0.0
k5.metric("Indice pollution OMS moyen", f"{oms_avg:.2f}x" if n_air > 0 else "N/A")

with st.expander("📘 Glossaire & Formules détaillées des métriques utilisées (P1–P4)"):
    st.markdown(
        """
**1. Métriques sources alignées au grain `(ville_id, heure_utc)`**
- **P1 Météo (Open-Meteo)** : `temperature` (°C), `dew_point` (°C), `relative_humidity` (%), `pressure` (hPa),
  `wind_speed` (`m/s`, converti via `km/h ÷ 3,6`), `precipitation` (`mm/h`), `visibility` (`m`, brouillard `< 1 000 m`).
- **P2 Mobilité (CityBikes)** :
  - `taux_occupation_velo = bikes_available / capacity` (offre disponible)
  - `taux_utilisation_velo = free_slots / capacity` (demande / vélos en circulation)
  - `pct_stations_critiques` : part de stations avec `taux_occupation ≤ 10 %` ou `≥ 90 %`
- **P3 Pollution (OpenAQ v3)** : concentrations harmonisées en `µg/m³` (`PM2.5`, `PM10`, `NO₂`, `O₃`, `CO`),
  `ratio_oms = concentration / seuil_OMS_2021` (`PM2.5: 15`, `PM10: 45`, `NO₂: 25`, `O₃: 100`, `CO: 4 000 µg/m³`),
  `indice_pollution_oms = max(ratio_oms)` (`> 1,0x` = dépassement OMS) et `polluant_dominant`.

**2. Indicateurs composites Gold P4**
- **`indice_confort_meteo` (`0 → 100`)** : Température (`45 - |T - 19| × 2,2`, max 45 pts)
  + Pluie (`30 / 16 / 5 / 0` pts) + Vent (`15 - max(0, vent - 3) × 1,8`, max 15 pts)
  + Visibilité (`10 / 6 / 2 / 0` pts).
- **`score_mobilite_douce` (`0 → 100`)** : `0,75 × indice_confort_meteo` +
  `clip(25 - max(0, indice_oms - 0,5) × 15, 0, 25)` (**75 % confort météo + 25 % qualité de l'air**).
- **`condition_favorable_velo` (`0 / 1`)** : `1` ssi `10 °C ≤ T ≤ 26 °C`, `pluie ≤ 0,2 mm/h`, `vent ≤ 6 m/s`,
  `visibilité ≥ 4 000 m` et `indice_oms ≤ 1,2x`.
- **`score_interaction_urbaine` (`0 → 100`)** : Stress météo/stagnation (`0–40 pts` : vent calme + extrême thermique
  + pluie) + Stress pollution (`min(40, indice_oms × 25)`) + Intensité mobilité
  (`min(30, util_velo × 22 + pct_critiques × 15)`).
- **`regime_interaction` (5 classes prioritaires)** :
  `exposition_cyclistes_pointe` (`OMS ≥ 1,1`, `vent < 3 m/s`, `util ≥ 35 %`),
  `episode_photochimique_chaleur` (`T ≥ 23 °C`, `O₃ ≥ 90 µg/m³`),
  `intemperies_report_modal` (`pluie > 1 mm/h` ou `vent ≥ 8 m/s`),
  `dispersion_favorable` (`favorable == 1` et `OMS ≤ 0,8`), sinon `normal`.

**3. Métriques statistiques**
- **Corrélation de Spearman (`ρ ∈ [-1, +1]`)** : corrélation de Pearson sur les rangs, mesurant les relations
  monotones non linéaires tout en restant robuste aux extrêmes.
- **Écarts en points (`pts`)** : différence absolue de taux d'utilisation vélo (favorable vs défavorable,
  pluie vs sec).
        """
    )

tab_q1, tab_q2, tab_q3, tab_q4 = st.tabs([
    "🌬️ Q1 — Météo × Pollution",
    "🚲 Q2 — Météo × Vélos",
    "🌿 Q3 — Conditions favorables mobilité douce",
    "⚡ Q4 — Interactions tripartites",
])

# ============================================================================
# Q1 : Existe-t-il une relation entre conditions météorologiques et pollution ?
# ============================================================================
with tab_q1:
    st.subheader("Q1 — Existe-t-il une relation entre conditions météorologiques et pollution ?")
    if n_mp == 0:
        st.warning(
            "Aucune heure commune entre Météo (P1) et Pollution (P3) pour les villes sélectionnées. "
            "Vérifiez que `OPENAQ_API_KEY` est renseignée et que P3 a ingéré des mesures sur la même période que P1."
        )
    else:
        corrs_mp = weather_pollution_correlations(gold)
        regimes_mp = pollution_by_weather_regime(gold)

        # Verdict chiffré dynamique calculé directement sur les données affichées
        verdict_parts = []
        if not corrs_mp.empty:
            strongest = corrs_mp.iloc[corrs_mp["correlation"].abs().argmax()]
            m_lbl = METEO_LABELS.get(strongest["meteo_var"], strongest["meteo_var"])
            p_lbl = POLLUTANT_LABELS.get(strongest["polluant"], strongest["polluant"])
            sens = "négative (effet dispersif / lessivant)" if strongest["correlation"] < 0 else "positive"
            verdict_parts.append(
                f"Plus forte relation mesurée ({n_mp:,} heures croisées) : **{m_lbl} ↔ {p_lbl}** "
                f"(Spearman ρ = **{strongest['correlation']:+.2f}**, corrélation {sens})."
            )
        wr_df = regimes_mp["par_vent_pluie"]
        if not wr_df.empty and wr_df["indice_oms_moyen"].notna().any():
            calm = wr_df[wr_df["classe_vent"].astype(str).str.startswith("1.")]["indice_oms_moyen"].mean()
            windy = wr_df[wr_df["classe_vent"].astype(str).str.startswith(("3.", "4."))]["indice_oms_moyen"].mean()
            if pd.notna(calm) and pd.notna(windy):
                verdict_parts.append(
                    f"Indice OMS moyen par vent calme (< 2 m/s) : **{calm:.2f}x** "
                    f"vs vent soutenu (≥ 5 m/s) : **{windy:.2f}x**."
                )
        if verdict_parts:
            st.success("**Réponse chiffrée (Q1)** : " + " ".join(verdict_parts))

        c_heat, c_wind = st.columns(2)
        with c_heat:
            if not corrs_mp.empty:
                piv_corr = corrs_mp.pivot(index="meteo_var", columns="polluant", values="correlation")
                piv_corr.index = [METEO_LABELS.get(i, i) for i in piv_corr.index]
                piv_corr.columns = [POLLUTANT_LABELS.get(c, c) for c in piv_corr.columns]
                fig_heat = px.imshow(
                    piv_corr,
                    text_auto=".2f",
                    color_continuous_scale="RdBu_r",
                    zmin=-0.8,
                    zmax=0.8,
                    aspect="auto",
                    title="Matrice de corrélation de Spearman (Météo ↔ Polluants)",
                    labels={"x": "Polluant", "y": "Variable météo", "color": "Spearman ρ"},
                )
                st.plotly_chart(fig_heat, use_container_width=True)
            else:
                st.info("Moins de 3 heures croisées pour calculer la matrice de corrélation.")

        with c_wind:
            if not wr_df.empty and wr_df["indice_oms_moyen"].notna().any():
                fig_wr = px.bar(
                    wr_df.dropna(subset=["indice_oms_moyen"]),
                    x="classe_vent",
                    y="indice_oms_moyen",
                    color="classe_pluie",
                    barmode="group",
                    title="Effet du vent (dispersion) et de la pluie (lessivage) sur l'indice OMS",
                    labels={
                        "classe_vent": "Régime de vent",
                        "indice_oms_moyen": "Indice OMS moyen",
                        "classe_pluie": "Précipitations",
                    },
                )
                fig_wr.add_hline(y=1.0, line_dash="dash", line_color="red", annotation_text="Seuil OMS (1,0x)")
                st.plotly_chart(fig_wr, use_container_width=True)

        c_temp, c_scat = st.columns(2)
        with c_temp:
            temp_df = regimes_mp["par_temperature"]
            pol_y = [
                c for c in ["no2_moyen", "pm25_moyen", "o3_moyen"]
                if c in temp_df.columns and temp_df[c].notna().any()
            ]
            if not temp_df.empty and pol_y:
                fig_tp = px.line(
                    temp_df,
                    x="classe_temperature",
                    y=pol_y,
                    markers=True,
                    title="Évolution de NO₂, PM2.5 et O₃ selon la température",
                    labels={
                        "classe_temperature": "Classe de température",
                        "value": "Concentration moyenne (µg/m³)",
                        "variable": "Polluant",
                    },
                )
                st.plotly_chart(fig_tp, use_container_width=True)
        with c_scat:
            mp_pts = gold.dropna(subset=["wind_speed", "indice_pollution_oms"]).head(1500)
            if not mp_pts.empty:
                fig_mp_sc = px.scatter(
                    mp_pts,
                    x="wind_speed",
                    y="indice_pollution_oms",
                    color="Ville",
                    hover_data=["heure_utc", "temperature", "precipitation", "polluant_dominant"],
                    title="Dispersion de la pollution (indice OMS) en fonction de la vitesse du vent",
                    labels={"wind_speed": "Vitesse du vent (m/s)", "indice_pollution_oms": "Indice OMS"},
                )
                fig_mp_sc.add_hline(y=1.0, line_dash="dot", line_color="red")
                st.plotly_chart(fig_mp_sc, use_container_width=True)

        with st.expander("📄 Inspecter le fichier Gold JSON Q1 (`gold/p4_croise/q1_meteo_pollution.json`)"):
            q1_json = _load_gold_json_preview("gold/p4_croise/q1_meteo_pollution.json", active_use_local)
            if q1_json:
                st.download_button(
                    "📥 Télécharger q1_meteo_pollution.json",
                    data=json.dumps(q1_json, indent=2, ensure_ascii=False).encode("utf-8"),
                    file_name="q1_meteo_pollution.json",
                    mime="application/json",
                )
                st.json(q1_json)

        st.info(
            "**Interprétation métier (Q1)** : La météorologie gouverne directement "
            "l'accumulation et la chimie des polluants :\n"
            "- **Dispersion éolienne** : un vent soutenu brasse la couche limite urbaine et fait chuter "
            "NO₂ et PM2.5 par rapport aux régimes calmes (< 2 m/s).\n"
            "- **Lessivage par la pluie** : les précipitations (> 0,2 mm/h) abaissent les concentrations "
            "particulaires (PM10, PM2.5).\n"
            "- **Contraste thermique** : le froid hivernal accroît PM2.5 et NO₂ (chauffage + inversion), "
            "tandis que la chaleur (> 20 °C) catalyse la photochimie de l'ozone (O₃)."
        )

# ============================================================================
# Q2 : Les conditions météo influencent-elles l'utilisation des vélos urbains ?
# ============================================================================
with tab_q2:
    st.subheader("Q2 — Les conditions météo influencent-elles l'utilisation des vélos urbains ?")
    impact_v = weather_bike_usage_impact(gold)
    tr_df = impact_v["par_temperature_pluie"]
    cr_df = impact_v["par_ville_pluie"]
    corr_v = impact_v["correlations"]

    if n_mv == 0 or tr_df.empty:
        st.warning(
            "Aucune heure commune entre Météo (P1) et Vélos (P2) pour les villes sélectionnées. "
            "Assurez-vous que P1 (données récentes) et P2 (streaming ou backfill) couvrent les mêmes créneaux horaires."
        )
    else:
        # Verdict chiffré dynamique Q2
        sec_avg = tr_df[tr_df["classe_pluie"].str.startswith("1.")]["taux_utilisation_pct"].mean()
        rain_avg = tr_df[~tr_df["classe_pluie"].str.startswith("1.")]["taux_utilisation_pct"].mean()
        v_msgs = [f"**{n_mv:,} heures croisées Météo × Vélos** analysées."]
        if pd.notna(sec_avg) and pd.notna(rain_avg):
            delta_rain = round(float(rain_avg - sec_avg), 1)
            v_msgs.append(
                f"Utilisation moyenne par temps sec : **{sec_avg:.1f} %** vs par temps de pluie : **{rain_avg:.1f} %** "
                f"(écart : **{delta_rain:+.1f} pts**)."
            )
        if not corr_v.empty:
            top_cv = corr_v.iloc[corr_v["correlation_utilisation_velo"].abs().argmax()]
            v_msgs.append(
                f"Facteur météo le plus corrélé : **{METEO_LABELS.get(top_cv['meteo_var'], top_cv['meteo_var'])}** "
                f"(Spearman ρ = **{top_cv['correlation_utilisation_velo']:+.2f}**)."
            )
        st.success("**Réponse chiffrée (Q2)** : " + " ".join(v_msgs))

        c_left, c_right = st.columns(2)
        with c_left:
            fig_tr = px.bar(
                tr_df,
                x="classe_temperature",
                y="taux_utilisation_pct",
                color="classe_pluie",
                barmode="group",
                title="Taux d'utilisation des vélos (%) par classe de température et de pluie",
                labels={
                    "classe_temperature": "Température",
                    "taux_utilisation_pct": "Taux d'utilisation moyen (%)",
                    "classe_pluie": "Régime de pluie",
                },
            )
            st.plotly_chart(fig_tr, use_container_width=True)
        with c_right:
            if not corr_v.empty:
                corr_plot = corr_v.copy()
                corr_plot["Variable météo"] = corr_plot["meteo_var"].map(lambda m: METEO_LABELS.get(m, m))
                fig_cv = px.bar(
                    corr_plot.sort_values("correlation_utilisation_velo"),
                    x="correlation_utilisation_velo",
                    y="Variable météo",
                    orientation="h",
                    color="correlation_utilisation_velo",
                    color_continuous_scale="RdBu",
                    range_color=[-0.8, 0.8],
                    title="Corrélation de Spearman : variables météo ↔ taux d'utilisation vélos",
                    labels={"correlation_utilisation_velo": "Coefficient de Spearman (ρ)"},
                )
                st.plotly_chart(fig_cv, use_container_width=True)
            elif not cr_df.empty:
                cr_df["Ville"] = cr_df["ville_id"].map(lambda c: VILLE_NOM_MAP.get(c, c))
                fig_cr = px.bar(
                    cr_df,
                    x="Ville",
                    y="taux_utilisation_pct",
                    color="classe_pluie",
                    barmode="group",
                    title="Sensibilité de l'usage vélo à la pluie par ville",
                    labels={"taux_utilisation_pct": "Taux d'utilisation (%)", "classe_pluie": "Précipitations"},
                )
                st.plotly_chart(fig_cr, use_container_width=True)

        if not corr_v.empty and not cr_df.empty:
            cr_plot = cr_df.copy()
            cr_plot["Ville"] = cr_plot["ville_id"].map(lambda c: VILLE_NOM_MAP.get(c, c))
            fig_cr2 = px.bar(
                cr_plot,
                x="Ville",
                y="taux_utilisation_pct",
                color="classe_pluie",
                barmode="group",
                title="Sensibilité de l'utilisation des vélos à la pluie par métropole",
                labels={"taux_utilisation_pct": "Taux d'utilisation (%)", "classe_pluie": "Précipitations"},
            )
            st.plotly_chart(fig_cr2, use_container_width=True)

        st.dataframe(tr_df, use_container_width=True, hide_index=True)

        with st.expander("📄 Inspecter le fichier Gold JSON Q2 (`gold/p4_croise/q2_meteo_velos.json`)"):
            q2_json = _load_gold_json_preview("gold/p4_croise/q2_meteo_velos.json", active_use_local)
            if q2_json:
                st.download_button(
                    "📥 Télécharger q2_meteo_velos.json",
                    data=json.dumps(q2_json, indent=2, ensure_ascii=False).encode("utf-8"),
                    file_name="q2_meteo_velos.json",
                    mime="application/json",
                )
                st.json(q2_json)

        st.info(
            "**Interprétation métier (Q2)** : L'usage des vélos suit une **courbe thermique en cloche** avec un "
            "maximum entre **12 °C et 26 °C**. À température comparable, une pluie forte (> 1 mm/h) entraîne un "
            "décrochage marqué du taux d'utilisation, tandis qu'une pluie fine (≤ 1 mm/h) reste partiellement "
            "absorbée par les déplacements domicile-travail contraints."
        )

# ============================================================================
# Q3 : Peut-on identifier des conditions météorologiques favorables à la mobilité douce ?
# ============================================================================
with tab_q3:
    st.subheader("Q3 — Peut-on identifier des conditions météorologiques favorables à la mobilité douce ?")
    st.markdown(
        "**Critères du créneau favorable (`condition_favorable_velo = 1`)** : "
        "Température **10 °C – 26 °C** · Temps sec (**≤ 0,2 mm/h**) · "
        "Vent modéré (**≤ 6 m/s**) · Visibilité **≥ 4 km** · Air sain (**indice OMS ≤ 1,2**)."
    )
    fav = favorable_cycling_conditions(gold)
    by_city_fav = fav["par_ville"]
    by_month_fav = fav["par_mois"]

    if not by_city_fav.empty:
        avg_fav_pct = float(by_city_fav["pct_heures_favorables"].mean())
        avg_score_md = float(by_city_fav["score_mobilite_moyen"].mean())
        best_city_row = by_city_fav.iloc[0]
        best_city_name = VILLE_NOM_MAP.get(best_city_row["ville_id"], best_city_row["ville_id"])
        gain_series = by_city_fav["gain_utilisation_pts"].dropna()
        gain_txt = (
            f" Gain moyen d'utilisation vélo pendant les créneaux favorables : **{gain_series.mean():+.1f} pts**."
            if not gain_series.empty
            else ""
        )
        st.success(
            f"**Réponse chiffrée (Q3)** : En moyenne **{avg_fav_pct:.1f} %** des créneaux horaires réunissent "
            f"les conditions optimales de mobilité douce (score moyen **{avg_score_md:.1f}/100**, "
            f"meilleure ville : **{best_city_name}** avec **{best_city_row['pct_heures_favorables']:.1f} %**)."
            + gain_txt
        )

    c_f1, c_f2 = st.columns(2)
    with c_f1:
        if not by_city_fav.empty:
            by_city_plot = by_city_fav.copy()
            by_city_plot["Ville"] = by_city_plot["ville_id"].map(lambda c: VILLE_NOM_MAP.get(c, c))
            has_both_util = (
                by_city_plot["util_velo_favorable_pct"].notna().any()
                and by_city_plot["util_velo_defavorable_pct"].notna().any()
            )
            if has_both_util:
                fig_fav_city = px.bar(
                    by_city_plot,
                    x="Ville",
                    y=["util_velo_favorable_pct", "util_velo_defavorable_pct"],
                    barmode="group",
                    title="Taux d'utilisation des vélos (%) : créneaux favorables vs défavorables",
                    labels={"value": "Taux d'utilisation vélo (%)", "variable": "Condition météo & air"},
                )
            else:
                fig_fav_city = px.bar(
                    by_city_plot.sort_values("score_mobilite_moyen", ascending=True),
                    x="score_mobilite_moyen",
                    y="Ville",
                    orientation="h",
                    color="pct_heures_favorables",
                    color_continuous_scale="YlGn",
                    title="Score moyen de praticabilité mobilité douce (/100) par métropole",
                    labels={
                        "score_mobilite_moyen": "Score mobilité douce (/100)",
                        "pct_heures_favorables": "% favorables",
                    },
                )
            st.plotly_chart(fig_fav_city, use_container_width=True)
    with c_f2:
        if not by_month_fav.empty and by_month_fav["mois"].nunique() > 1:
            by_month_plot = by_month_fav.copy()
            by_month_plot["Ville"] = by_month_plot["ville_id"].map(lambda c: VILLE_NOM_MAP.get(c, c))
            by_month_plot["Mois"] = by_month_plot["mois"].map(MOIS_NOMS)
            fig_fav_m = px.line(
                by_month_plot,
                x="Mois",
                y="pct_heures_favorables",
                color="Ville",
                markers=True,
                category_orders={"Mois": list(MOIS_NOMS.values())},
                title="Saisonnalité des créneaux favorables à la mobilité douce (% d'heures par mois)",
                labels={"pct_heures_favorables": "% d'heures favorables"},
            )
            st.plotly_chart(fig_fav_m, use_container_width=True)
        else:
            # Fenêtre courte (ex. 1 semaine) : profil par heure locale (0h-23h)
            hourly_fav = gold.copy()
            ts_local = pd.to_datetime(hourly_fav["heure_utc"], utc=True).dt.tz_convert("Europe/Paris")
            hourly_fav["heure_locale"] = ts_local.dt.hour
            prof_h = (
                hourly_fav.groupby("heure_locale", as_index=False)
                .agg(
                    score_mobilite_moyen=("score_mobilite_douce", "mean"),
                    pct_heures_favorables=("condition_favorable_velo", lambda s: round(float(s.mean() * 100.0), 1)),
                )
            )
            fig_fav_h = px.bar(
                prof_h,
                x="heure_locale",
                y="score_mobilite_moyen",
                color="pct_heures_favorables",
                color_continuous_scale="YlGn",
                title="Créneaux horaires les plus favorables à la mobilité douce (0h–23h heure locale)",
                labels={
                    "heure_locale": "Heure locale (Europe/Paris)",
                    "score_mobilite_moyen": "Score mobilité douce (/100)",
                    "pct_heures_favorables": "% d'heures favorables",
                },
            )
            fig_fav_h.update_xaxes(dtick=1, range=[-0.5, 23.5])
            st.plotly_chart(fig_fav_h, use_container_width=True)

    if not by_city_fav.empty:
        by_city_tbl = by_city_fav.copy()
        by_city_tbl["Ville"] = by_city_tbl["ville_id"].map(lambda c: VILLE_NOM_MAP.get(c, c))
        st.dataframe(
            by_city_tbl[[
                "Ville", "score_mobilite_moyen", "indice_confort_meteo_moyen", "pct_heures_favorables",
                "util_velo_favorable_pct", "util_velo_defavorable_pct", "gain_utilisation_pts", "nb_heures",
            ]],
            use_container_width=True,
            hide_index=True,
        )

    q3_key = "gold/p4_croise/q3_conditions_favorables_mobilite_douce.json"
    with st.expander(f"📄 Inspecter le fichier Gold JSON Q3 (`{q3_key}`)"):
        q3_json = _load_gold_json_preview(q3_key, active_use_local)
        if q3_json:
            st.download_button(
                "📥 Télécharger q3_conditions_favorables_mobilite_douce.json",
                data=json.dumps(q3_json, indent=2, ensure_ascii=False).encode("utf-8"),
                file_name="q3_conditions_favorables_mobilite_douce.json",
                mime="application/json",
            )
            st.json(q3_json)

# ============================================================================
# Q4 : Existe-t-il des périodes où mobilité, pollution et météo interagissent fortement ?
# ============================================================================
with tab_q4:
    st.subheader("Q4 — Existe-t-il des périodes où mobilité, pollution et météo interagissent fortement ?")
    inter = strong_tripartite_interactions(gold, min_score=min_inter_score)
    regimes_df = inter["repartition_regimes"]
    episodes_df = inter["episodes"]
    crit_df = inter["heures_critiques"]

    if not episodes_df.empty:
        top_ep = episodes_df.iloc[0]
        top_ville = VILLE_NOM_MAP.get(top_ep["ville_id"], top_ep["ville_id"])
        st.success(
            f"**Réponse chiffrée (Q4)** : **{len(episodes_df)} épisodes continus** d'interaction tripartite forte "
            f"(score ≥ {min_inter_score:.0f}/100) identifiés. Pic maximal observé à **{top_ville}** "
            f"(score **{top_ep['score_interaction_max']}/100**, durée **{top_ep['duree_heures']} h**, "
            f"régime : **{REGIME_LABELS.get(top_ep['regime_dominant'], top_ep['regime_dominant'])}**)."
        )
    else:
        st.info(
            f"**Réponse chiffrée (Q4)** : Aucun épisode continu ne dépasse le seuil de {min_inter_score:.0f}/100 "
            "sur la sélection actuelle. Abaissez le curseur de seuil dans la barre latérale "
            "pour explorer les interactions modérées."
        )

    c_r1, c_r2 = st.columns([2, 3])
    with c_r1:
        if not regimes_df.empty:
            reg_plot = regimes_df.copy()
            reg_plot["Régime"] = reg_plot["regime_interaction"].map(lambda r: REGIME_LABELS.get(r, r))
            fig_reg = px.pie(
                reg_plot,
                names="Régime",
                values="nb_heures",
                hole=0.4,
                title="Répartition des heures selon le régime d'interaction urbaine",
            )
            st.plotly_chart(fig_reg, use_container_width=True)
    with c_r2:
        scatter_df = (
            crit_df.dropna(subset=["taux_utilisation_velo", "indice_pollution_oms"]).copy()
            if not crit_df.empty
            else pd.DataFrame()
        )
        if not scatter_df.empty:
            scatter_df["Ville"] = scatter_df["ville_id"].map(lambda c: VILLE_NOM_MAP.get(c, c))
            scatter_df["Régime"] = scatter_df["regime_interaction"].map(lambda r: REGIME_LABELS.get(r, r))
            fig_sc = px.scatter(
                scatter_df,
                x="taux_utilisation_velo",
                y="indice_pollution_oms",
                color="Régime",
                size="score_interaction_urbaine",
                hover_data=["Ville", "heure_utc", "temperature", "wind_speed", "polluant_dominant"],
                title="Heures d'interaction forte : Utilisation vélo vs Pollution OMS",
                labels={
                    "taux_utilisation_velo": "Taux d'utilisation vélos (0–1)",
                    "indice_pollution_oms": "Indice de pollution OMS",
                },
            )
            fig_sc.add_hline(y=1.0, line_dash="dash", line_color="red", annotation_text="Seuil OMS")
            st.plotly_chart(fig_sc, use_container_width=True)
        else:
            # Chronologie temporelle du score d'interaction tripartite
            ts_inter = (
                gold.groupby(["heure_utc", "Ville"], as_index=False)["score_interaction_urbaine"]
                .mean()
                .sort_values("heure_utc")
            )
            fig_ts = px.line(
                ts_inter,
                x="heure_utc",
                y="score_interaction_urbaine",
                color="Ville",
                title="Évolution temporelle du score d'interaction tripartite Météo × Mobilité × Pollution (/100)",
                labels={"heure_utc": "Date / Heure (UTC)", "score_interaction_urbaine": "Score d'interaction (/100)"},
            )
            fig_ts.add_hline(y=min_inter_score, line_dash="dash", line_color="red", annotation_text="Seuil fort")
            st.plotly_chart(fig_ts, use_container_width=True)

    if not regimes_df.empty:
        st.markdown("**Caractéristiques moyennes par régime d'interaction urbaine** :")
        st.dataframe(regimes_df, use_container_width=True, hide_index=True)

    if not episodes_df.empty:
        ep_tbl = episodes_df.copy()
        ep_tbl["Ville"] = ep_tbl["ville_id"].map(lambda c: VILLE_NOM_MAP.get(c, c))
        st.markdown(f"**{len(ep_tbl)} épisodes continus d'interaction tripartite forte détectés** :")
        st.dataframe(
            ep_tbl[[
                "Ville", "regime_dominant", "debut_utc", "fin_utc", "duree_heures",
                "score_interaction_max", "temperature_moy", "vent_moy",
                "taux_utilisation_velo_pct", "indice_oms_max", "polluant_dominant",
            ]],
            use_container_width=True,
            hide_index=True,
        )

    with st.expander("📄 Inspecter le fichier Gold JSON Q4 (`gold/p4_croise/q4_interactions_fortes.json`)"):
        q4_json = _load_gold_json_preview("gold/p4_croise/q4_interactions_fortes.json", active_use_local)
        if q4_json:
            st.download_button(
                "📥 Télécharger q4_interactions_fortes.json",
                data=json.dumps(q4_json, indent=2, ensure_ascii=False).encode("utf-8"),
                file_name="q4_interactions_fortes.json",
                mime="application/json",
            )
            st.json(q4_json)

    st.info(
        "**Aide à la décision publique (Q4)** :\n"
        "- **Exposition cyclistes en heure de pointe** : forte fréquentation vélo + vent calme (< 3 m/s) "
        "+ dépassement OMS → recommander des itinéraires cyclables secondaires éloignés des axes routiers.\n"
        "- **Épisode photochimique estival** : chaleur (≥ 23 °C) + pic O₃ l'après-midi "
        "→ limiter les efforts intenses entre 14h et 17h.\n"
        "- **Intempéries & report modal** : pluie forte (> 1 mm/h) ou vent fort (≥ 8 m/s) "
        "→ anticiper le report brusque des cyclistes vers les transports en commun."
    )
