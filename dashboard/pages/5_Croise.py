"""Dashboard — P4 Analyse croisée, organisé par question métier (gold_ville_heure, gold_ville_jour + mêmes fonctions
que le notebook 04). Responsable : commun."""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import lib
from common import viz
from p4_croise import analytics as a

if not lib.page("🔀 Analyse croisée", "Météo × pollution (5 ans) et météo × vélos (nov. 2024 → sept. 2026), "
                "effets mesurés à heure et saison égales."):
    st.stop()
_, d1, d2 = lib.sidebar()

COLS = ["ville_id", "heure_utc", "temperature_c", "precipitation_mm", "vent_ms", "humidite_pct", "pression_hpa",
        "nebulosite_pct", "pm25", "pm10", "no2", "o3", "activite_par_station", "a_meteo", "a_pollution", "a_velos",
        "heure_locale", "jour_semaine", "est_weekend", "date_locale", "mois", "saison"]


@st.cache_data(ttl=900, show_spinner="Chargement de gold_ville_heure…")
def data(villes: tuple, start, end):
    df = lib.client().query_df(f"SELECT {', '.join(COLS)} FROM urbanhub.gold_ville_heure "
                               f"WHERE ville_id IN ({lib.sql_in(villes)}) AND {lib.period_sql('date_locale', start, end)}")
    df["date_locale"] = df["date_locale"].astype(str)
    return a.pollution_indices(df), a.velo_frame(df), df


choix = st.multiselect("Villes analysées", list(lib.VILLES), default=list(lib.VILLES), format_func=lib.VILLES.get)
if not choix:
    st.stop()
pol, velo, df = data(tuple(sorted(choix)), d1, d2)
st.info(f"**{lib.period_label(d1, d2)}** · **Indice relatif** : chaque mesure est rapportée à la normale de la même "
        "ville, du même mois (de la même saison pour le vélo) et de la même heure. Indice 1 = normal ; 0,7 = 30 % en "
        "dessous. On isole ainsi l'effet de la météo des cycles jour / nuit et été / hiver.")
WL = ["< 1", "1-2", "2-3", "3-5", "5-8", "> 8"]
TL = ["< 0", "0-5", "5-10", "10-15", "15-20", "20-25", "25-30", "> 30"]


SHORT_RAIN = dict(zip(a.RAIN_LABELS, ["sec", "bruine", "modérée", "forte"]))


def bar_index(b, title, ytitle="indice (1 = normale)"):
    """Barres d'indice ; chaque barre porte l'étiquette de SA classe (aucun décalage si une classe est vide)."""
    if b.empty:
        return go.Figure().update_layout(title=f"{title} — pas assez de données sur la période", height=340)
    x = b.iloc[:, 0].astype(str).map(lambda v: SHORT_RAIN.get(v, v))
    fig = go.Figure(go.Bar(x=x, y=b["moyenne"], marker_color=viz.BLUE, text=b["moyenne"].round(2),
                           textposition="outside", customdata=b["heures"],
                           hovertemplate="%{x} : %{y:.2f}<br>%{customdata:,} heures<extra></extra>"))
    fig.add_hline(y=1, line_dash="dash", line_color=viz.INK_2)
    fig.update_layout(title=title, yaxis_title=ytitle, height=340,
                      yaxis_range=[0, max(1.45, b["moyenne"].max() + .15)])
    return fig


q1, q2, q3, q4 = st.tabs(["Q1 · Météo → pollution", "Q2 · Météo → vélo", "Q3 · Conditions favorables",
                          "Q4 · Interactions & modèle"])

with q1:
    p = st.radio("Polluant", ["i_no2", "i_pm25", "i_pm10"], horizontal=True,
                 format_func=lambda x: {"i_no2": "NO2", "i_pm25": "PM2.5", "i_pm10": "PM10"}[x])
    c1, c2 = st.columns(2)
    c1.plotly_chart(bar_index(a.binned(pol, "vent_ms", p, a.WIND_BINS, WL), "Selon le vent (m/s) : dispersion"),
                    use_container_width=True)
    w = a.rain_washout(df)
    fig = go.Figure(go.Bar(x=w.index.astype(str), y=w[p], marker_color=viz.BLUE, text=w[p].round(2),
                           textposition="outside"))
    fig.add_hline(y=1, line_dash="dash", line_color=viz.INK_2)
    fig.update_layout(title="Selon la pluie de la journée : lessivage", yaxis_title="indice", height=340,
                      yaxis_range=[0, 1.45])
    c2.plotly_chart(fig, use_container_width=True)
    c3, c4 = st.columns(2)
    raw = a.binned(pol, "temperature_c", "o3", a.TEMP_BINS, TL)
    fig = go.Figure(go.Bar(x=raw.iloc[:, 0].astype(str), y=raw["moyenne"], marker_color=viz.BLUE, text=raw["moyenne"].round(0),
                           textposition="outside"))
    fig.add_hline(y=100, line_dash="dash", line_color=viz.INK_2, annotation_text="valeur guide OMS 8 h")
    fig.update_layout(title="Ozone (µg/m³) selon la température (°C)", yaxis_title="µg/m³", height=340)
    c3.plotly_chart(fig, use_container_width=True)
    stag = a.stagnation(df)
    if not stag.empty:
        fig = go.Figure(go.Heatmap(z=stag.values, x=stag.columns, y=stag.index, colorscale=lib.SEQ,
                                   text=stag.round(2).values, texttemplate="%{text}",
                                   hovertemplate="%{y}, %{x} : indice %{z:.2f}<extra></extra>"))
        fig.update_layout(title="Hiver : PM2.5 selon pression et vent", height=340)
        c4.plotly_chart(fig, use_container_width=True)
    lib.conclusion("**le vent disperse** (NO2 ÷ 3, PM2.5 ÷ 2 entre vent calme et vent fort) ; **la pluie lave l'air** "
                   "(particules −36 % un jour très pluvieux) ; **l'anticyclone d'hiver piège les particules** (× 2,2 "
                   "entre « haute pression + calme » et « dépression ventée ») ; **la chaleur fabrique l'ozone** "
                   "(113 µg/m³ au-dessus de 30 °C) — d'où les épisodes d'ozone pendant les vagues de chaleur.")

with q2:
    if velo.empty:
        st.warning("Pas de données vélo sur la période choisie (l'archive CityBikes commence en novembre 2024).")
    else:
        day = velo[velo["heure_locale"].between(7, 20)]
        c1, c2, c3 = st.columns(3)
        c1.plotly_chart(bar_index(a.rain_effect(velo),
                                  "Selon la pluie dans l'heure", "activité vélo (indice)"), use_container_width=True)
        c2.plotly_chart(bar_index(a.binned(day, "temperature_c", "i_velo", a.TEMP_BINS, TL),
                                  "Selon la température (°C)", "activité vélo (indice)"), use_container_width=True)
        c3.plotly_chart(bar_index(a.binned(day, "vent_ms", "i_velo", a.WIND_BINS, WL), "Selon le vent (m/s)",
                                  "activité vélo (indice)"), use_container_width=True)
        eff = a.rain_effect_by_city(velo).reset_index()
        eff["ville"] = eff["ville_id"].map(lib.VILLES)
        eff = eff.sort_values("effet_pluie_pct", ascending=False)
        fig = px.bar(eff, x="effet_pluie_pct", y="ville", orientation="h",
                     text=eff["effet_pluie_pct"].map(lambda v: f"{v:.0f} %"),
                     labels={"effet_pluie_pct": "%", "ville": ""},
                     title="Baisse de l'activité sous la pluie (≥ 0,5 mm/h), par ville")
        fig.update_traces(marker_color=viz.BLUE, textposition="outside")
        st.plotly_chart(fig, use_container_width=True)
    lib.conclusion("**la pluie est le premier facteur** : −12 % sous la bruine, −29 % sous une pluie modérée, **−36 % "
                   "sous une forte pluie**. Optimum de température **20-30 °C**, −19 % par temps de gel ; le vent ne "
                   "compte qu'au-delà de 5 m/s (−18 % au-delà de 8 m/s). Les usages « loisir » sont les plus "
                   "sensibles à la pluie (Montpellier −48 %, Strasbourg −39 %).")

with q3:
    st.markdown("**Score de favorabilité (0 à 3)** : +1 si 10-25 °C, +1 s'il ne pleut pas, +1 si vent < 5 m/s.")
    c1, c2 = st.columns([1, 1.4])
    if not velo.empty:
        val = a.favorability_validation(velo).reset_index()
        fig = go.Figure(go.Bar(x=val["score"].astype(str), y=val["indice_activite"], marker_color=viz.BLUE,
                               text=val["indice_activite"].round(2), textposition="outside"))
        fig.add_hline(y=1, line_dash="dash", line_color=viz.INK_2)
        fig.update_layout(title="Validation : activité observée selon le score", xaxis_title="score",
                          yaxis_title="activité vélo (indice)", height=360, yaxis_range=[0, 1.3])
        c1.plotly_chart(fig, use_container_width=True)
    fav = a.favorable_share(df).reset_index()
    fav["ville"] = fav["ville_id"].map(lib.VILLES)
    fav = fav.sort_values("pct_heures_favorables")
    fig = px.bar(fav, x="pct_heures_favorables", y="ville", orientation="h",
                 text=fav["pct_heures_favorables"].map(lambda v: f"{v:.1f} %"),
                 labels={"pct_heures_favorables": "% des heures 7 h-21 h", "ville": ""},
                 title="Heures de journée pleinement favorables (score 3)")
    fig.update_traces(marker_color=viz.BLUE, textposition="outside")
    c2.plotly_chart(fig, use_container_width=True)
    lib.conclusion("le score est **validé par les usagers** : activité 0,81 → 0,87 → 0,98 → 1,07, soit **+32 %** entre "
                   "les pires et les meilleures conditions. **Bordeaux et Montpellier** offrent le plus d'heures "
                   "favorables, Lille le moins. Paradoxe : les villes au meilleur climat ne sont pas celles où l'on "
                   "roule le plus → **l'infrastructure compte plus que le climat**.")

with q4:
    j = lib.q(f"""SELECT ville_id, date_locale, pm25, o3_max_8h, t_max, pluie_mm, vent_moy_ms, velo_indice_activite
                  FROM urbanhub.gold_ville_jour WHERE velo_indice_activite IS NOT NULL
                  AND ville_id IN ({lib.sql_in(choix)}) AND {lib.period_sql('date_locale', d1, d2)}""")
    if j.empty:
        st.warning("Pas de données vélo sur la période choisie.")
    else:
        j["pollution_elevee"] = (j["pm25"] > 15) | (j["o3_max_8h"] > 120)
        j["meteo_defavorable"] = ((j["pluie_mm"] >= 5) | (j["t_max"] >= 30) | (j["t_max"] <= 3)
                                  | (j["vent_moy_ms"] >= 6))
        j["velo_atypique"] = (j["velo_indice_activite"] < 0.75) | (j["velo_indice_activite"] > 1.25)
        triple = j[j["pollution_elevee"] & j["meteo_defavorable"] & j["velo_atypique"]]
        c = st.columns(3)
        c[0].metric("Jours ville analysés", f"{len(j):,}".replace(",", " "))
        c[1].metric("Les 3 conditions réunies", len(triple))
        ct = pd.crosstab(j["pollution_elevee"], j["velo_atypique"], normalize="index")
        c[2].metric("Usage atypique : jour pollué vs normal",
                    f"{100 * ct.loc[True, True]:.0f} % vs {100 * ct.loc[False, True]:.0f} %"
                    if True in ct.index and True in ct.columns and False in ct.index else "—")
        st.markdown("**Jours où pollution élevée, météo défavorable et usage atypique coïncident**")
        st.dataframe(triple.sort_values("velo_indice_activite")[["ville", "date_locale", "pm25", "o3_max_8h", "t_max",
                                                                "pluie_mm", "velo_indice_activite"]].head(12),
                     hide_index=True, use_container_width=True, column_config={
                         "ville": "Ville", "date_locale": st.column_config.DateColumn("Date", format="DD/MM/YYYY"),
                         "pm25": st.column_config.NumberColumn("PM2.5", format="%.1f"),
                         "o3_max_8h": st.column_config.NumberColumn("O3 max 8 h", format="%.0f"),
                         "t_max": st.column_config.NumberColumn("T max", format="%.1f"),
                         "pluie_mm": st.column_config.NumberColumn("Pluie (mm)", format="%.1f"),
                         "velo_indice_activite": st.column_config.NumberColumn("Indice vélo", format="%.2f")})
    lib.method("pollution élevée = PM2.5 > 15 µg/m³ ou O3 > 120 ; météo défavorable = ≥ 5 mm de pluie, T max ≥ 30 °C "
               "ou ≤ 3 °C, ou vent ≥ 6 m/s ; usage atypique = indice vélo < 0,75 ou > 1,25. Le modèle (gradient "
               "boosting) est présenté dans le notebook 04.")
    lib.conclusion("les coïncidences sont rares et **calendaires** (21 juin, vacances de Noël) ou **caniculaires** : "
                   "**la pollution ne fait pas fuir les cyclistes**. Un modèle explique **68 %** de l'activité "
                   "horaire, presque entièrement par **le réseau et l'heure** ; la météo seule ne prédit pas les "
                   "variations d'un jour à l'autre — il faudrait ajouter vacances, jours fériés et événements.")
lib.source("gold_ville_heure, gold_ville_jour")
