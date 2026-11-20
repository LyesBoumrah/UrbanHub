"""Dashboard — Qualité des données et pipeline (gold_pipeline, gold_couverture, system.tables). Responsable : commun."""
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import lib
from common import viz

if not lib.page("🧪 Qualité des données & pipeline", "Volumes de chaque couche, couverture des sources, contrôles "
                "qualité — le travail de data engineering derrière les indicateurs."):
    st.stop()
ville, _, _ = lib.sidebar(period=False)

st.subheader("Le pipeline")
st.markdown("""
```
EXTRACT                      TRANSFORM                                    LOAD
3 archives (5 ans) ──► Silo bronze/ ──► nettoyage, UTC, qualité ──► Silo silver/ (Parquet) ──► ClickHouse gold ──► dashboard
   Open-Meteo            (bruts, tels      unités, dédoublonnage,       croisement P4              10 tables
   CityBikes              quels,           marquage des valeurs         ville × heure              agrégées
   OpenAQ                 manifestes)      suspectes (sans supprimer)
```""")

pipe = lib.q("SELECT * FROM urbanhub.gold_pipeline ORDER BY couche, source")
gold = lib.q("""SELECT name AS table_gold, total_rows AS lignes, round(total_bytes / 1e6, 2) AS taille_mo
                FROM system.tables WHERE database = 'urbanhub' AND name LIKE 'gold_%' ORDER BY total_rows DESC""")
c = st.columns(4)
b = pipe[pipe["couche"] == "bronze"]
s = pipe[pipe["couche"] == "silver"]
c[0].metric("Bronze (Silo)", f"{b['taille_mo'].sum():,.0f} Mo".replace(",", " "),
            f"{int(b['objets'].sum()):,} fichiers bruts".replace(",", " "), delta_color="off")
c[1].metric("Silver (Silo, Parquet)", f"{s['taille_mo'].sum():,.0f} Mo".replace(",", " "),
            f"{int(s['lignes'].fillna(0).sum()) / 1e6:.1f} M lignes", delta_color="off")
c[2].metric("Gold (ClickHouse)", f"{gold['taille_mo'].sum():.0f} Mo", f"{len(gold)} tables", delta_color="off")
c[3].metric("Compression bronze → silver", f"÷ {b['taille_mo'].sum() / s[s['source'] != 'croisé P4']['taille_mo'].sum():.1f}",
            "JSON / CSV / Parquet bruts → Parquet typé", delta_color="off")

c1, c2 = st.columns([1.1, 1])
f = px.bar(pipe, x="taille_mo", y="source", color="couche", orientation="h", barmode="group", log_x=True,
           text=pipe["taille_mo"].map(lambda v: f"{v:,.1f} Mo".replace(",", " ")),
           color_discrete_map={"bronze": viz.ORANGE, "silver": viz.BLUE},
           labels={"taille_mo": "taille (Mo, échelle logarithmique)", "source": ""},
           title="Volume par couche et par source")
f.update_traces(textposition="outside")
f.update_layout(height=380)
c1.plotly_chart(f, use_container_width=True)
c2.markdown("**Détail des couches** (d'après les manifestes de chaque traitement)")
c2.dataframe(pipe, hide_index=True, use_container_width=True, column_config={
    "couche": "Couche", "source": "Source", "objets": st.column_config.NumberColumn("Fichiers", format="%d"),
    "taille_mo": st.column_config.NumberColumn("Mo", format="%.1f"),
    "lignes": st.column_config.NumberColumn("Lignes", format="%d"),
    "lignes_valides": st.column_config.NumberColumn("Lignes valides", format="%d"),
    "derniere_maj": "Dernier traitement (UTC)"})
st.caption("Le bronze pollution compte ~69 000 petits fichiers (un par station et par jour) : regroupés en quelques "
           "Parquet au Transform — le « problème des petits fichiers », classique en Big Data.")

st.subheader("Couverture des données")
src = st.radio("Source", ["météo", "pollution", "vélos"], horizontal=True)
cov = lib.q(f"SELECT * FROM urbanhub.gold_couverture WHERE source = '{src}'")
if src == "pollution":
    det = st.radio("Polluant", ["no2", "pm25", "pm10", "o3", "co"], format_func=lib.POLLUANTS.get, horizontal=True)
    cov = cov[cov["detail"] == det]
grid = cov.pivot_table(index="ville", columns="mois", values="pct_valides")
fig = go.Figure(go.Heatmap(z=grid.values, x=[f"{m:%m/%Y}" for m in grid.columns], y=grid.index, colorscale=lib.SEQ,
                           zmin=0, zmax=100, colorbar=dict(title="%"),
                           hovertemplate="%{y} — %{x} : %{z:.1f} % d'heures valides<extra></extra>"))
fig.update_layout(height=430, title=f"% d'heures présentes et valides par ville et par mois — {src}")
st.plotly_chart(fig, use_container_width=True)
moy = cov.groupby("ville")["pct_valides"].mean().round(1).sort_values()
st.markdown(f"Couverture moyenne ({src}) : **{cov['pct_valides'].mean():.1f} %** — la plus faible : "
            f"**{moy.index[0]} ({moy.iloc[0]:.1f} %)**, la plus forte : **{moy.index[-1]} ({moy.iloc[-1]:.1f} %)**.")
notes = {
    "météo": "réanalyse Open-Meteo : une valeur pour chaque heure, 100 % de couverture, aucune valeur suspecte.",
    "pollution": "trous de l'archive OpenAQ sur 2021-2023 ; 14 stations renumérotées par l'EEA en 2024 rattachées à "
                 "leur ancien identifiant (stations sous 50 % de couverture : 33 → 7) ; 1,3 % de valeurs marquées "
                 "(négatives ou au-dessus d'un plafond physique).",
    "vélos": "état reconstitué heure par heure à partir des changements d'état ; report limité à 24 h sans événement "
             "(au-delà, trou non comblé) ; une station ouverte en cours de mois compte pour le mois entier.",
}
lib.conclusion(notes[src])

st.subheader("Tables de la couche gold (ClickHouse)")
st.dataframe(gold, hide_index=True, use_container_width=True, column_config={
    "table_gold": "Table", "lignes": st.column_config.NumberColumn("Lignes", format="%d"),
    "taille_mo": st.column_config.NumberColumn("Taille (Mo, compressée)", format="%.2f")})

st.subheader("Principes qualité appliqués")
st.markdown("""
| Principe | Mise en œuvre |
|---|---|
| Bronze immuable et rejouable | copies octet pour octet + un manifeste JSON par extraction |
| Idempotence | relancer une extraction ne retélécharge rien ; le gold est rechargé par échange atomique de tables, sans doublon |
| Marquer, ne pas supprimer | `quality_flag` + `is_valid` au silver ; seules les lignes valides alimentent le gold |
| Ne pas combler les trous | pas d'interpolation ; couverture chiffrée par source, ville et mois |
| Tout en UTC | conversion en heure locale (Europe/Paris) uniquement pour les analyses et l'affichage |
| Tests | 90 tests automatisés (pytest) sur les règles de nettoyage, les agrégations et le chargement |
""")
lib.source("gold_pipeline, gold_couverture, system.tables")
