"""Dashboard — P2 Vélos, organisé par question métier (gold_station_velo, gold_zone_velo, gold_velo_profil).
Responsable : P2."""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import lib
from common import viz

if not lib.page("🚲 Vélos en libre-service", "Archive CityBikes (nov. 2024 → sept. 2026), 3 843 stations exploitables."):
    st.stop()
ville, _, _ = lib.sidebar(period=False)
nom = lib.VILLES[ville]
st.caption("Les indicateurs par station sont calculés sur toute la période vélo (novembre 2024 → septembre 2026) : "
           "le filtre de période ne s'applique pas à cette page.")

st_v = lib.q("SELECT * FROM urbanhub.gold_station_velo")
ok_all = st_v[st_v["statut_station"] == "exploitable"]
ok = ok_all[ok_all["ville_id"] == ville]
kpi = lib.q("SELECT ville_id, velo_activite_station_jour, pct_zones_sous_offre, pct_stations_critiques "
            "FROM urbanhub.gold_kpi_ville")
MAP = dict(lat="latitude", lon="longitude", zoom=11.3, height=520, map_style="carto-positron")


def city_bars(col, title, fmt="{:.0f}", suffix=""):
    d = kpi.sort_values(col)
    f = px.bar(d, x=col, y="ville", orientation="h", text=d[col].map(lambda v: fmt.format(v) + suffix), title=title,
               labels={col: "", "ville": ""})
    f.update_traces(marker_color=[viz.ORANGE if v == ville else viz.BLUE for v in d["ville_id"]],
                    textposition="outside")
    f.update_layout(height=420)
    return f


q1, q2, q3, q4, q5 = st.tabs(["Q1 · Stations les plus utilisées", "Q2 · Zones en sous-offre", "Q3 · Pics journaliers",
                              "Q4 · Déséquilibres", "Q5 · Stations à rééquilibrer"])

with q1:
    c1, c2 = st.columns([1.3, 1])
    top = ok.nsmallest(10, "rang_activite_ville")[::-1]
    f = px.bar(top, x="activite_jour", y="station_nom", orientation="h", text=top["activite_jour"].round(0),
               title=f"{nom} : les 10 stations les plus utilisées",
               labels={"activite_jour": "mouvements / jour (emprunts + retours)", "station_nom": ""})
    f.update_traces(marker_color=viz.BLUE, textposition="outside")
    c1.plotly_chart(f, use_container_width=True)
    c2.plotly_chart(city_bars("velo_activite_station_jour", "Mouvements par station et par jour"),
                    use_container_width=True)
    m = px.scatter_map(ok, color="activite_jour", color_continuous_scale=lib.SEQ, hover_name="station_nom",
                       range_color=(0, ok["activite_jour"].quantile(0.97)),
                       hover_data={"activite_jour": ":.0f", "latitude": False, "longitude": False},
                       labels={"activite_jour": "mouvements/jour"}, **MAP)
    m.update_traces(marker=dict(size=9))
    m.update_layout(margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(m, use_container_width=True)
    lib.method("activité = somme des variations du nombre de vélos (emprunts + retours) ; stations techniques, "
               "aberrantes et hors service exclues (listées dans le notebook 02).")
    lib.conclusion("les stations les plus utilisées sont des **gares et places centrales** : Augagneur / Fosse aux "
                   "Ours (544 mouvements/jour), Part-Dieu et Opéra à Lyon, Matabiau à Toulouse, gares de Lyon et "
                   "Saint-Lazare à Paris. **Lyon** a l'usage le plus intense (≈ 144 mouvements / station / jour) ; "
                   "Strasbourg et Montpellier sont très en retrait.")

with q2:
    z = lib.q(f"SELECT * FROM urbanhub.gold_zone_velo WHERE ville_id = '{ville}'")
    c1, c2 = st.columns([1.5, 1])
    m = px.scatter_map(z, color="pct_vide", color_continuous_scale=lib.SEQ, range_color=(0, 40), size="stations",
                       size_max=14, hover_data={"pct_vide": ":.1f", "stations": True, "latitude": False,
                                                "longitude": False}, labels={"pct_vide": "% du temps vide"}, **MAP)
    m.update_layout(margin=dict(l=0, r=0, t=0, b=0))
    c1.markdown(f"**{nom} : {int(z['sous_offre'].sum())} zones de 500 m sur {len(z)} en sous-offre** "
                f"(stations vides ≥ 15 % du temps)")
    c1.plotly_chart(m, use_container_width=True)
    c2.plotly_chart(city_bars("pct_zones_sous_offre", "Part des zones en sous-offre", "{:.1f}", " %"),
                    use_container_width=True)
    lib.conclusion("**≈ 12 % des zones** de 500 m sont en sous-offre. **Marseille** est très touchée (49 % de ses "
                   "zones), Paris presque pas (2 %). La sous-offre se situe **près des centres**, là où la demande "
                   "dépasse l'offre (distance médiane 2,2-2,6 km contre 3,9-4,5 km pour les autres zones).")

with q3:
    p = lib.q(f"SELECT jour_semaine, heure_locale, indice FROM urbanhub.gold_velo_profil WHERE ville_id = '{ville}'")
    grid = p.pivot(index="jour_semaine", columns="heure_locale", values="indice")
    f = go.Figure(go.Heatmap(z=grid.values, x=[f"{h} h" for h in grid.columns], y=lib.JOURS, colorscale=lib.SEQ,
                             colorbar=dict(title="indice"),
                             hovertemplate="%{y} %{x} : %{z:.2f} × la moyenne<extra></extra>"))
    f.update_layout(height=320, yaxis_autorange="reversed", title=f"{nom} : activité par heure locale (1 = moyenne)")
    st.plotly_chart(f, use_container_width=True)
    wd = p[p["jour_semaine"] < 5].groupby("heure_locale")["indice"].mean()
    st.markdown(f"Jours ouvrés à {nom} : pic du matin à **{int(wd.loc[5:11].idxmax())} h**, pic du soir à "
                f"**{int(wd.loc[14:21].idxmax())} h**.")
    lib.conclusion("**les 12 villes ont le même rythme** : pic à **8 h**, pic plus fort à **17-18 h** les jours "
                   "ouvrés (domicile-travail), un seul pic vers 18 h le week-end. Usage plus « loisir » à Montpellier "
                   "et Strasbourg (week-end ≥ semaine), très pendulaire à Clermont-Ferrand et Nantes.")

with q4:
    d = ok_all.dropna(subset=["remplissage_matin", "remplissage_soir"]).copy()
    bands = [0, 1, 2, 4, 8, 50]
    labels = ["0-1 km", "1-2 km", "2-4 km", "4-8 km", "> 8 km"]
    d["zone"] = pd.cut(d["distance_centre_km"], bands, labels=labels, right=False)
    portee = st.radio("Portée", [f"{nom}", "12 villes"], horizontal=True)
    if portee != "12 villes":
        d = d[d["ville_id"] == ville]
    g = d.groupby("zone", observed=True)[["remplissage_matin", "remplissage_soir"]].mean().reset_index()
    f = go.Figure([go.Bar(x=g["zone"], y=g["remplissage_matin"], name="pointe du matin (7 h-10 h)",
                          marker_color=viz.BLUE),
                   go.Bar(x=g["zone"], y=g["remplissage_soir"], name="pointe du soir (17 h-20 h)",
                          marker_color=viz.ORANGE)])
    f.update_layout(barmode="group", height=380, yaxis_title="taux de remplissage moyen",
                    xaxis_title="distance au centre-ville", title=f"Remplissage matin / soir — {portee}")
    st.plotly_chart(f, use_container_width=True)
    lib.conclusion("le **centre se remplit pendant la journée** (< 1 km : 0,35 le matin → 0,41 le soir) et **la "
                   "couronne de 4 à 8 km se vide** (0,39 → 0,32) : c'est le flux pendulaire. Au-delà de 8 km, les "
                   "stations restent pleines (sous-utilisées). → Ramener des vélos du centre vers la couronne en fin "
                   "de journée.")

with q5:
    crit = ok[ok["action"] != "aucune"]
    c1, c2 = st.columns([1.5, 1])
    d = ok.assign(action=ok["action"].replace({"aucune": "aucune action"}))
    m = px.scatter_map(d, color="action", hover_name="station_nom",
                       category_orders={"action": ["réapprovisionner", "désengorger", "aucune action"]},
                       color_discrete_map={"réapprovisionner": viz.SERIES[0], "désengorger": viz.SERIES[1],
                                           "aucune action": "#c3c2b7"},
                       hover_data={"score_criticite": ":.0f", "pct_vide_pointe": ":.0f", "pct_pleine_pointe": ":.0f",
                                   "latitude": False, "longitude": False},
                       labels={"score_criticite": "criticité (%)", "pct_vide_pointe": "vide en pointe (%)",
                               "pct_pleine_pointe": "pleine en pointe (%)"}, **MAP)
    m.update_traces(marker=dict(size=9))
    m.update_layout(margin=dict(l=0, r=0, t=0, b=0))
    c1.markdown(f"**{nom} : {len(crit)} stations critiques sur {len(ok)}**")
    c1.plotly_chart(m, use_container_width=True)
    c2.plotly_chart(city_bars("pct_stations_critiques", "Part des stations critiques", "{:.1f}", " %"),
                    use_container_width=True)
    st.markdown("**Stations à rééquilibrer en priorité**")
    cols = ["station_nom", "score_criticite", "pct_vide_pointe", "pct_pleine_pointe", "action", "activite_jour"]
    st.dataframe(crit.nlargest(15, "score_criticite")[cols],
                 hide_index=True, use_container_width=True, column_config={
                     "station_nom": "Station", "action": "Action",
                     "score_criticite": st.column_config.NumberColumn("Criticité (%)", format="%.0f"),
                     "pct_vide_pointe": st.column_config.NumberColumn("Vide en pointe (%)", format="%.0f"),
                     "pct_pleine_pointe": st.column_config.NumberColumn("Pleine en pointe (%)", format="%.0f"),
                     "activite_jour": st.column_config.NumberColumn("Mouvements / jour", format="%.0f")})
    lib.method("criticité = % des heures de pointe (jours ouvrés, 7 h-10 h et 17 h-20 h) où la station est vide + % "
               "où elle est pleine ; critique si ≥ 30 %.")
    lib.conclusion("**Marseille (16,5 % de stations critiques) et Lyon (≈ 11 %)** en tête, **Paris < 1 %** "
                   "(régulation efficace). La plus critique : **Damrémont - Caulaincourt** au sommet de Montmartre, "
                   "vide 88 % des heures de pointe — les usagers descendent la butte mais ne la remontent pas.")
lib.source("CityBikes dumps (ODbL) — « Bike-share data by CityBikes contributors »")
