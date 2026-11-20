"""Dashboard — P3 Pollution, organisé par question métier (gold_station_pollution, gold_pollution_profil,
gold_ville_jour, gold_kpi_ville). Responsable : P3."""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import lib
from common import viz

if not lib.page("🏭 Pollution de l'air", "Archive OpenAQ (stations de référence EEA), 55 stations, 5 ans — "
                "valeurs guides de l'OMS (2021)."):
    st.stop()
ville, d1, d2 = lib.sidebar()
nom = lib.VILLES[ville]
per = lib.period_sql("date_locale", d1, d2)
st.caption("Valeurs guides OMS 2021 — annuel : PM2.5 5 µg/m³, PM10 15, NO2 10 · 24 h : PM2.5 15, PM10 45, NO2 25 · "
           "O3 (max 8 h) 100.")

q1, q2, q3, q4 = st.tabs(["Q1 · Zones les plus polluées", "Q2 · Variation journalière", "Q3 · Polluants dominants (OMS)",
                          "Q4 · Épisodes"])
sp = lib.q(f"SELECT * FROM urbanhub.gold_station_pollution WHERE ville_id = '{ville}'")

with q1:
    pol = st.radio("Polluant", [p for p in lib.POLLUANTS if p in set(sp["polluant"])], format_func=lib.POLLUANTS.get,
                   horizontal=True)
    d = sp[sp["polluant"] == pol].sort_values("moyenne")
    c1, c2 = st.columns([1.2, 1])
    f = px.bar(d, x="moyenne", y="station_nom", orientation="h", color="type_station", text=d["moyenne"].round(1),
               color_discrete_map={"trafic": viz.ORANGE, "fond urbain": viz.BLUE}, hover_data={"p95": ":.1f"},
               labels={"moyenne": "µg/m³ (moyenne 5 ans)", "station_nom": "", "type_station": "type", "p95": "P95"},
               title=f"{nom} : {lib.POLLUANTS[pol]} par station")
    if pol in lib.OMS_ANNUEL:
        f.add_vline(x=lib.OMS_ANNUEL[pol], line_dash="dash", line_color=viz.INK_2, annotation_text="OMS annuel",
                    annotation_font_color=viz.INK_2)
    f.update_traces(textposition="outside")
    c1.plotly_chart(f, use_container_width=True)
    m = px.scatter_map(d, lat="latitude", lon="longitude", color="moyenne", color_continuous_scale=lib.SEQ, zoom=10,
                       height=420, map_style="carto-positron", hover_name="station_nom",
                       hover_data={"moyenne": ":.1f", "type_station": True, "latitude": False, "longitude": False})
    m.update_traces(marker=dict(size=16))
    m.update_layout(margin=dict(l=0, r=0, t=0, b=0))
    c2.plotly_chart(m, use_container_width=True)
    k = lib.q("SELECT ville_id, no2_moy, pm25_moy FROM urbanhub.gold_kpi_ville")
    c1, c2 = st.columns(2)
    for col, var, titre, who in ((c1, "no2_moy", "NO2 moyen par ville (µg/m³)", 10),
                                 (c2, "pm25_moy", "PM2.5 moyen par ville (µg/m³)", 5)):
        dd = k.sort_values(var)
        f = px.bar(dd, x=var, y="ville", orientation="h", text=dd[var].round(1), title=titre,
                   labels={var: "", "ville": ""})
        f.update_traces(marker_color=[viz.ORANGE if v == ville else viz.BLUE for v in dd["ville_id"]],
                        textposition="outside")
        f.add_vline(x=who, line_dash="dash", line_color=viz.INK_2, annotation_text="OMS")
        f.update_layout(height=420)
        col.plotly_chart(f, use_container_width=True)
    st.caption("Type de station déduit du nom (« périphérique », « A7 », « Proxi »…) : approximation.")
    lib.conclusion("NO2 le plus élevé à **Paris (27,4), Montpellier (27,2), Lyon (25,2) et Marseille (24,0)**. Les "
                   "stations les plus polluées sont **au bord des grands axes** (périphériques de Paris et de Lyon, "
                   "A35, A7) : les stations « trafic » mesurent **2 fois plus de NO2** que le fond urbain. Les PM2.5 "
                   "varient peu entre villes (7 à 10,5) : pollution de fond.")

with q2:
    pr = lib.q(f"""SELECT polluant, type_jour, heure_locale, valeur_moy FROM urbanhub.gold_pollution_profil
                   WHERE ville_id = '{ville}' AND polluant IN ('no2', 'o3', 'pm25', 'pm10')""")
    pr["polluant"] = pr["polluant"].map(lib.POLLUANTS)
    f = px.line(pr, x="heure_locale", y="valeur_moy", color="type_jour", facet_col="polluant", facet_col_wrap=4,
                color_discrete_map={"semaine": viz.BLUE, "week-end": viz.ORANGE},
                labels={"valeur_moy": "µg/m³", "heure_locale": "heure", "type_jour": ""},
                title=f"{nom} : profil journalier moyen (heure locale)")
    f.update_yaxes(matches=None, showticklabels=True)
    f.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    f.update_layout(height=360)
    st.plotly_chart(f, use_container_width=True)
    lib.conclusion("le **NO2** suit le trafic : double pic **8-9 h / 19-21 h** et **−14 % le week-end**. L'**ozone** a "
                   "le profil inverse, maximum à **15-16 h** (formé par le soleil). Les **PM2.5** montent le soir "
                   "(chauffage), les PM10 ont un pic vers 10 h les jours ouvrés seulement.")

with q3:
    k = lib.q("SELECT ville_id, pm25_moy, pm10_moy, no2_moy FROM urbanhub.gold_kpi_ville ORDER BY no2_moy DESC")
    ratio = pd.DataFrame({lib.POLLUANTS[p]: k[f"{p}_moy"] / v for p, v in lib.OMS_ANNUEL.items()})
    ratio.index = k["ville"]
    f = go.Figure(go.Heatmap(z=ratio.values, x=ratio.columns, y=ratio.index, colorscale=lib.SEQ, zmin=0.8, zmax=3,
                             text=[[f"× {v:.1f}" for v in row] for row in ratio.values], texttemplate="%{text}",
                             hovertemplate="%{y} — %{x} : %{z:.2f} × la valeur guide<extra></extra>",
                             colorbar=dict(title="× OMS")))
    f.update_layout(height=470, yaxis_autorange="reversed",
                    title="Moyenne 5 ans ÷ valeur guide annuelle OMS (1 = au niveau de la valeur guide)")
    st.plotly_chart(f, use_container_width=True)
    ex = lib.q(f"""SELECT ville_id, avg(depassement_oms_pm25) * 100 AS pm25, avg(depassement_oms_no2) * 100 AS no2,
                          avg(depassement_oms_o3) * 100 AS o3, avg(depassement_oms_pm10) * 100 AS pm10
                   FROM urbanhub.gold_ville_jour WHERE {per} GROUP BY ville_id ORDER BY no2 DESC""")
    st.markdown(f"**Part des jours où la moyenne de la ville dépasse la valeur guide 24 h — {lib.period_label(d1, d2)}**")
    st.dataframe(ex[["ville", "no2", "pm25", "pm10", "o3"]], hide_index=True, use_container_width=True,
                 column_config={"ville": "Ville", **{c: st.column_config.NumberColumn(
                     f"% jours {lib.POLLUANTS[c]}", format="%.1f") for c in ("no2", "pm25", "pm10", "o3")}})
    lib.method("ici, moyenne de la ville (toutes stations) ; le notebook 03 compte aussi les jours où au moins une "
               "station dépasse (ex. NO2 : 91 % des jours à Lyon, 88 % à Paris, à cause des stations de trafic).")
    lib.conclusion("**les 12 villes dépassent les valeurs guides de l'OMS pour les PM2.5 (× 1,4 à 2,1) et le NO2 "
                   "(× 1,2 à 2,7)**. Polluant dominant : **NO2 dans les grandes agglomérations** (Paris, Montpellier, "
                   "Lyon, Marseille, Strasbourg), **PM2.5 ailleurs**. Les moyennes de ville restent sous les valeurs "
                   "limites européennes, mais **les périphériques de Paris et de Lyon dépassent celle du NO2**.")

with q4:
    pol2 = st.radio("Polluant ", ["pm25", "no2", "o3", "pm10"], format_func=lib.POLLUANTS.get, horizontal=True)
    cal = lib.q(f"""SELECT date_locale, countIf(depassement_oms_{pol2} = 1) AS villes
                    FROM urbanhub.gold_ville_jour WHERE {per} GROUP BY date_locale ORDER BY date_locale""")
    seuil = {"pm25": "24 h > 15", "no2": "24 h > 25", "o3": "max 8 h > 100", "pm10": "24 h > 45"}
    f = go.Figure(go.Bar(x=cal["date_locale"], y=cal["villes"], marker_color=viz.BLUE,
                         hovertemplate="%{x|%d/%m/%Y} : %{y} ville(s)<extra></extra>"))
    f.update_layout(height=320, yaxis=dict(range=[0, 12.5], title="villes"),
                    title=f"Villes au-dessus de la valeur guide OMS ({lib.POLLUANTS[pol2]} {seuil[pol2]} µg/m³)")
    st.plotly_chart(f, use_container_width=True)
    col = "o3_max_8h" if pol2 == "o3" else pol2
    pires = lib.q(f"SELECT date_locale, {col} AS valeur FROM urbanhub.gold_ville_jour WHERE ville_id = '{ville}' "
                  f"AND {per} AND {col} IS NOT NULL ORDER BY {col} DESC LIMIT 8")
    st.markdown(f"**{nom} : les jours les plus pollués ({lib.POLLUANTS[pol2]}) de la période**")
    st.dataframe(pires.drop(columns="ville", errors="ignore"), hide_index=True, use_container_width=True,
                 column_config={"date_locale": st.column_config.DateColumn("Date", format="DD/MM/YYYY"),
                                "valeur": st.column_config.NumberColumn("µg/m³", format="%.1f")})
    st.caption("Peu d'épisodes en 2022 : en partie un effet des trous de l'archive OpenAQ sur 2021-2023 "
               "(on ne compare pas le nombre d'épisodes d'une année à l'autre).")
    lib.conclusion("**particules fines et NO2 : épisodes d'hiver** (9-16 janvier 2024 : les 12 villes à la fois ; "
                   "février 2023). **Ozone : épisodes d'été, pendant les vagues de chaleur** de la partie météo "
                   "(7-26 juillet 2026 et 27-31 mai 2026 : les 12 villes).")
lib.source("OpenAQ archive (EEA France)")
