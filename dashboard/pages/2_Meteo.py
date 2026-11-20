"""Dashboard — P1 Météo, organisé par question métier (gold_ville_jour, gold_ville_heure). Responsable : P1."""
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import lib
from common import viz

if not lib.page("🌦️ Météo", "Open-Meteo (réanalyse ERA5), 12 villes, 5 ans."):
    st.stop()
ville, d1, d2 = lib.sidebar()
nom = lib.VILLES[ville]
per = lib.period_sql("date_locale", d1, d2)

q1, q2, q3, q4 = st.tabs(["Q1 · Périodes anormales", "Q2 · Corrélations", "Q3 · Saisonnalité", "Q4 · Extrêmes"])

with q1:
    st.subheader(f"Périodes météorologiques anormales — {nom}")
    a = lib.q(f"""
        WITH d AS (SELECT date_locale, t_moy FROM urbanhub.gold_ville_jour WHERE ville_id = '{ville}'),
             n AS (SELECT toDayOfYear(date_locale) AS doy, avg(t_moy) AS t_norm FROM d GROUP BY doy)
        SELECT d.date_locale, d.t_moy, d.t_moy - n.t_norm AS anomalie
        FROM d JOIN n ON toDayOfYear(d.date_locale) = n.doy
        WHERE {lib.period_sql('d.date_locale', d1, d2)} ORDER BY d.date_locale""")
    f = go.Figure(go.Bar(x=a["date_locale"], y=a["anomalie"],
                         marker_color=[viz.WARM if v > 0 else viz.COOL for v in a["anomalie"]],
                         hovertemplate="%{x|%d/%m/%Y} : %{y:+.1f} °C<extra></extra>"))
    f.update_layout(height=340, yaxis_title="écart à la normale (°C)",
                    title="Écart journalier à la normale (rouge : plus chaud, bleu : plus froid)")
    st.plotly_chart(f, use_container_width=True)
    lib.method("normale = température moyenne du même jour de l'année sur les 5 ans. Le notebook 01 lisse la "
               "normale sur ± 7 jours et détecte les épisodes (≥ 3 jours, |z| ≥ 1,5).")
    c1, c2 = st.columns(2)
    cfg = {"date_locale": st.column_config.DateColumn("Date", format="DD/MM/YYYY"),
           "t_moy": st.column_config.NumberColumn("T moy (°C)", format="%.1f"),
           "anomalie": st.column_config.NumberColumn("Écart (°C)", format="%+.1f")}
    c1.markdown("**Jours les plus chauds par rapport à la normale**")
    c1.dataframe(a.nlargest(6, "anomalie"), hide_index=True, use_container_width=True, column_config=cfg)
    c2.markdown("**Jours les plus froids par rapport à la normale**")
    c2.dataframe(a.nsmallest(6, "anomalie"), hide_index=True, use_container_width=True, column_config=cfg)
    lib.conclusion("349 épisodes détectés dans les 12 villes (172 chauds, 177 froids). Les trois plus longs épisodes "
                   "**nationaux** sont des **vagues de chaleur en 2026** (23-30 mai : les 12 villes ; 21-27 juin ; "
                   "6-13 juillet). Le froid le plus marqué : **décembre 2022** (Rouen, 10 jours à −7 °C sous la "
                   "normale).")

with q2:
    st.subheader(f"Corrélations entre variables météo — {nom}")
    h = lib.q(f"""SELECT temperature_c, humidite_pct, precipitation_mm, pression_hpa, nebulosite_pct, vent_ms
                  FROM urbanhub.gold_ville_heure WHERE ville_id = '{ville}' AND {per}""")
    h = h.drop(columns="ville", errors="ignore")
    corr = h.corr(method="spearman").round(2)
    lab = ["température", "humidité", "pluie", "pression", "nébulosité", "vent"]
    f = go.Figure(go.Heatmap(z=corr.values, x=lab, y=lab, zmin=-1, zmax=1, colorscale=lib.DIV,
                             text=corr.values, texttemplate="%{text:+.2f}", colorbar=dict(title="ρ"),
                             hovertemplate="%{y} ↔ %{x} : %{z:+.2f}<extra></extra>"))
    f.update_layout(height=470, yaxis_autorange="reversed",
                    title=f"Corrélation de Spearman ({len(h):,} heures)".replace(",", " "))
    st.plotly_chart(f, use_container_width=True)
    lib.method("la visibilité, demandée par le sujet, est absente de l'archive Open-Meteo : la question est traitée "
               "comme une analyse des corrélations entre les variables disponibles.")
    lib.conclusion("le lien le plus fort est **température ↔ humidité (ρ ≈ −0,58 sur les 12 villes)** : cycle jour / "
                   "nuit. La pluie va avec la nébulosité, une **pression basse** et le **vent** : la signature des "
                   "dépressions. Les corrélations restent modérées : chaque variable apporte une information propre.")

with q3:
    st.subheader("Évolution saisonnière de la température")
    villes = st.multiselect("Villes comparées (3 au plus)", list(lib.VILLES), max_selections=3,
                            default=list(dict.fromkeys([ville, "marseille", "strasbourg"]))[:3],
                            format_func=lib.VILLES.get)
    m = lib.q(f"""SELECT ville_id, toStartOfMonth(date_locale) AS mois, avg(t_moy) AS t_moy
                  FROM urbanhub.gold_ville_jour WHERE ville_id IN ({lib.sql_in(villes or [ville])}) AND {per}
                  GROUP BY ville_id, mois ORDER BY mois""")
    f = px.line(m, x="mois", y="t_moy", color="ville", labels={"t_moy": "température moyenne (°C)", "mois": ""},
                color_discrete_sequence=viz.SERIES)
    f.update_traces(hovertemplate="%{x|%b %Y} : %{y:.1f} °C")
    st.plotly_chart(f, use_container_width=True)
    k = lib.q("SELECT ville_id, t_moy_c, pluie_mm_an FROM urbanhub.gold_kpi_ville")
    c1, c2 = st.columns(2)
    for col, var, titre, fmt in ((c1, "t_moy_c", "Température moyenne 5 ans (°C)", ".1f"),
                                 (c2, "pluie_mm_an", "Pluie (mm / an)", ".0f")):
        d = k.sort_values(var)
        f = px.bar(d, x=var, y="ville", orientation="h", text=d[var].map(lambda v: format(v, fmt)), title=titre,
                   labels={var: "", "ville": ""})
        f.update_traces(marker_color=[viz.ORANGE if v == ville else viz.BLUE for v in d["ville_id"]],
                        textposition="outside")
        f.update_layout(height=420)
        col.plotly_chart(f, use_container_width=True)
    st.caption(f"En orange : {nom}. Les cumuls de pluie d'une réanalyse surestiment les petites pluies : "
               "à lire en comparaison.")
    lib.conclusion("climat **océanique** à l'ouest (amplitude annuelle 14,5 °C à Rennes) contre **continental** "
                   "(20,2 °C à Lyon). Sur des années comparables (novembre → octobre), **2025-26 est la plus chaude "
                   "(14,8 °C, ≈ +1,3 °C), dans les 12 villes**. 2021-22 a été la plus sèche, 2023-24 la plus "
                   "pluvieuse.")

with q4:
    st.subheader(f"Jours extrêmes — {lib.period_label(d1, d2)}")
    rec = lib.q(f"""
      SELECT 'Chaleur (T max, °C)' AS type, argMax(ville_id, t_max) AS ville_id, argMax(date_locale, t_max) AS date,
             max(t_max) AS valeur FROM urbanhub.gold_ville_jour WHERE {per}
      UNION ALL SELECT 'Froid (T min, °C)', argMin(ville_id, t_min), argMin(date_locale, t_min), min(t_min)
             FROM urbanhub.gold_ville_jour WHERE {per}
      UNION ALL SELECT 'Vent (max horaire, m/s)', argMax(ville_id, vent_max_ms), argMax(date_locale, vent_max_ms),
             max(vent_max_ms) FROM urbanhub.gold_ville_jour WHERE {per}
      UNION ALL SELECT 'Pluie (cumul du jour, mm)', argMax(ville_id, pluie_mm), argMax(date_locale, pluie_mm),
             max(pluie_mm) FROM urbanhub.gold_ville_jour WHERE {per}""")
    st.markdown("**Records des 12 villes sur la période**")
    for col, (_, r) in zip(st.columns(4), rec.iterrows()):
        col.metric(r["type"], f"{r['valeur']:.1f}", f"{r['ville']} — {r['date']:%d/%m/%Y}", delta_color="off")
    j = lib.q(f"SELECT date_locale, t_max, t_min, vent_max_ms, pluie_mm FROM urbanhub.gold_ville_jour "
              f"WHERE ville_id = '{ville}' AND {per}")
    st.markdown(f"**{nom} : jours extrêmes de la période** (au-delà du percentile 99 ou en deçà du 1 de la ville)")
    rows = []
    for nomx, col, haut in (("chaleur", "t_max", True), ("froid", "t_min", False), ("vent", "vent_max_ms", True),
                            ("pluie", "pluie_mm", True)):
        s = j[col]
        seuil = s.quantile(0.99 if haut else 0.01)
        n = int((s >= seuil).sum() if haut else (s <= seuil).sum())
        rows.append({"type": nomx, "seuil": round(seuil, 1), "jours": n,
                     "record": round(s.max() if haut else s.min(), 1)})
    st.dataframe(rows, hide_index=True, use_container_width=True)
    lib.conclusion("records sur 5 ans : **41,9 °C** à Montpellier (08/07/2026), **−11 °C** à Strasbourg (17/12/2022), "
                   "**19,4 m/s** à Lille (18/02/2022, tempête Eunice), **180 mm** à Marseille (20/10/2026). La chaleur "
                   "extrême n'existe que de juin à septembre ; la pluie extrême a deux pics : juin (orages) et "
                   "l'automne (épisodes méditerranéens).")
lib.source("Open-Meteo Archive")
