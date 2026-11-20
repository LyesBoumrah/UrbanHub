"""Dashboard — fiche « Ma ville » : tous les indicateurs d'une ville, comparée aux 11 autres. Responsable : commun."""
import plotly.graph_objects as go
import streamlit as st

import lib
from common import viz

if not lib.page("📍 Ma ville", "Fiche de synthèse : climat, qualité de l'air, vélos — et rang parmi les 12 villes."):
    st.stop()
ville, d1, d2 = lib.sidebar()
nom = lib.VILLES[ville]
st.header(f"{nom} — {lib.period_label(d1, d2)}")

kpi = lib.q("SELECT * FROM urbanhub.gold_kpi_ville")
k = kpi.set_index("ville_id").loc[ville]


def rang(col: str, plus_haut_est_premier: bool = True) -> str:
    r = kpi[col].rank(ascending=not plus_haut_est_premier, method="min")[kpi["ville_id"] == ville].iloc[0]
    return f"{int(r)}ᵉ / 12" if r > 1 else "1ʳᵉ / 12"


jour = lib.q(f"""SELECT * FROM urbanhub.gold_ville_jour WHERE ville_id = '{ville}'
                 AND {lib.period_sql('date_locale', d1, d2)} ORDER BY date_locale""")

st.subheader("🌦️ Climat")
c = st.columns(4)
c[0].metric("Température moyenne (période)", f"{jour['t_moy'].mean():.1f} °C",
            f"5 ans : {k['t_moy_c']:.1f} °C — {rang('t_moy_c')} plus chaude", delta_color="off")
c[1].metric("Pluie (5 ans)", f"{k['pluie_mm_an']:.0f} mm/an", rang("pluie_mm_an") + " plus pluvieuse",
            delta_color="off")
c[2].metric("Heures favorables au vélo", f"{k['pct_heures_favorables']:.1f} %",
            rang("pct_heures_favorables") + " plus favorable", delta_color="off")
c[3].metric("Jours ≥ 30 °C (période)", int((jour["t_max"] >= 30).sum()))

st.subheader("🏭 Qualité de l'air")
c = st.columns(4)
c[0].metric("NO2 moyen (5 ans)", f"{k['no2_moy']:.1f} µg/m³", f"× {k['ratio_oms_no2']:.1f} la valeur guide OMS — "
            f"{rang('no2_moy')} plus exposée", delta_color="off")
c[1].metric("PM2.5 moyen (5 ans)", f"{k['pm25_moy']:.1f} µg/m³",
            f"× {k['ratio_oms_pm25']:.1f} la valeur guide OMS", delta_color="off")
c[2].metric("Polluant dominant", k["polluant_dominant"] or "—")
jours_pol = jour["depassement_oms_pm25"].notna().sum()
c[3].metric("Jours PM2.5 > OMS (période)", f"{int(jour['depassement_oms_pm25'].sum())}",
            f"sur {jours_pol} jours mesurés", delta_color="off")

st.subheader("🚲 Vélos en libre-service")
c = st.columns(4)
c[0].metric("Mouvements / station / jour", f"{k['velo_activite_station_jour']:.0f}",
            rang("velo_activite_station_jour") + " plus utilisée", delta_color="off")
c[1].metric("Stations exploitables", int(k["stations_velo"]), f"{int(k['stations_velo_inactives'])} hors service",
            delta_color="off")
c[2].metric("Stations critiques", f"{k['pct_stations_critiques']:.1f} %", rang("pct_stations_critiques")
            + " (1ʳᵉ = la plus touchée)", delta_color="off")
c[3].metric("Effet de la pluie sur l'usage", f"{k['effet_pluie_velo_pct']:.0f} %",
            "activité sous la pluie vs temps sec", delta_color="off")

st.subheader("Évolution sur la période")
c1, c2 = st.columns(2)
f = go.Figure(go.Scatter(x=jour["date_locale"], y=jour["t_moy"], mode="lines", line=dict(color=viz.BLUE, width=1.5),
                         hovertemplate="%{x|%d/%m/%Y} : %{y:.1f} °C<extra></extra>"))
f.update_layout(title="Température moyenne journalière (°C)", height=300)
c1.plotly_chart(f, use_container_width=True)
f = go.Figure(go.Scatter(x=jour["date_locale"], y=jour["pm25"], mode="lines", line=dict(color=viz.BLUE, width=1.5),
                         hovertemplate="%{x|%d/%m/%Y} : %{y:.1f} µg/m³<extra></extra>", connectgaps=False))
f.add_hline(y=15, line_dash="dash", line_color=viz.INK_2, annotation_text="OMS 24 h")
f.update_layout(title="PM2.5 moyen journalier (µg/m³)", height=300)
c2.plotly_chart(f, use_container_width=True)
velo = jour.dropna(subset=["velo_indice_activite"])
if len(velo):
    f = go.Figure(go.Bar(x=velo["date_locale"], y=velo["velo_indice_activite"], marker_color=viz.BLUE,
                         hovertemplate="%{x|%d/%m/%Y} : indice %{y:.2f}<extra></extra>"))
    f.add_hline(y=1, line_dash="dash", line_color=viz.INK_2)
    f.update_layout(title="Activité vélo journalière (indice, 1 = normale pour la saison et le type de jour)",
                    height=300)
    st.plotly_chart(f, use_container_width=True)
else:
    st.info("Pas de données vélo sur cette période (l'archive CityBikes commence en novembre 2024).")

st.subheader("Alertes et priorités")
c1, c2 = st.columns(2)
pires = jour.dropna(subset=["pm25"]).nlargest(8, "pm25")[["date_locale", "pm25", "no2", "o3_max_8h", "t_max",
                                                          "vent_moy_ms"]]
c1.markdown("**Jours les plus pollués (PM2.5) de la période**")
c1.dataframe(pires, hide_index=True, use_container_width=True, column_config={
    "date_locale": st.column_config.DateColumn("Date", format="DD/MM/YYYY"),
    "pm25": st.column_config.NumberColumn("PM2.5", format="%.1f"), "no2": st.column_config.NumberColumn(
        "NO2", format="%.1f"), "o3_max_8h": st.column_config.NumberColumn("O3 max 8 h", format="%.0f"),
    "t_max": st.column_config.NumberColumn("T max", format="%.1f"),
    "vent_moy_ms": st.column_config.NumberColumn("Vent (m/s)", format="%.1f")})
crit = lib.q(f"""SELECT station_nom, score_criticite, action FROM urbanhub.gold_station_velo
                 WHERE ville_id = '{ville}' AND action != 'aucune' ORDER BY score_criticite DESC LIMIT 8""")
c2.markdown("**Stations vélo à rééquilibrer en priorité**")
if len(crit):
    c2.dataframe(crit, hide_index=True, use_container_width=True, column_config={
        "station_nom": "Station", "score_criticite": st.column_config.NumberColumn("Criticité (%)", format="%.0f"),
        "action": "Action"})
else:
    c2.success("Aucune station critique (vide ou pleine ≥ 30 % des heures de pointe).")
lib.source("gold_kpi_ville, gold_ville_jour, gold_station_velo")
