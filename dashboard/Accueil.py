"""Dashboard Streamlit — accueil : indicateurs clés des 12 villes (couche gold).

Lancement : streamlit run dashboard/Accueil.py
Responsable : commun.
"""
import plotly.express as px
import streamlit as st

import lib

if not lib.page("🏙️ UrbanHub — Jumeau numérique urbain",
                "Météo, vélos en libre-service et pollution de l'air dans 12 villes françaises — "
                "5 ans d'archives (01/11/2021 → 31/10/2026), pipeline bronze → silver → gold."):
    st.stop()
lib.sidebar(period=False)

kpi = lib.q("SELECT * FROM urbanhub.gold_kpi_ville")
tot = lib.q("""SELECT count() AS heures, min(heure_utc) AS debut, max(heure_utc) AS fin,
                      countIf(a_velos) AS heures_velo FROM urbanhub.gold_ville_heure""").iloc[0]
mesures = int(lib.q("SELECT sum(heures) AS n FROM urbanhub.gold_station_pollution").iloc[0]["n"])

c = st.columns(5)
c[0].metric("Villes", len(kpi))
c[1].metric("Heures ville × heure", f"{tot['heures']:,}".replace(",", " "))
c[2].metric("Stations vélo analysées", f"{int(kpi['stations_velo'].sum()):,}".replace(",", " "))
c[3].metric("Stations de mesure de l'air", int(kpi["stations_pollution"].sum()))
c[4].metric("Mesures de pollution", f"{mesures / 1e6:.1f} M")

st.subheader("Carte des villes")
indic = {
    "NO2 moyen (µg/m³)": "no2_moy", "PM2.5 moyen (µg/m³)": "pm25_moy",
    "Activité vélo (mouvements / station / jour)": "velo_activite_station_jour",
    "Heures favorables au vélo (%)": "pct_heures_favorables", "Température moyenne (°C)": "t_moy_c",
}
choix = st.radio("Indicateur", list(indic), horizontal=True)
col = indic[choix]
fig = px.scatter_map(kpi, lat="latitude", lon="longitude", color=col, size=[18] * len(kpi), size_max=18,
                     hover_name="ville", hover_data={col: ":.1f", "latitude": False, "longitude": False},
                     color_continuous_scale=lib.SEQ, zoom=4.6, center={"lat": 46.6, "lon": 2.4}, height=520,
                     map_style="carto-positron", labels={col: choix})
fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
st.plotly_chart(fig, use_container_width=True)

st.subheader("Indicateurs clés par ville")
tab = kpi[["ville", "t_moy_c", "pluie_mm_an", "pct_heures_favorables", "no2_moy", "pm25_moy", "ratio_oms_no2",
           "ratio_oms_pm25", "polluant_dominant", "velo_activite_station_jour", "pct_zones_sous_offre",
           "pct_stations_critiques", "effet_pluie_velo_pct"]].sort_values("no2_moy", ascending=False)
st.dataframe(tab, hide_index=True, use_container_width=True, column_config={
    "ville": "Ville", "t_moy_c": st.column_config.NumberColumn("T moy. (°C)", format="%.1f"),
    "pluie_mm_an": st.column_config.NumberColumn("Pluie (mm/an)", format="%.0f"),
    "pct_heures_favorables": st.column_config.NumberColumn("Heures favorables vélo (%)", format="%.1f"),
    "no2_moy": st.column_config.NumberColumn("NO2 (µg/m³)", format="%.1f"),
    "pm25_moy": st.column_config.NumberColumn("PM2.5 (µg/m³)", format="%.1f"),
    "ratio_oms_no2": st.column_config.NumberColumn("NO2 × OMS", format="× %.1f"),
    "ratio_oms_pm25": st.column_config.NumberColumn("PM2.5 × OMS", format="× %.1f"),
    "polluant_dominant": "Polluant dominant",
    "velo_activite_station_jour": st.column_config.NumberColumn("Vélo : mvts / station / jour", format="%.0f"),
    "pct_zones_sous_offre": st.column_config.NumberColumn("Zones en sous-offre (%)", format="%.1f"),
    "pct_stations_critiques": st.column_config.NumberColumn("Stations critiques (%)", format="%.1f"),
    "effet_pluie_velo_pct": st.column_config.NumberColumn("Effet pluie sur le vélo (%)", format="%.0f"),
})

st.subheader("Ce qu'il faut retenir")
st.markdown("""
- **Climat** : 2025-26 est l'année la plus chaude des cinq (≈ +1,3 °C), avec trois vagues de chaleur nationales en
  mai-juillet 2026 ; records : 41,9 °C à Montpellier, −11 °C à Strasbourg.
- **Vélos** : rythme domicile-travail identique partout (pics à 8 h et 17-18 h) ; usage le plus intense à **Lyon** ;
  sous-offre forte à **Marseille** ; le centre se remplit dans la journée, la première couronne se vide.
- **Pollution** : les **12 villes dépassent les valeurs guides de l'OMS** pour les PM2.5 et le NO2 ; le NO2 domine
  dans les grandes agglomérations, au bord des grands axes ; particules l'hiver, ozone l'été.
- **Croisements** : le vent divise le NO2 par 3, la pluie lave les particules (−36 %), la chaleur fabrique l'ozone ;
  la pluie fait baisser l'usage du vélo jusqu'à −36 %, mais la pollution ne fait pas fuir les cyclistes.
""")
lib.source(f"Open-Meteo, CityBikes, OpenAQ — du {tot['debut']:%d/%m/%Y} au {tot['fin']:%d/%m/%Y}")
