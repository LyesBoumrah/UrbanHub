"""Outils communs du dashboard : connexion ClickHouse (couche GOLD uniquement), cache, thème des graphiques.

Le dashboard ne lit QUE le gold (ClickHouse) : jamais le bronze ni le silver — comme dans une architecture
de production où la restitution est découplée des traitements.

Responsable : commun.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402
import plotly.graph_objects as go  # noqa: E402
import plotly.io as pio  # noqa: E402
import streamlit as st  # noqa: E402

from common import viz  # noqa: E402
from common.config import load_villes  # noqa: E402

VILLES = {v["ville_id"]: v["nom"] for v in load_villes()}
POLLUANTS = {"pm25": "PM2.5", "pm10": "PM10", "no2": "NO2", "o3": "O3", "co": "CO"}
OMS_ANNUEL = {"pm25": 5, "pm10": 15, "no2": 10}
OMS_24H = {"pm25": 15, "pm10": 45, "no2": 25, "o3": 100}
JOURS = ["lun", "mar", "mer", "jeu", "ven", "sam", "dim"]


# ---------------------------------------------------------------- données (gold)

@st.cache_resource(show_spinner=False)
def client():
    from common.warehouse import get_warehouse

    return get_warehouse().client


@st.cache_data(ttl=900, show_spinner=False)
def q(sql: str) -> pd.DataFrame:
    """Exécute une requête sur le gold ; résultat mis en cache 15 min (même requête = pas de nouvel appel)."""
    df = client().query_df(sql)
    if "ville_id" in df:
        df["ville"] = df["ville_id"].map(VILLES)
    return df


def gold_ok() -> bool:
    try:
        return int(client().command("SELECT count() FROM urbanhub.gold_kpi_ville")) > 0
    except Exception as exc:  # ClickHouse arrêté ou gold absent
        st.error(f"La couche gold est inaccessible ({exc}). Lancez `docker compose up -d clickhouse` puis "
                 "`python -m p4_croise.build_gold`.")
        return False


# ---------------------------------------------------------------- thème des graphiques (palette validée)

pio.templates["urbanhub"] = go.layout.Template(layout=go.Layout(
    colorway=viz.SERIES,
    font=dict(family="Segoe UI, system-ui, sans-serif", size=13, color=viz.INK),
    paper_bgcolor=viz.SURFACE, plot_bgcolor=viz.SURFACE,
    title=dict(font=dict(size=15, color=viz.INK), x=0, xanchor="left"),
    xaxis=dict(gridcolor=viz.GRID, linecolor=viz.AXIS, zeroline=False, tickfont=dict(color=viz.INK_2)),
    yaxis=dict(gridcolor=viz.GRID, linecolor=viz.AXIS, zeroline=False, tickfont=dict(color=viz.INK_2)),
    legend=dict(orientation="h", y=1.02, yanchor="bottom", x=0, title=None, font=dict(color=viz.INK_2)),
    hoverlabel=dict(bgcolor="#ffffff", font=dict(color=viz.INK)),
    margin=dict(l=10, r=10, t=50, b=10),
))
pio.templates.default = "urbanhub"

SEQ = [[0.0, "#cde2fb"], [0.33, "#6da7ec"], [0.66, "#256abf"], [1.0, "#0d366b"]]
DIV = [[0.0, viz.COOL], [0.5, viz.NEUTRAL], [1.0, viz.WARM]]


def page(title: str, subtitle: str) -> bool:
    st.set_page_config(page_title=f"UrbanHub — {title}", page_icon="🏙️", layout="wide")
    st.title(title)
    st.caption(subtitle)
    return gold_ok()


def city_select(label="Ville", default="paris", key=None) -> str:
    ids = list(VILLES)
    return st.selectbox(label, ids, index=ids.index(default), format_func=VILLES.get, key=key)


def cities_select(label="Villes (3 au plus)", default=("paris", "lyon", "marseille"), key=None) -> list[str]:
    sel = st.multiselect(label, list(VILLES), default=list(default), format_func=VILLES.get, max_selections=3,
                         key=key)
    return sel or list(default)[:1]


def sql_in(values) -> str:
    return ", ".join(f"'{v}'" for v in values)


# ---------------------------------------------------------------- filtres globaux (conservés entre les pages)

def sidebar(period: bool = True):
    """Ville + période dans la barre latérale, mémorisées dans st.session_state d'une page à l'autre.

    Renvoie (ville_id, date_debut, date_fin). Les widgets ont une clé propre à la page ; la valeur choisie est
    recopiée dans des clés « persistantes » (f_ville, f_periode) relues par toutes les pages.
    """
    b = q("SELECT min(date_locale) AS d1, max(date_locale) AS d2 FROM urbanhub.gold_ville_jour").iloc[0]
    d1, d2 = pd.Timestamp(b["d1"]).date(), pd.Timestamp(b["d2"]).date()
    ss = st.session_state
    ss.setdefault("f_ville", "paris")
    ss.setdefault("f_periode", (d1, d2))
    ids = list(VILLES)
    with st.sidebar:
        st.header("Filtres")
        ville = st.selectbox("Ville", ids, index=ids.index(ss["f_ville"]), format_func=VILLES.get, key="w_ville")
        ss["f_ville"] = ville
        if period:
            per = st.slider("Période", min_value=d1, max_value=d2, value=ss["f_periode"], format="DD/MM/YYYY",
                            key="w_periode")
            ss["f_periode"] = per
        st.caption("Les filtres sont conservés d'une page à l'autre.")
        st.divider()
        st.caption("Données : couche **gold** (ClickHouse), calculée depuis le silver Parquet (Silo).")
    start, end = ss["f_periode"]
    return ville, start, end


def period_sql(col: str, start, end) -> str:
    return f"{col} BETWEEN toDate('{start}') AND toDate('{end}')"


def period_label(start, end) -> str:
    return f"du {start:%d/%m/%Y} au {end:%d/%m/%Y}"


def conclusion(text: str) -> None:
    """Encadré « conclusion » sous un graphique (conclusions chiffrées des notebooks)."""
    with st.container(border=True):
        st.markdown(f"**📌 Conclusion** — {text}")


def method(text: str) -> None:
    st.caption(f"Méthode : {text}")


def source(text: str) -> None:
    st.caption(f"Source : {text} — couche gold (ClickHouse), calculée depuis le silver Parquet (Silo).")
