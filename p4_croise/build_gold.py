"""ÉTAPE 3 — LOAD : silver (Parquet dans Silo) → couche GOLD dans ClickHouse.

10 tables (schéma : sql/40_gold.sql), toutes RECALCULÉES depuis le silver avec les fonctions d'analyse
testées des parties P1 à P4 — les chiffres du gold sont donc exactement ceux des notebooks :

    gold_ville_heure        croisement ville × heure (+ score de favorabilité)        séries, ML
    gold_ville_jour         synthèse ville × jour (météo, pollution, OMS, vélos)       tableaux de bord
    gold_station_velo       indicateurs par station de vélos (statut, criticité…)      P2
    gold_zone_velo          zones de ~500 m (sous-offre)                                P2
    gold_velo_profil        activité par jour de semaine × heure                        P2
    gold_station_pollution  indicateurs par station de mesure (moyenne, P95, OMS)      P3
    gold_pollution_profil   profil horaire par polluant (semaine / week-end)           P3
    gold_kpi_ville          indicateurs clés par ville                                  accueil
    gold_couverture         couverture des données (source × ville × mois)            page Qualité
    gold_pipeline           volumes et état de chaque couche du pipeline               page Qualité

Chargement IDEMPOTENT et sans coupure : chaque table est remplie dans une copie <nom>__new, puis échangée
d'un coup avec l'ancienne (EXCHANGE TABLES) — le dashboard ne voit jamais de table vide ou à moitié remplie.

Usage : python -m p4_croise.build_gold

Responsable : commun (Load de l'ETL local).
"""
from __future__ import annotations

import re
import time
from pathlib import Path

import numpy as np
import pandas as pd

from common.config import load_villes
from common.log import get_logger
from common.silver import read_silver, read_table
from p2_velos import analytics as p2
from p3_pollution import analytics as p3
from p4_croise import analytics as p4

ROOT = Path(__file__).resolve().parents[1]
SQL_FILE = ROOT / "sql" / "40_gold.sql"
DB = "urbanhub"

log = get_logger("gold")


# ---------------------------------------------------------------- construction des tables (fonctions pures)

def build_ville_heure(cr: pd.DataFrame) -> pd.DataFrame:
    g = cr.copy()
    g["score_favorabilite"] = p4.favorability(g).astype("uint8")
    g["date_locale"] = pd.to_datetime(g["date_locale"]).dt.date
    return g


def build_ville_jour(cr: pd.DataFrame, cday: pd.DataFrame, v: pd.DataFrame) -> pd.DataFrame:
    d = cr.assign(fav=(p4.favorability(cr) == 3) & cr["heure_locale"].between(7, 20))
    g = d.groupby(["ville_id", "date_locale"]).agg(
        saison=("saison", "first"), est_weekend=("est_weekend", "first"),
        t_moy=("temperature_c", "mean"), t_min=("temperature_c", "min"), t_max=("temperature_c", "max"),
        pluie_mm=("precipitation_mm", "sum"), vent_moy_ms=("vent_ms", "mean"), vent_max_ms=("vent_ms", "max"),
        humidite_pct=("humidite_pct", "mean"), heures_favorables=("fav", "sum"),
        velo_activite_totale=("activite_totale", lambda s: s.sum(min_count=1)),
        velo_activite_par_station=("activite_par_station", "mean"),
        velo_remplissage_moyen=("remplissage_moyen", "mean"),
        velo_part_stations_vides=("part_stations_vides", "mean")).reset_index()
    # pollution : moyennes journalières de ville sur jours complets (≥ 18 h), comme en P3
    pol = cday.assign(date_locale=cday["date"].dt.strftime("%Y-%m-%d"))
    wide = pol.pivot_table(index=["ville_id", "date_locale"], columns="polluant", values="moyenne")
    o3max = pol[pol["polluant"] == "o3"].set_index(["ville_id", "date_locale"])["o3_8h"].rename("o3_max_8h")
    g = g.merge(wide.reset_index(), on=["ville_id", "date_locale"], how="left") \
         .merge(o3max.reset_index(), on=["ville_id", "date_locale"], how="left")
    for p, col, seuil in (("pm25", "pm25", 15), ("pm10", "pm10", 45), ("no2", "no2", 25), ("o3", "o3_max_8h", 100)):
        if col not in g:
            g[col] = np.nan
        g[f"depassement_oms_{p}"] = np.where(g[col].isna(), np.nan, (g[col] > seuil).astype(float))
    for p in ("pm25", "pm10", "no2", "o3", "co"):
        if p not in g:
            g[p] = np.nan
    vi = v[v["heure_locale"].between(7, 20)].groupby(["ville_id", "date_locale"])["i_velo"].mean()
    g = g.merge(vi.rename("velo_indice_activite").reset_index(), on=["ville_id", "date_locale"], how="left")
    g["date_locale"] = pd.to_datetime(g["date_locale"]).dt.date
    g["heures_favorables"] = g["heures_favorables"].astype("uint8")
    return g


def build_station_velo(ind: pd.DataFrame) -> pd.DataFrame:
    s = ind.copy()
    ok = s["statut_station"] == "exploitable"
    s["score_criticite"] = (s["pct_vide_pointe"] + s["pct_pleine_pointe"]).where(ok)
    crit = ok & (s["score_criticite"] >= 30)
    s["action"] = np.where(~crit, "aucune",
                           np.where(s["pct_vide_pointe"] >= s["pct_pleine_pointe"], "réapprovisionner",
                                    "désengorger"))
    s["rang_activite_ville"] = 0
    rang = s[ok].groupby("ville_id")["activite_jour"].rank(ascending=False, method="first")
    s.loc[ok, "rang_activite_ville"] = rang
    return s.rename(columns={"jours": "jours_observes", "rempl_matin": "remplissage_matin",
                             "rempl_soir": "remplissage_soir"}).assign(
        station_nom=lambda x: x["station_nom"].fillna(""),
        rang_activite_ville=lambda x: x["rang_activite_ville"].astype("uint32"))


def build_zone_velo(cells: pd.DataFrame) -> pd.DataFrame:
    z = cells.copy()
    z["zone_id"] = z["cell_lat"].astype(str) + "_" + z["cell_lon"].astype(str)
    z["sous_offre"] = z["pct_vide"] >= 15
    return z


def build_velo_profil(week: pd.DataFrame) -> pd.DataFrame:
    w = week.rename(columns={"wd": "jour_semaine", "h": "heure_locale"}).copy()
    w["indice"] = w["activite_station_h"] / w.groupby("ville_id")["activite_station_h"].transform("mean")
    return w


def build_station_pollution(st: pd.DataFrame) -> pd.DataFrame:
    s = st.rename(columns={"type": "type_station"}).copy()
    s["ratio_oms"] = s["moyenne"] / s["polluant"].map(p3.WHO_ANNUAL)
    s["station_nom"] = s["station_nom"].fillna("")
    return s


def build_pollution_profil(d: pd.DataFrame) -> pd.DataFrame:
    x = d.assign(type_jour=np.where(d["wd"] >= 5, "week-end", "semaine"))
    return x.groupby(["ville_id", "polluant", "type_jour", "h"])["valeur"].mean().rename("valeur_moy") \
            .reset_index().rename(columns={"h": "heure_locale"})


def build_kpi_ville(villes, jour, st_pol, cday, ind, cells, cr, v) -> pd.DataFrame:
    k = pd.DataFrame(villes)[["ville_id", "nom", "lat", "lon"]].rename(columns={"lat": "latitude",
                                                                                "lon": "longitude"})
    years = (pd.to_datetime(jour["date_locale"]).max() - pd.to_datetime(jour["date_locale"]).min()).days / 365.25
    met = jour.groupby("ville_id").agg(t_moy_c=("t_moy", "mean"), pluie_tot=("pluie_mm", "sum"))
    met["pluie_mm_an"] = met.pop("pluie_tot") / years
    cm = p3.city_means(st_pol).add_suffix("_moy")
    ratio = p3.who_ratio(st_pol)
    ex = p3.exceedance_days(cday).pivot(index="ville_id", columns="polluant", values="pct_jours")
    u = p2.usable(ind)
    zones = cells.groupby("ville_id")["pct_vide"].apply(lambda s: round(100 * (s >= 15).mean(), 1))
    k = (k.merge(met.reset_index(), on="ville_id", how="left")
          .merge(p4.favorable_share(cr).reset_index(), on="ville_id", how="left")
          .merge(cm.reset_index(), on="ville_id", how="left")
          .merge(pd.DataFrame({"ratio_oms_pm25": ratio.get("pm25"), "ratio_oms_no2": ratio.get("no2"),
                               "polluant_dominant": p3.dominant_pollutant(ratio)}).reset_index(), on="ville_id",
                 how="left")
          .merge(ex.reindex(columns=["pm25", "no2", "o3"]).add_prefix("pct_jours_depassement_").reset_index(),
                 on="ville_id", how="left")
          .merge(st_pol.groupby("ville_id")["station_id"].nunique().rename("stations_pollution").reset_index(),
                 on="ville_id", how="left")
          .merge(u.groupby("ville_id").size().rename("stations_velo").reset_index(), on="ville_id", how="left")
          .merge(ind[ind["statut_station"] == "inactive"].groupby("ville_id").size()
                 .rename("stations_velo_inactives").reset_index(), on="ville_id", how="left")
          .merge(p2.concentration(u)[["ville_id", "activite_jour_station"]]
                 .rename(columns={"activite_jour_station": "velo_activite_station_jour"}), on="ville_id",
                 how="left")
          .merge(zones.rename("pct_zones_sous_offre").reset_index(), on="ville_id", how="left")
          .merge(p2.critical_share(u)[["ville_id", "pct_critiques"]]
                 .rename(columns={"pct_critiques": "pct_stations_critiques"}), on="ville_id", how="left")
          .merge(p4.rain_effect_by_city(v)["effet_pluie_pct"].rename("effet_pluie_velo_pct").reset_index(),
                 on="ville_id", how="left"))
    for c in ("stations_pollution", "stations_velo", "stations_velo_inactives"):
        k[c] = k[c].fillna(0).astype("uint32")
    k["polluant_dominant"] = k["polluant_dominant"].fillna("")
    return k


def build_couverture(meteo: pd.DataFrame, pollution: pd.DataFrame, velos: pd.DataFrame) -> pd.DataFrame:
    """Unifie les 3 tables silver/_couverture/*.parquet : source × ville × détail × mois."""
    cols = ["source", "ville_id", "detail", "mois", "pct_presentes", "pct_valides"]
    m = meteo.assign(source="météo", detail="")[cols]
    p = (pollution.groupby(["ville_id", "polluant", "mois"])[["pct_presentes", "pct_valides"]].mean()
         .reset_index().rename(columns={"polluant": "detail"}).assign(source="pollution"))
    v = velos.assign(source="vélos", detail="",
                     pct_valides=100 * velos["heures_valides"] / velos["heures_station_attendues"])
    out = pd.concat([m, p, v[["source", "ville_id", "detail", "mois", "pct_presentes", "pct_valides"]]],
                    ignore_index=True)
    out["mois"] = pd.to_datetime(out["mois"] + "-01").dt.date
    out[["pct_presentes", "pct_valides"]] = out[["pct_presentes", "pct_valides"]].clip(upper=100).round(1)
    return out


PIPELINE = {  # (couche, source) -> (préfixe Silo, préfixe du manifeste, champ lignes, champ lignes valides)
    ("bronze", "météo"): ("bronze/meteo/", "bronze/meteo/_manifests/", None, None),
    ("bronze", "vélos"): ("bronze/velos_historique/", "bronze/velos_historique/_manifests/", None, None),
    ("bronze", "pollution"): ("bronze/openaq_archive/", "bronze/openaq_archive/_manifests/", None, None),
    ("silver", "météo"): ("silver/meteo/", "silver/_manifests/meteo_", "lignes_silver", "lignes_valides"),
    ("silver", "vélos"): ("silver/velos/", "silver/_manifests/velos_", "lignes_silver", None),
    ("silver", "pollution"): ("silver/pollution/", "silver/_manifests/pollution_", "lignes_silver",
                              "lignes_valides"),
    ("silver", "croisé P4"): ("silver/croise/", "silver/_manifests/croise_", "lignes", None),
}


def build_pipeline(storage) -> pd.DataFrame:
    """Volumes (objets, Mo) de chaque couche × source dans Silo + lignes et date du dernier manifeste."""
    rows = []
    for (couche, source), (prefix, man_prefix, f_lignes, f_valides) in PIPELINE.items():
        objs = [o for o in storage.client.list_objects(storage.bucket, prefix=prefix, recursive=True)
                if "/_manifests/" not in o.object_name]
        mans = sorted(k for k in storage.list(man_prefix) if k.endswith(".json"))
        man = storage.get_json(mans[-1]) if mans else {}
        rows.append({"couche": couche, "source": source, "objets": len(objs),
                     "taille_mo": round(sum(o.size for o in objs) / 1e6, 1),
                     "lignes": man.get(f_lignes) if f_lignes else None,
                     "lignes_valides": man.get(f_valides) if f_valides else None,
                     "derniere_maj": man.get("generated_at") or man.get("started_at") or ""})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- chargement ClickHouse

def ddl_statements(sql_text: str) -> dict:
    """Instructions CREATE TABLE du fichier SQL, indexées par nom de table."""
    stmts = [s.strip() for s in re.sub(r"--[^\n]*", "", sql_text).split(";") if s.strip()]
    out = {}
    for s in stmts:
        m = re.search(r"CREATE TABLE IF NOT EXISTS\s+\w+\.(\w+)", s)
        if m:
            out[m.group(1)] = s
    return out


def to_clickhouse(df: pd.DataFrame, columns: list[tuple[str, str]]) -> pd.DataFrame:
    """Réordonne / convertit les colonnes selon le schéma ClickHouse (NaN → NULL pour les Nullable)."""
    out = pd.DataFrame(index=df.index)
    for name, typ in columns:
        if name not in df:
            raise KeyError(f"colonne {name} absente du DataFrame")
        s = df[name]
        if typ.startswith("Nullable"):
            s = s.astype(object).where(s.notna(), None)
        elif typ.startswith("DateTime"):
            s = pd.to_datetime(s, utc=True)
        elif "Int" in typ:
            s = s.astype("int64")
        elif typ == "Bool":
            s = s.astype(bool)
        out[name] = s
    return out.reset_index(drop=True)


def load_table(client, name: str, ddl: str, df: pd.DataFrame) -> int:
    """Charge `df` dans urbanhub.<name> via une copie <name>__new puis EXCHANGE TABLES (atomique)."""
    client.command(ddl)
    tmp = f"{DB}.{name}__new"
    client.command(f"DROP TABLE IF EXISTS {tmp}")
    client.command(f"CREATE TABLE {tmp} AS {DB}.{name}")
    cols = client.query(f"SELECT name, type FROM system.columns WHERE database = '{DB}' AND table = '{name}__new' "
                        "ORDER BY position").result_rows
    data = to_clickhouse(df, cols)
    client.insert_df(tmp, data)
    client.command(f"EXCHANGE TABLES {DB}.{name} AND {tmp}")
    client.command(f"DROP TABLE {tmp}")
    n = client.command(f"SELECT count() FROM {DB}.{name}")
    if int(n) != len(df):
        raise RuntimeError(f"{name} : {n} lignes chargées au lieu de {len(df)}")
    return int(n)


def build_all(storage) -> dict:
    t0 = time.monotonic()
    villes = load_villes()
    log.info("Lecture du silver…")
    cr = read_silver(storage, "silver/croise/")
    d = p3.valid(read_silver(storage, "silver/pollution/", filename="pollution_heure.parquet"))
    st_pol = p3.station_stats(d, read_table(storage, "silver/pollution/stations.parquet"))
    cday = p3.city_daily(p3.daily(d))
    agg = p2.load_aggregates(storage, read_table)
    ind = p2.station_indicators(agg, read_table(storage, "silver/velos/stations.parquet"), villes)
    cells = p2.grid_cells(p2.usable(ind))
    v = p4.velo_frame(cr)
    log.info("Silver lu en %.0f s ; construction des tables gold…", time.monotonic() - t0)

    jour = build_ville_jour(cr, cday, v)
    tables = {
        "gold_ville_heure": build_ville_heure(cr),
        "gold_ville_jour": jour,
        "gold_station_velo": build_station_velo(ind),
        "gold_zone_velo": build_zone_velo(cells),
        "gold_velo_profil": build_velo_profil(p2.weekly_profile(agg)),
        "gold_station_pollution": build_station_pollution(st_pol),
        "gold_pollution_profil": build_pollution_profil(d),
        "gold_kpi_ville": build_kpi_ville(villes, jour, st_pol, cday, ind, cells, cr, v),
        "gold_couverture": build_couverture(read_table(storage, "silver/_couverture/meteo.parquet"),
                                            read_table(storage, "silver/_couverture/pollution.parquet"),
                                            read_table(storage, "silver/_couverture/velos.parquet")),
        "gold_pipeline": build_pipeline(storage),
    }
    return tables


def main():
    from common.storage import get_storage
    from common.warehouse import get_warehouse

    t0 = time.monotonic()
    tables = build_all(get_storage())
    ddl = ddl_statements(SQL_FILE.read_text(encoding="utf-8"))
    client = get_warehouse(database="").client
    client.command(f"CREATE DATABASE IF NOT EXISTS {DB}")
    for name, df in tables.items():
        n = load_table(client, name, ddl[name], df)
        log.info("%-24s %9d lignes chargées", name, n)
    log.info("LOAD terminé en %.0f s : %d tables gold dans ClickHouse (%s)", time.monotonic() - t0, len(tables), DB)


if __name__ == "__main__":
    main()
