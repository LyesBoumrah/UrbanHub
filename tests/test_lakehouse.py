"""Tests unitaires Lakehouse — client ClickHouse, DDL `sql/00..40`, init_warehouse et sync Gold.

Responsable : commun / Lakehouse.
"""

from __future__ import annotations

import pandas as pd

from common.alerts import emit_alert
from common.storage import LocalBronzeStorage
from common.warehouse import ClickHouseWarehouse
from p4_croise.build_gold import build_gold
from scripts.init_warehouse import SQL_DIR, apply_sql_files, init_lakehouse, seed_ref_ville, split_sql_statements


class FakeClickHouseClient:
    """Stub léger remplaçant clickhouse_connect pour tester toute la couche Lakehouse hors Docker."""

    def __init__(self) -> None:
        self.commands: list[str] = []
        self.inserted_dfs: dict[str, list[pd.DataFrame]] = {}
        self.inserted_rows: dict[str, list[tuple[list[str], list[tuple]]]] = {}

    def command(self, sql: str, parameters=None):
        del parameters
        self.commands.append(sql)
        return 1

    def insert_df(self, table: str, df: pd.DataFrame):
        self.inserted_dfs.setdefault(table, []).append(df.copy())

    def insert(self, table: str, data, column_names):
        self.inserted_rows.setdefault(table, []).append((list(column_names), list(data)))

    def query_df(self, sql: str, parameters=None) -> pd.DataFrame:
        del parameters
        if "count()" in sql:
            return pd.DataFrame({"n": [42]})
        return pd.DataFrame({"ok": [1]})


def _make_fake_warehouse() -> tuple[ClickHouseWarehouse, FakeClickHouseClient]:
    wh = ClickHouseWarehouse(host="clickhouse", port=8123, user="u", password="p", database="urbanhub")
    fake = FakeClickHouseClient()
    wh._client = fake
    return wh, fake


def test_sql_files_lakehouse_complets():
    """Vérifie que les 5 scripts SQL (commun + P1 à P4) sont présents et valides."""
    files = sorted(p.name for p in SQL_DIR.glob("*.sql"))
    assert files == [
        "00_commun.sql",
        "10_p1_meteo.sql",
        "20_p2_velos.sql",
        "30_p3_pollution.sql",
        "40_gold.sql",
    ]
    for path in sorted(SQL_DIR.glob("*.sql")):
        raw = path.read_text(encoding="utf-8")
        assert "FINAL AS " not in raw, f"{path.name} : en ClickHouse l'alias doit précéder FINAL (AS alias FINAL)"
        stmts = split_sql_statements(raw)
        assert len(stmts) >= 2, f"{path.name} devrait contenir au moins 2 instructions SQL"


def test_init_lakehouse_ddl_et_ref_ville(tmp_path):
    """Vérifie l'initialisation complète MinIO + DDL ClickHouse + seed ref_ville (12 villes)."""
    wh, fake = _make_fake_warehouse()
    st = LocalBronzeStorage(base_dir=tmp_path)

    res = init_lakehouse(use_local=False, storage=st, warehouse=wh)
    assert res["villes"] == 12
    assert res["clickhouse_ready"] is True
    assert len(res["sql_files"]) == 5
    assert "urbanhub.ref_ville" in fake.inserted_rows
    cols, rows = fake.inserted_rows["urbanhub.ref_ville"][0]
    assert len(rows) == 12
    assert "ville_id" in cols and "noaa_station_id" in cols
    assert any("CREATE TABLE IF NOT EXISTS urbanhub.gold_ville_heure" in cmd for cmd in fake.commands)
    assert any("CREATE OR REPLACE VIEW urbanhub.v_gold_synthese_villes" in cmd for cmd in fake.commands)


def test_clickhouse_warehouse_et_alertes(tmp_path):
    """Vérifie ping, table_count, insert_df, apply_sql_files et double persistance des alertes."""
    wh, fake = _make_fake_warehouse()
    st = LocalBronzeStorage(base_dir=tmp_path)

    assert wh.ping() is True
    assert wh.table_count("urbanhub.meteo_obs") == 42
    assert len(apply_sql_files(wh)) == 5
    assert seed_ref_ville(wh) == 12

    emit_alert(
        pipeline="p4_croise",
        niveau="WARNING",
        ville_id="paris",
        message="Test alerte Lakehouse",
        details={"score": 78},
        storage=st,
        warehouse=wh,
    )
    assert len(st.list("gold/alertes/")) == 1
    assert "urbanhub.pipeline_alertes" in fake.inserted_rows


def test_build_gold_synchronise_minio_et_clickhouse(tmp_path):
    """Vérifie que build_gold écrit à la fois les JSON Gold et la table ClickHouse `gold_ville_heure`."""
    from tests.test_p4_croise import _sample_tripartite_data

    wh, fake = _make_fake_warehouse()
    st = LocalBronzeStorage(base_dir=tmp_path)
    meteo_df, velos_df, _ = _sample_tripartite_data()

    meteo_obs = meteo_df.assign(
        station_id="07149099999",
        timestamp=meteo_df["heure_utc"],
        wind_direction=210.0,
        latitude=48.8566,
        longitude=2.3522,
        quality_flags="",
        is_valid=1,
    )
    st.put_parquet("silver/meteo/meteo_obs.parquet", meteo_obs)
    st.put_parquet(
        "silver/velos/velo_releve_20260501.parquet",
        velos_df.assign(network_id="velib", station_name="Châtelet", latitude=48.858, longitude=2.347),
    )

    res = build_gold(use_local=True, storage=st, warehouse=wh)
    assert res["gold_rows"] == 72
    assert res["clickhouse_gold_rows"] == 72
    assert "urbanhub.gold_ville_heure" in fake.inserted_dfs
    assert len(fake.inserted_dfs["urbanhub.gold_ville_heure"][0]) == 72
    assert all(k.endswith(".json") for k in st.list("gold/"))
