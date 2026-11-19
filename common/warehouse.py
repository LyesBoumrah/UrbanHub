"""Client de l'entrepôt (ClickHouse) : insert_df, query_df.

Responsable : commun.
"""

from __future__ import annotations

import pandas as pd
import clickhouse_connect

from .config import settings
from .log import setup_logging

log = setup_logging(__name__)


class Warehouse:
    """Encapsule la connexion ClickHouse et les opérations courantes."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
    ) -> None:
        self.client = clickhouse_connect.get_client(
            host=host or settings.clickhouse_host,
            port=port or settings.clickhouse_port,
            username=user or settings.clickhouse_user,
            password=password or settings.clickhouse_password,
            database=database or settings.clickhouse_db,
        )

    def command(self, sql: str) -> None:
        self.client.command(sql)

    def query_df(self, sql: str, parameters: dict | None = None) -> pd.DataFrame:
        return self.client.query_df(sql, parameters=parameters or {})

    def insert_df(self, table: str, df: pd.DataFrame) -> None:
        if df.empty:
            log.warning("insert_df(%s) : DataFrame vide, rien à insérer", table)
            return
        self.client.insert_df(table, df)
