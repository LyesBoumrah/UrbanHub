"""Client analytique ClickHouse du Data Lakehouse UrbanHub : insert_df, insert_rows, query_df, execute.

Complète le Data Lake MinIO S3 (buckets `bronze`, `silver`, `gold`) en exposant les couches
Silver et Gold sous forme de tables MergeTree / ReplacingMergeTree et de vues SQL métier.

Responsable : commun.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Sequence

from common.config import Settings, get_settings
from common.log import get_logger

logger = get_logger(__name__)


class ClickHouseWarehouse:
    """Client ClickHouse thread-safe basé sur `clickhouse-connect` (import paresseux)."""

    def __init__(
        self,
        settings: Settings | None = None,
        database: str | None = None,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self._host = host or self.settings.clickhouse_host
        self._port = port if port is not None else self.settings.clickhouse_port
        self._user = user or self.settings.clickhouse_user
        self._password = password or self.settings.clickhouse_password
        self.database = database if database is not None else self.settings.clickhouse_db
        self._client: Any = None
        self._lock = threading.RLock()

    @property
    def client(self) -> Any:
        if self._client is None:
            with self._lock:
                if self._client is None:
                    import clickhouse_connect

                    kwargs: dict[str, Any] = {
                        "host": self._host,
                        "port": self._port,
                        "username": self._user,
                        "password": self._password,
                    }
                    if self.database:
                        kwargs["database"] = self.database
                    self._client = clickhouse_connect.get_client(**kwargs)
        return self._client

    def ping(self) -> bool:
        try:
            self.client.command("SELECT 1")
            return True
        except Exception as exc:  # noqa: BLE001
            logger.debug("ClickHouse injoignable (%s:%s) : %s", self._host, self._port, exc)
            return False

    def execute(self, sql: str, parameters: dict[str, Any] | None = None) -> Any:
        with self._lock:
            return self.client.command(sql, parameters=parameters)

    command = execute

    def insert_df(self, table: str, df: Any) -> int:
        if df is None or len(df) == 0:
            return 0
        with self._lock:
            self.client.insert_df(table=table, df=df)
        return len(df)

    def insert_rows(
        self,
        table: str,
        rows: Sequence[Sequence[Any]],
        column_names: Sequence[str],
    ) -> int:
        if not rows:
            return 0
        with self._lock:
            self.client.insert(table=table, data=rows, column_names=list(column_names))
        return len(rows)

    def query_df(self, sql: str, parameters: dict[str, Any] | None = None) -> Any:
        with self._lock:
            return self.client.query_df(sql, parameters=parameters)

    def table_count(self, table: str) -> int:
        """Retourne le nombre de lignes d'une table ClickHouse (0 si absente/injoignable)."""
        try:
            df = self.query_df(f"SELECT count() AS n FROM {table}")
            return int(df["n"].iloc[0]) if not df.empty else 0
        except Exception:  # noqa: BLE001
            return 0

    def query_df_by_year_parallel(
        self,
        sql_template: str,
        years: Sequence[int],
        max_workers: int = 5,
    ) -> Any:
        """Exécute une requête SQL paramétrée par `{year}` en parallèle (un client dédié par thread)."""
        import pandas as pd

        unique_years = sorted({int(y) for y in years})
        if not unique_years:
            return pd.DataFrame()

        def _run_year(year: int) -> Any:
            worker_wh = ClickHouseWarehouse(
                settings=self.settings,
                database=self.database,
                host=self._host,
                port=self._port,
                user=self._user,
                password=self._password,
            )
            return worker_wh.query_df(sql_template.format(year=year))

        workers = max(1, min(int(max_workers), len(unique_years)))
        frames_by_year: dict[int, Any] = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_run_year, y): y for y in unique_years}
            for fut in as_completed(futures):
                y = futures[fut]
                df_y = fut.result()
                if df_y is not None and len(df_y) > 0:
                    frames_by_year[y] = df_y

        ordered = [frames_by_year[y] for y in unique_years if y in frames_by_year]
        return pd.concat(ordered, ignore_index=True) if ordered else pd.DataFrame()


Warehouse = ClickHouseWarehouse


def get_warehouse(settings: Settings | None = None, database: str | None = None) -> ClickHouseWarehouse:
    """Fabrique un client ClickHouseWarehouse."""
    return ClickHouseWarehouse(settings=settings, database=database)


def try_get_warehouse(settings: Settings | None = None, database: str | None = None) -> ClickHouseWarehouse | None:
    """Retourne un client ClickHouse prêt si le serveur répond au ping, sinon None (mode dégradé/local)."""
    wh = get_warehouse(settings=settings, database=database)
    return wh if wh.ping() else None
