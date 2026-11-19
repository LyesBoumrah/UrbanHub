"""Client de l'entrepôt (ClickHouse) : insert_df, insert_rows, query_df, execute.

Responsable : commun.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Sequence

from common.config import Settings, get_settings
from common.log import get_logger

logger = get_logger(__name__)


class ClickHouseWarehouse:
    """Wrapper ClickHouse utilisant clickhouse-connect (import paresseux)."""

    def __init__(self, settings: Settings | None = None, database: str | None = None) -> None:
        self.settings = settings or get_settings()
        self.database = database if database is not None else self.settings.clickhouse_db
        self._client: Any = None

    @property
    def client(self) -> Any:
        if self._client is None:
            import clickhouse_connect

            kwargs: dict[str, Any] = {
                "host": self.settings.clickhouse_host,
                "port": self.settings.clickhouse_port,
                "username": self.settings.clickhouse_user,
                "password": self.settings.clickhouse_password,
            }
            if self.database:
                kwargs["database"] = self.database
            self._client = clickhouse_connect.get_client(**kwargs)
        return self._client

    def ping(self) -> bool:
        try:
            self.client.command("SELECT 1")
            return True
        except Exception as exc:
            logger.warning("ClickHouse injoignable : %s", exc)
            return False

    def execute(self, sql: str, parameters: dict[str, Any] | None = None) -> Any:
        return self.client.command(sql, parameters=parameters)

    def insert_df(self, table: str, df: Any) -> int:
        if df is None or len(df) == 0:
            return 0
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
        self.client.insert(table=table, data=rows, column_names=list(column_names))
        return len(rows)

    def query_df(self, sql: str, parameters: dict[str, Any] | None = None) -> Any:
        return self.client.query_df(sql, parameters=parameters)

    def query_df_by_year_parallel(
        self,
        sql_template: str,
        years: Sequence[int],
        max_workers: int = 5,
    ) -> Any:
        """Exécute une requête SQL paramétrée par `{year}` en parallèle (un thread/client par année)."""
        import pandas as pd

        unique_years = sorted({int(y) for y in years})
        if not unique_years:
            return pd.DataFrame()

        def _run_year(year: int) -> Any:
            worker_wh = ClickHouseWarehouse(settings=self.settings, database=self.database)
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


def get_warehouse(settings: Settings | None = None, database: str | None = None) -> ClickHouseWarehouse:
    """Fabrique un client ClickHouseWarehouse."""
    return ClickHouseWarehouse(settings=settings, database=database)
