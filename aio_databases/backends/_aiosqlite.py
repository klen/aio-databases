from __future__ import annotations

from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl

import aiosqlite

from . import RE_PARAM, ABCDatabaseBackend
from .common import Connection

if TYPE_CHECKING:
    from collections.abc import Callable


class Backend(ABCDatabaseBackend[aiosqlite.Connection]):
    name = "aiosqlite"
    db_type = "sqlite"
    connection_cls = Connection

    def __init__(
        self,
        url,
        isolation_level: str | None = None,
        init: Callable | None = None,
        pragmas: tuple[tuple[str, str], ...] | None = None,
        functions: tuple[tuple[str, int, Callable], ...] | None = None,
        **options,
    ):
        """Set a default isolation level (enable autocommit). Fix in memory URL."""
        will_be_uri = options.get("uri") or any(k == "uri" for k, _ in parse_qsl(url.query))
        if not will_be_uri and ":memory:" in url.path:
            url = url._replace(path="")

        if init is None and (pragmas or functions):

            async def init_conn(conn):
                for pragma, value in pragmas or []:
                    await conn.execute(f"PRAGMA {pragma} = {value};")

                for name, num_params, func in functions or []:
                    await conn.create_function(name, num_params, func)
                return conn

            init = init_conn

        super().__init__(url, isolation_level=isolation_level, init=init, **options)

        if self.options.get("uri"):
            self._clean_uri_options(options)
            self._database = self._build_database()
        else:
            self._database = self.url.path

    def _clean_uri_options(self, explicit_options):
        """Remove URL-derived query keys from options (they belong in filename)."""
        url_query_keys = {k for k, _ in parse_qsl(self.url.query)}
        for key in url_query_keys:
            if key == "uri" or key in explicit_options:
                continue
            self.options.pop(key, None)

    def _build_database(self) -> str:
        database = self.url.netloc + self.url.path
        database = database.lstrip("/")
        if self.url.query:
            database += "?" + self.url.query
        return database

    def __convert_sql__(self, sql: Any) -> str:
        sql = str(sql)
        if self.convert_params:
            sql = RE_PARAM.sub(r"\1?", sql)
        return sql

    async def _acquire(self) -> aiosqlite.Connection:
        return await aiosqlite.connect(database=self._database, **self.options)

    async def release(self, conn: aiosqlite.Connection):
        await conn.commit()
        await conn.close()
