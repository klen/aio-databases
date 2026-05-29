from __future__ import annotations

from typing import TYPE_CHECKING, Any

import aiosqlite

from aio_databases.log import logger as base_logger

from . import RE_PARAM, ABCDatabaseBackend
from .common import Connection

if TYPE_CHECKING:
    import logging
    from collections.abc import Callable


class Backend(ABCDatabaseBackend[aiosqlite.Connection]):
    name = "aiosqlite"
    db_type = "sqlite"
    connection_cls = Connection

    def __init__(  # noqa: PLR0913
        self,
        url,
        *,
        uri: bool = False,
        convert_params: bool = False,
        init: Callable | None = None,
        isolation_level: str | None = None,
        logger: logging.Logger = base_logger,
        pragmas: tuple[tuple[str, str], ...] | None = None,
        functions: tuple[tuple[str, int, Callable], ...] | None = None,
        **options,
    ):
        """Set a default isolation level (enable autocommit). Fix in memory URL."""
        if not uri and ":memory:" in url.path:
            url = url._replace(path="")

        if init is None and (pragmas or functions):

            async def init_conn(conn):
                for pragma, value in pragmas or []:
                    await conn.execute(f"PRAGMA {pragma} = {value};")

                for name, num_params, func in functions or []:
                    await conn.create_function(name, num_params, func)
                return conn

            init = init_conn

        super().__init__(url, init=init, logger=logger, convert_params=convert_params, **options)

        if uri:
            self.options = {**options, "isolation_level": isolation_level, "uri": True}

        self._database = self._build_database() if self.options.get("uri") else self.url.path

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
