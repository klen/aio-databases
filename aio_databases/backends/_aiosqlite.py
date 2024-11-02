from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

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
        isolation_level: Optional[str] = None,
        init: Optional[Callable] = None,
        pragmas: Optional[tuple[tuple[str, str], ...]] = None,
        functions: Optional[tuple[tuple[str, int, Callable], ...]] = None,
        **options,
    ):
        """Set a default isolation level (enable autocommit). Fix in memory URL."""
        if ":memory:" in url.path:
            url = url._replace(path="")
        elif url.path:
            url = url._replace(path=url.path[1:])

        if init is None and (pragmas or functions):

            async def init_conn(conn):
                for pragma, value in pragmas or []:
                    await conn.execute(f"PRAGMA {pragma} = {value};")

                for name, num_params, func in functions or []:
                    await conn.create_function(name, num_params, func)
                return conn

            init = init_conn

        super(Backend, self).__init__(url, isolation_level=isolation_level, init=init, **options)

    def __convert_sql__(self, sql: Any) -> str:
        sql = str(sql)
        if self.convert_params:
            sql = RE_PARAM.sub(r"\1?", sql)
        return sql

    async def _acquire(self) -> aiosqlite.Connection:
        return await aiosqlite.connect(database=self.url.path, **self.options)

    async def release(self, conn: aiosqlite.Connection):
        await conn.commit()
        await conn.close()
