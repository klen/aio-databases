from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Tuple

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
        pragmas: Optional[Tuple[Tuple[str, str]]] = None,
        **kwargs,
    ):
        """Set a default isolation level (enable autocommit). Fix in memory URL."""
        if ":memory:" in url.path:
            url = url._replace(path="")
        elif url.path:
            url = url._replace(path=url.path[1:])

        if init is None and pragmas is not None:
            _pragmas = pragmas

            async def init(conn):
                for pragma, value in _pragmas:
                    await conn.execute(f"PRAGMA {pragma} = {value};")
                return conn

        super(Backend, self).__init__(url, isolation_level=isolation_level, init=init, **kwargs)

    def __convert_sql__(self, sql: Any) -> str:
        sql = str(sql)
        if self.convert_params:
            sql = RE_PARAM.sub(r"\1?", sql)
        return sql

    async def connect(self) -> None:
        self.logger.warning("'aiosqlite' doesn't support pools")

    async def disconnect(self) -> None:
        pass

    async def _acquire(self) -> aiosqlite.Connection:
        return await aiosqlite.connect(database=self.url.path, **self.options)

    async def release(self, conn: aiosqlite.Connection):
        await conn.commit()
        await conn.close()
