from __future__ import annotations

import typing as t

import aiopg

from . import ABCDatabaseBackend
from .common import Connection as Connection_


class Connection(Connection_):

    async def _executemany(self, query: str, *params, **options) -> t.Any:
        async with self._conn.cursor() as cursor:
            for args_ in params:
                await cursor.execute(query, args_, **options)


class Backend(ABCDatabaseBackend):

    name = 'aiopg'
    db_type = 'postgresql'
    connection_cls = Connection

    pool: t.Optional[aiopg.Pool] = None

    async def connect(self) -> None:
        dsn = self.url._replace(scheme='postgresql').geturl()
        self.pool = await aiopg.create_pool(dsn, **self.options)

    async def disconnect(self) -> None:
        pool = self.pool
        assert pool is not None, "Database is not connected"
        self.pool = None
        pool.close()
        await pool.wait_closed()

    async def acquire(self) -> aiopg.Connection:
        pool = self.pool
        assert pool is not None, "Database is not connected"
        return await pool.acquire()

    async def release(self, conn: aiopg.Connection):
        pool = self.pool
        assert pool is not None, "Database is not connected"
        await pool.release(conn)
