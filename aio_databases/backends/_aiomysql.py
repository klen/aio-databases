from __future__ import annotations

import typing as t

import aiomysql

from . import ABCDatabaseBackend
from .common import Connection


class Backend(ABCDatabaseBackend):

    name = 'aiomysql'
    db_type = 'mysql'
    connection_cls = Connection

    pool: t.Optional[aiomysql.Pool] = None

    async def connect(self) -> None:
        self.pool = await aiomysql.create_pool(
            **self.options,
            host=self.url.hostname,
            port=self.url.port,
            user=self.url.username,
            password=self.url.password,
            db=self.url.path.strip('/'),
        )

    async def disconnect(self) -> None:
        pool = self.pool
        assert pool is not None, "Database is not connected"
        self.pool = None
        pool.close()
        await pool.wait_closed()

    async def acquire(self) -> aiomysql.Connection:
        pool = self.pool
        assert pool is not None, "Database is not connected"
        return await pool.acquire()

    async def release(self, conn: aiomysql.Connection):
        pool = self.pool
        assert pool is not None, "Database is not connected"
        await pool.release(conn)
