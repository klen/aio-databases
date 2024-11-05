from __future__ import annotations

from typing import Any, Optional

from aiopg import Connection, Pool, connect, create_pool

from . import ABCDatabaseBackend
from .common import Connection as Ses


class Session(Ses[Connection]):
    async def _executemany(self, query: str, *params, **options) -> Any:
        conn = self._conn
        assert conn is not None, "Database is not connected"
        async with conn.cursor() as cursor:
            for args_ in params:
                await cursor.execute(query, args_, **options)


class Backend(ABCDatabaseBackend[Connection]):
    name = "aiopg"
    db_type = "postgresql"
    connection_cls = Session

    def __init__(self, url, **kwargs):
        super(Backend, self).__init__(url, **kwargs)
        self.dsn = self.url._replace(scheme="postgresql").geturl()

    async def _acquire(self) -> Connection:
        return await connect(self.dsn, **self.options)

    async def release(self, conn: Connection):
        conn.close()


class PoolBackend(Backend):
    name = "aiopg+pool"

    _pool: Optional[Pool] = None

    def __init__(self, *args, **kwargs):
        super(PoolBackend, self).__init__(*args, **kwargs)
        self.pool_options = {
            name: self.options.pop(name)
            for name in ("minsize", "maxsize", "pool_recycle", "on_connect")
            if name in self.options
        }

    async def connect(self) -> None:
        self.pool = await create_pool(self.dsn, **self.options, **self.pool_options)

    async def disconnect(self) -> None:
        pool = self.pool
        pool.close()
        await pool.wait_closed()

    async def _acquire(self) -> Connection:
        return await self.pool.acquire()

    async def release(self, conn: Connection):
        await self.pool.release(conn)
