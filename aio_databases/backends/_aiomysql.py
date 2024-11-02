from __future__ import annotations

from typing import Optional

from aiomysql import Connection, Pool, connect, create_pool

from . import ABCDatabaseBackend
from .common import Connection as Session


class Backend(ABCDatabaseBackend[Connection]):
    name = "aiomysql"
    db_type = "mysql"
    connection_cls = Session

    def __init__(self, *args, **kwargs):
        super(Backend, self).__init__(*args, **kwargs)
        url = self.url
        self.options["host"] = url.hostname
        self.options["port"] = url.port
        self.options["user"] = url.username
        self.options["password"] = url.password
        self.options["db"] = url.path.strip("/")

    async def _acquire(self) -> Connection:
        return await connect(**self.options)

    async def release(self, conn: Connection):
        conn.close()


class PoolBackend(Backend):
    name = "aiomysql+pool"

    _pool: Optional[Pool] = None

    def __init__(self, *args, **kwargs):
        super(PoolBackend, self).__init__(*args, **kwargs)
        self.pool_options = {
            name: self.options.pop(name)
            for name in ("minsize", "maxsize", "pool_recycle")
            if name in self.options
        }

    async def connect(self) -> None:
        self.pool = await create_pool(**self.options, **self.pool_options)

    async def disconnect(self) -> None:
        pool = self.pool
        pool.close()
        await pool.wait_closed()

    async def _acquire(self) -> Connection:
        return await self.pool.acquire()

    async def release(self, conn: Connection):
        await self.pool.release(conn)
