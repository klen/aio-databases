from __future__ import annotations

from typing import Optional

from aiomysql import Connection, Pool, connect, create_pool

from . import ABCDatabaseBackend
from .common import Connection as Session


class Backend(ABCDatabaseBackend[Connection]):
    name = "aiomysql"
    db_type = "mysql"
    connection_cls = Session

    pool: Optional[Pool] = None

    def __init__(self, *args, **kwargs):
        super(Backend, self).__init__(*args, **kwargs)
        self.pool_options = {
            name: self.options.pop(name)
            for name in ("minsize", "maxsize", "pool_recycle")
            if name in self.options
        }

    async def connect(self) -> None:
        self.pool = await create_pool(
            **self.options,
            **self.pool_options,
            host=self.url.hostname,
            port=self.url.port,
            user=self.url.username,
            password=self.url.password,
            db=self.url.path.strip("/"),
        )

    async def disconnect(self) -> None:
        pool = self.pool
        assert pool is not None, "Database is not connected"
        self.pool = None
        pool.close()
        await pool.wait_closed()

    async def _acquire(self) -> Connection:
        pool = self.pool
        if pool is None:
            return await connect(
                **self.options,
                host=self.url.hostname,
                port=self.url.port,
                user=self.url.username,
                password=self.url.password,
                db=self.url.path.strip("/"),
            )

        return await pool.acquire()

    async def release(self, conn: Connection):
        pool = self.pool
        if pool is None:
            conn.close()
        else:
            await pool.release(conn)
