from __future__ import annotations

from typing import Any, Optional

import aioodbc

from . import RE_PARAM, ABCDatabaseBackend
from .common import Connection


class DBType:
    def __get__(self, obj, **_):
        if obj is None:
            return "odbc"

        return obj.options["dsn"]


class Backend(ABCDatabaseBackend[aioodbc.Connection]):
    name = "aioodbc"
    db_type = "odbc"
    connection_cls = Connection

    def __init__(self, *args, db_type: Optional[str] = None, **kwargs):
        self.db_type = db_type or self.db_type
        super(Backend, self).__init__(*args, **kwargs)

    def __convert_sql__(self, sql: Any) -> str:
        sql = str(sql)
        if self.convert_params:
            sql = RE_PARAM.sub(r"\1?", sql)
        return sql

    async def _acquire(self):  # type: ignore[]
        return aioodbc.connect(**self.options)

    async def release(self, conn: aioodbc.Connection):
        await conn.close()


class PoolBackend(Backend):
    name = "aioodbc+pool"

    _pool: Optional[aioodbc.Pool] = None

    def __init__(self, *args, db_type: Optional[str] = None, **kwargs):
        super(PoolBackend, self).__init__(*args, **kwargs)
        self.pool_options = {
            name: self.options.pop(name)
            for name in ("minsize", "maxsize", "pool_recycle")
            if name in self.options
        }

    async def connect(self) -> None:
        self.pool = await aioodbc.create_pool(**self.options, **self.pool_options)

    async def disconnect(self) -> None:
        pool = self.pool
        pool.close()
        await pool.wait_closed()

    async def _acquire(self):
        return await self.pool.acquire()

    async def release(self, conn: aioodbc.Connection):
        await self.pool.release(conn)
