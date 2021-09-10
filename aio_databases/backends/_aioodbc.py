from __future__ import annotations

import typing as t

import aioodbc

from . import ABCDatabaseBackend, RE_PARAM
from .common import Connection


class DBType:

    def __get__(self, obj, objtype=None):
        if obj is None:
            return 'odbc'
        breakpoint()
        return obj.options['dsn']


class Backend(ABCDatabaseBackend):

    name = 'aioodbc'
    connection_cls = Connection

    pool: t.Optional[aioodbc.Pool] = None
    db_type = 'odbc'

    def __init__(self, *args, db_type: str = None, **kwargs):
        self.db_type = db_type or self.db_type  # type: ignore
        super(Backend, self).__init__(*args, **kwargs)

    def __convert_sql__(self, sql: t.Any) -> str:
        sql = str(sql)
        if self.convert_params:
            sql = RE_PARAM.sub(r'\1?', sql)
        return sql

    async def connect(self) -> None:
        self.pool = await aioodbc.create_pool(**self.options)

    async def disconnect(self) -> None:
        pool = self.pool
        assert pool is not None, "Database is not connected"
        self.pool = None
        pool.close()
        await pool.wait_closed()

    async def acquire(self) -> aioodbc.Connection:
        pool = self.pool
        assert pool is not None, "Database is not connected"
        return await pool.acquire()

    async def release(self, conn: aioodbc.Connection):
        pool = self.pool
        assert pool is not None, "Database is not connected"
        await pool.release(conn)
