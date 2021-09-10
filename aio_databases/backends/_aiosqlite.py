from __future__ import annotations

import typing as t

import aiosqlite

from . import ABCDatabaseBackend, RE_PARAM
from .common import Connection


class Backend(ABCDatabaseBackend):

    name = 'aiosqlite'
    db_type = 'sqlite'
    connection_cls = Connection

    def __init__(self, url, isolation_level: str = None, **kwargs):
        """Set a default isolation level (enable autocommit). Fix in memory URL."""
        if ':memory:' in url.path:
            url = url._replace(path='')
        elif url.path:
            url = url._replace(path=url.path[1:])

        super(Backend, self).__init__(url, isolation_level=isolation_level, **kwargs)

    def __convert_sql__(self, sql: t.Any) -> str:
        sql = str(sql)
        if self.convert_params:
            sql = RE_PARAM.sub(r'\1?', sql)
        return sql

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass

    async def acquire(self) -> aiosqlite.Connection:
        return await aiosqlite.connect(database=self.url.path, **self.options)

    async def release(self, conn: aiosqlite.Connection):
        await conn.commit()
        await conn.close()
