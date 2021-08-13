from __future__ import annotations

import typing as t

import aiosqlite

from . import ABCDabaseBackend, ABCConnectionBackend


class SqliteBackend(ABCDabaseBackend):

    name = 'sqlite'

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass

    def connection(self) -> SQLiteConnection:
        return SQLiteConnection(self)

    async def acquire(self) -> aiosqlite.Connection:
        conn = aiosqlite.connect(database=self.url.netloc, isolation_level=None, **self.options)
        await conn.__aenter__()
        return conn

    async def release(self, conn: aiosqlite.Connection) -> None:
        await conn.__aexit__(None, None, None)


class SQLiteConnection(ABCConnectionBackend):

    async def execute(self, query: str, *args, **params) -> t.Any:
        async with self.conn.cursor() as cursor:
            await cursor.execute(query, args)
            if cursor.lastrowid == 0:
                return cursor.rowcount
            return cursor.lastrowid

    async def executemany(self, query: str, *args, **params) -> None:
        for _args in args:
            await self.execute(query, _args, **params)

    async def fetch(self, query: str, *args, **params) -> t.List[t.Tuple]:
        async with self.conn.execute(query, args) as cursor:
            return await cursor.fetchall()

    async def fetchrow(self, query: str, *args, **params) -> t.Optional[t.Tuple]:
        async with self.conn.execute(query, args) as cursor:
            return await cursor.fetchone()

    async def fetchval(self, query: str, *args, column: t.Any = 0, **params) -> t.Any:
        row = await self.fetchrow(query, *args, **params)
        if row:
            return row[column]
