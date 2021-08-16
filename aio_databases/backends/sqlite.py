from __future__ import annotations

import typing as t
from uuid import uuid4

import aiosqlite

from . import ABCDabaseBackend, ABCConnection, ABCTransaction


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


class SQLiteConnection(ABCConnection):

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

    def transaction(self) -> SQLiteTransaction:
        return SQLiteTransaction(self)


class SQLiteTransaction(ABCTransaction):

    savepoint: t.Optional[str] = None

    async def _start(self) -> t.Any:
        connection = self.connection
        if connection.transactions:
            self.savepoint = savepoint = f"AIODB_SAVEPOINT_{uuid4().hex}"
            return await connection.execute(f"SAVEPOINT {savepoint}")

        return await connection.execute("BEGIN")

    async def _commit(self) -> t.Any:
        savepoint = self.savepoint
        if savepoint:
            return await self.connection.execute(f"RELEASE SAVEPOINT {savepoint}")

        return await self.connection.execute("COMMIT")

    async def _rollback(self) -> t.Any:
        savepoint = self.savepoint
        if savepoint:
            return await self.connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")

        return await self.connection.execute("ROLLBACK")
