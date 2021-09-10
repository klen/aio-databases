from __future__ import annotations

import typing as t

from uuid import uuid4

import aiosqlite

from . import ABCDatabaseBackend, ABCConnection, ABCTransaction, RE_PARAM
from ..record import Record


class Transaction(ABCTransaction):

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


class Connection(ABCConnection):

    transaction_cls = Transaction

    async def _execute(self, query: str, *params, **options) -> t.Any:
        async with self.conn.cursor() as cursor:
            await cursor.execute(query, params)
            if cursor.lastrowid == 0:
                return cursor.rowcount
            return cursor.lastrowid

    async def _executemany(self, query: str, *params, **options) -> t.Any:
        async with self.conn.cursor() as cursor:
            await cursor.executemany(query, params)
            #  for args in params:
            #      await cursor.execute(query, args)
            if cursor.lastrowid == 0:
                return cursor.rowcount
            return cursor.lastrowid

    async def _fetchall(self, query: str, *params, **options) -> t.List[t.Mapping]:
        async with self.conn.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            desc = cursor.description
            return [Record(row, desc) for row in rows]

    async def _fetchmany(self, size: int, query: str, *params, **options) -> t.List[t.Mapping]:
        async with self.conn.execute(query, params) as cursor:
            rows = await cursor.fetchmany(size)
            desc = cursor.description
            return [Record(row, desc) for row in rows]

    async def _fetchone(self, query: str, *params, **options) -> t.Optional[t.Mapping]:
        async with self.conn.execute(query, params) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return row

            return Record(row, cursor.description)

    async def _fetchval(self, query: str, *params, column: t.Any = 0, **options) -> t.Any:
        row = await self.fetchone(query, *params)
        if row:
            return row[column]

    async def _iterate(self, query: str, *params, **options) -> t.AsyncIterator[Record]:
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, params)
            while True:
                row = await cursor.fetchone()
                if row is None:
                    break
                yield Record(row, cursor.description)

        finally:
            await cursor.close()


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
