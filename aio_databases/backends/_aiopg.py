from __future__ import annotations

import typing as t
from uuid import uuid4

import aiopg

from . import ABCDatabaseBackend, ABCConnection, ABCTransaction
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
        cursor: aiopg.Cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, params, **options)
            if cursor.lastrowid == 0:
                return cursor.rowcount
            return cursor.lastrowid

        finally:
            cursor.close()

    async def _executemany(self, query: str, *params, **options) -> t.Any:
        cursor = await self.conn.cursor()
        try:
            for args_ in params:
                await cursor.execute(query, args_, **options)
        finally:
            cursor.close()

    async def _fetchall(self, query: str, *params, **options) -> t.List[t.Mapping]:
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, params, **options)
            rows = await cursor.fetchall()
            desc = cursor.description
            return [Record(row, desc) for row in rows]

        finally:
            cursor.close()

    async def _fetchmany(self, size: int, query: str, *params, **options) -> t.List[t.Mapping]:
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, params)
            rows = await cursor.fetchmany(size)
            desc = cursor.description
            return [Record(row, desc) for row in rows]

        finally:
            cursor.close()

    async def _fetchone(self, query: str, *params, **options) -> t.Optional[t.Mapping]:
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, params, **options)
            row = await cursor.fetchone()
            if row is None:
                return row

            return Record(row, cursor.description)

        finally:
            cursor.close()

    async def _fetchval(self, query: str, *params, column: t.Any = 0, **options) -> t.Any:
        res = await self.fetchone(query, *params, **options)
        if res:
            res = res[column]
        return res

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
            cursor.close()


class Backend(ABCDatabaseBackend):

    name = 'aiopg'
    db_type = 'postgresql'
    connection_cls = Connection

    pool: t.Optional[aiopg.Pool] = None

    async def connect(self) -> None:
        dsn = self.url._replace(scheme='postgresql').geturl()
        self.pool = await aiopg.create_pool(dsn, **self.options)

    async def disconnect(self) -> None:
        pool = self.pool
        assert pool is not None, "Database is not connected"
        self.pool = None
        pool.close()
        await pool.wait_closed()

    async def acquire(self) -> aiopg.Connection:
        pool = self.pool
        assert pool is not None, "Database is not connected"
        return await pool.acquire()

    async def release(self, conn: aiopg.Connection):
        pool = self.pool
        assert pool is not None, "Database is not connected"
        await pool.release(conn)
