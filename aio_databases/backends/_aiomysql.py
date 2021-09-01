from __future__ import annotations

import typing as t
from uuid import uuid4

import aiomysql

from . import ABCDatabaseBackend, ABCConnection, ABCTransaction
from ..record import Record


class Transaction(ABCTransaction):

    savepoint: t.Optional[str] = None

    async def _start(self) -> t.Any:
        connection = self.connection
        if connection.transactions:
            self.savepoint = savepoint = f"AIODB_SAVEPOINT_{uuid4().hex}"
            return await connection.execute(f"SAVEPOINT {savepoint}")

        connection.logger.debug(('BEGIN',))
        return await connection.conn.begin()

    async def _commit(self) -> t.Any:
        savepoint = self.savepoint
        connection = self.connection
        if savepoint:
            return await connection.execute(f"RELEASE SAVEPOINT {savepoint}")

        connection.logger.debug(('COMMIT',))
        return await connection.conn.commit()

    async def _rollback(self) -> t.Any:
        savepoint = self.savepoint
        connection = self.connection
        if savepoint:
            return await connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")

        connection.logger.debug(('ROLLBACK',))
        return await connection.conn.rollback()


class Connection(ABCConnection):

    transaction_cls = Transaction

    async def _execute(self, query: str, *params, **options) -> t.Any:
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, params)
            if cursor.lastrowid == 0:
                return cursor.rowcount
            return cursor.lastrowid

        finally:
            await cursor.close()

    async def _executemany(self, query: str, *params, **options) -> t.Any:
        cursor = await self.conn.cursor()
        try:
            await cursor.executemany(query, params)
        finally:
            await cursor.close()

    async def _fetchall(self, query: str, *params, **options) -> t.List[t.Mapping]:
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, params)
            rows = await cursor.fetchall()
            desc = cursor.description
            return [Record(row, desc) for row in rows]

        finally:
            await cursor.close()

    async def _fetchmany(self, size: int, query: str, *params, **options) -> t.List[t.Mapping]:
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, params)
            rows = await cursor.fetchmany(size)
            desc = cursor.description
            return [Record(row, desc) for row in rows]

        finally:
            await cursor.close()

    async def _fetchone(self, query: str, *params, **options) -> t.Optional[t.Mapping]:
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, params)
            row = await cursor.fetchone()
            if row is None:
                return row

            return Record(row, cursor.description)
        finally:
            await cursor.close()

    async def _fetchval(self, query: str, *params, column: t.Any = 0, **options) -> t.Any:
        res = await self.fetchone(query, *params)
        if res:
            res = res[column]
        return res

    async def _iterate(self, query: str, *params, **options) -> t.AsyncIterator[Record]:
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, params)
            desc = cursor.description
            while True:
                row = await cursor.fetchone()
                if row is None:
                    break
                yield Record(row, desc)

        finally:
            await cursor.close()


class Backend(ABCDatabaseBackend):

    name = 'aiomysql'
    db_type = 'mysql'
    connection_cls = Connection

    pool: t.Optional[aiomysql.Pool] = None

    async def connect(self) -> None:
        self.pool = await aiomysql.create_pool(
            **self.options,
            host=self.url.hostname,
            port=self.url.port,
            user=self.url.username,
            password=self.url.password,
            db=self.url.path.strip('/'),
        )

    async def disconnect(self) -> None:
        pool = self.pool
        assert pool is not None, "Database is not connected"
        self.pool = None
        pool.close()
        await pool.wait_closed()

    async def acquire(self) -> aiomysql.Connection:
        pool = self.pool
        assert pool is not None, "Database is not connected"
        return await pool.acquire()

    async def release(self, conn: aiomysql.Connection):
        pool = self.pool
        assert pool is not None, "Database is not connected"
        await pool.release(conn)
