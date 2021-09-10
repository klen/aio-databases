from __future__ import annotations

import typing as t
from uuid import uuid4

import aioodbc

from . import ABCDatabaseBackend, ABCConnection, ABCTransaction, RE_PARAM
from ..record import Record


class Transaction(ABCTransaction):

    savepoint: t.Optional[str] = None

    async def _start(self) -> t.Any:
        connection = self.connection
        if connection.transactions:
            self.savepoint = savepoint = f"AIODB_SAVEPOINT_{uuid4().hex}"
            return await connection.execute(f"SAVEPOINT {savepoint}")

        return await connection.execute("BEGIN TRANSACTION")

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
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, params)
            return cursor.rowcount
        finally:
            cursor.close()

    async def _executemany(self, query: str, *params, **options) -> t.Any:
        cursor = await self.conn.cursor()
        try:
            await cursor.executemany(query, params)
        finally:
            cursor.close()

    async def _fetchall(self, query: str, *params, **options) -> t.List[t.Mapping]:
        cursor = await self.conn.cursor()
        try:
            await cursor.execute(query, params)
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
            cursor.close()

    async def _fetchval(self, query: str, *params, column: t.Any = 0, **options) -> t.Any:
        res = await self.fetchone(query, *params)
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
            await cursor.close()


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
