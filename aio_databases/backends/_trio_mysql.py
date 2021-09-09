from __future__ import annotations

import typing as t
from uuid import uuid4

import trio
import trio_mysql

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
    lock_cls = trio.Lock

    async def _execute(self, query: str, *params, **options) -> t.Any:
        async with self.conn.cursor() as cursor:
            await cursor.execute(query, params)
            if cursor.lastrowid == 0:
                return cursor.rowcount
            return cursor.lastrowid

    async def _executemany(self, query: str, *params, **options) -> t.Any:
        async with self.conn.cursor() as cursor:
            await cursor.executemany(query, params)

    async def _fetchall(self, query: str, *params, **options) -> t.List[t.Mapping]:
        async with self.conn.cursor() as cursor:
            await cursor.execute(query, params)
            rows = await cursor.fetchall()
            desc = cursor.description
            return [Record(row, desc) for row in rows]

    async def _fetchmany(self, size: int, query: str, *params, **options) -> t.List[t.Mapping]:
        async with self.conn.cursor() as cursor:
            await cursor.execute(query, params)
            rows = await cursor.fetchmany(size)
            desc = cursor.description
            return [Record(row, desc) for row in rows]

    async def _fetchone(self, query: str, *params, **options) -> t.Optional[t.Mapping]:
        async with self.conn.cursor() as cursor:
            await cursor.execute(query, params)
            row = await cursor.fetchone()
            if row is None:
                return row

            return Record(row, cursor.description)

    async def _fetchval(self, query: str, *params, column: t.Any = 0, **options) -> t.Any:
        res = await self.fetchone(query, *params)
        if res:
            res = res[column]
        return res

    async def _iterate(self, query: str, *params, **options) -> t.AsyncIterator[Record]:
        async with self.conn.cursor() as cursor:
            await cursor.execute(query, params)
            desc = cursor.description
            while True:
                row = await cursor.fetchone()
                if row is None:
                    break
                yield Record(row, desc)


class Backend(ABCDatabaseBackend):

    name = 'trio-mysql'
    db_type = 'mysql'
    connection_cls = Connection

    def __init__(self, *args, autocommit=True, charset='utf8', use_unicode=True, **options):
        """Setup default value for autocommit."""
        super(Backend, self).__init__(
            *args, autocommit=autocommit, charset=charset, use_unicode=use_unicode, **options)

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass

    async def acquire(self) -> trio_mysql.Connection:
        conn = trio_mysql.connect(
            **self.options,
            host=self.url.hostname,
            port=self.url.port,
            user=self.url.username,
            password=self.url.password,
            db=self.url.path.strip('/'),
        )
        await conn.connect()
        return conn

    async def release(self, conn: trio_mysql.Connection):
        conn.close()
