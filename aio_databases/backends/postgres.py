from __future__ import annotations

import typing as t

from asyncpg import create_pool, Connection, Pool, Record
from asyncpg.transaction import Transaction

from . import ABCDatabaseBackend, ABCConnection, ABCTransaction


class PostgresBackend(ABCDatabaseBackend):

    name = 'postgresql'
    record_cls = Record
    pool: t.Optional[Pool] = None

    def connection(self) -> PostgresConnection:
        return PostgresConnection(self)

    async def connect(self) -> None:
        self.pool: Pool = await create_pool(
            **self.options,
            host=self.url.hostname,
            port=self.url.port,
            user=self.url.username,
            password=self.url.password,
            database=self.url.path.strip('/'),
        )

    async def disconnect(self) -> None:
        pool = self.pool
        assert pool is not None, "Database is not connected"
        self.pool = None
        await pool.close()

    async def acquire(self) -> Connection:
        pool = self.pool
        assert pool is not None, "Database is not connected"
        return await pool.acquire()

    async def release(self, conn: Connection):
        pool = self.pool
        assert pool is not None, "Database is not connected"
        await pool.release(conn)


class PostgresConnection(ABCConnection):

    async def execute(self, query: str, *args, **params) -> t.Any:
        conn: Connection = self.conn
        return await conn.execute(query, *args, **params)

    async def executemany(self, query: str, *args, **params) -> t.Any:
        conn: Connection = self.conn
        return await conn.executemany(query, args, **params)

    async def fetchall(self, query: str, *args, **params) -> t.List[Record]:
        conn: Connection = self.conn
        return await conn.fetch(query, *args, **params)

    async def fetchone(self, query: str, *args, **params) -> t.Optional[Record]:
        conn: Connection = self.conn
        return await conn.fetchrow(query, *args, **params)

    async def fetchval(self, query: str, *args, column: t.Any = 0, **params) -> t.Any:
        conn: Connection = self.conn
        return await conn.fetchval(query, *args, **params)

    def transaction(self) -> PostgresTransaction:
        return PostgresTransaction(self)


class PostgresTransaction(ABCTransaction):

    _trans: t.Optional[Transaction] = None

    @property
    def trans(self) -> Transaction:
        if self._trans is None:
            self._trans = self.connection.conn.transaction()
        return self._trans

    async def _start(self):
        return await self.trans.start()

    async def _commit(self):
        return await self.trans.commit()

    async def _rollback(self):
        return await self.trans.rollback()
