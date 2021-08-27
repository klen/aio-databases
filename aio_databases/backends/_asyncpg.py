from __future__ import annotations

import typing as t

import asyncpg

from . import ABCDatabaseBackend, ABCConnection, ABCTransaction


class Transaction(ABCTransaction):

    _trans: t.Optional[asyncpg.transactions.Transaction] = None

    @property
    def trans(self) -> asyncpg.transactions.Transaction:
        if self._trans is None:
            self._trans = self.connection.conn.transaction()
        return self._trans

    async def _start(self):
        return await self.trans.start()

    async def _commit(self):
        return await self.trans.commit()

    async def _rollback(self):
        return await self.trans.rollback()


class Connection(ABCConnection):

    transaction_cls = Transaction

    async def execute(self, query: str, *args, **params) -> t.Any:
        conn: asyncpg.Connection = self.conn
        return await conn.execute(query, *args, **params)

    async def executemany(self, query: str, *args, **params) -> t.Any:
        conn: asyncpg.Connection = self.conn
        return await conn.executemany(query, args, **params)

    async def fetchall(self, query: str, *args, **params) -> t.List[asyncpg.Record]:
        conn: asyncpg.Connection = self.conn
        return await conn.fetch(query, *args, **params)

    async def fetchone(self, query: str, *args, **params) -> t.Optional[asyncpg.Record]:
        conn: asyncpg.Connection = self.conn
        return await conn.fetchrow(query, *args, **params)

    async def fetchval(self, query: str, *args, column: t.Any = 0, **params) -> t.Any:
        conn: asyncpg.Connection = self.conn
        return await conn.fetchval(query, *args, **params)


class Backend(ABCDatabaseBackend):

    name = 'asyncpg'
    db_type = 'postgresql'
    record_cls = asyncpg.Record
    connection_cls = Connection

    pool: t.Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        self.pool: asyncpg.Pool = await asyncpg.create_pool(
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

    async def acquire(self) -> asyncpg.Connection:
        pool = self.pool
        assert pool is not None, "Database is not connected"
        return await pool.acquire()

    async def release(self, conn: asyncpg.Connection):
        pool = self.pool
        assert pool is not None, "Database is not connected"
        await pool.release(conn)
