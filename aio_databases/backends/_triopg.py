from __future__ import annotations

import typing as t

import asyncpg
import trio
import trio_asyncio
import triopg

from . import ABCDatabaseBackend, ABCConnection, ABCTransaction


class Transaction(ABCTransaction):

    _trans: t.Optional[asyncpg.transactions.Transaction] = None

    @property
    def trans(self) -> asyncpg.transactions.Transaction:
        if self._trans is None:
            self._trans = self.connection.conn._asyncpg_conn.transaction()
        return self._trans

    @trio_asyncio.aio_as_trio
    async def _start(self):
        return await self.trans.start()

    @trio_asyncio.aio_as_trio
    async def _commit(self):
        return await self.trans.commit()

    @trio_asyncio.aio_as_trio
    async def _rollback(self):
        return await self.trans.rollback()


class Connection(ABCConnection):

    transaction_cls = Transaction
    lock_cls = trio.Lock

    @trio_asyncio.aio_as_trio
    async def _execute(self, query: str, *params, **options) -> t.Any:
        conn = self.conn
        return await conn.execute(query, *params, **options)

    @trio_asyncio.aio_as_trio
    async def _executemany(self, query: str, *params, **options) -> t.Any:
        conn = self.conn
        return await conn.executemany(query, params, **options)

    @trio_asyncio.aio_as_trio
    async def _fetchall(self, query: str, *params, **options) -> t.List[asyncpg.Record]:
        conn = self.conn
        return await conn.fetch(query, *params, **options)

    async def _fetchmany(self, size: int, query: str, *params, **options) -> t.List[asyncpg.Record]:
        res = await self.fetchall(query, *params, **options)
        return res[:size]

    @trio_asyncio.aio_as_trio
    async def _fetchone(self, query: str, *params, **options) -> t.Optional[asyncpg.Record]:
        conn = self.conn
        return await conn.fetchrow(query, *params, **options)

    @trio_asyncio.aio_as_trio
    async def _fetchval(self, query: str, *params, column: t.Any = 0, **options) -> t.Any:
        conn = self.conn
        return await conn.fetchval(query, *params, column=column, **options)

    async def _iterate(self, query: str, *params, **options) -> t.AsyncIterator[asyncpg.Record]:
        conn = self.conn
        async with conn.transaction():
            async for rec in conn.cursor(query, *params):
                yield rec


class Backend(ABCDatabaseBackend):

    name = 'triopg'
    db_type = 'postgresql'
    connection_cls = Connection

    pool: t.Optional[triopg._triopg.TrioPoolProxy] = None

    async def connect(self) -> None:
        self.pool: triopg._triopg.TrioPoolProxy = triopg.create_pool(
            **self.options,
            host=self.url.hostname,
            port=self.url.port,
            user=self.url.username,
            password=self.url.password,
            database=self.url.path.strip('/'),
        )
        await self.pool.__aenter__()

    async def disconnect(self) -> None:
        pool = self.pool
        assert pool is not None, "Database is not connected"
        self.pool = None
        await pool.close()

    @trio_asyncio.aio_as_trio
    async def acquire(self) -> triopg._triopg.TrioConnectionProxy:
        assert self.pool is not None, "Database is not connected"
        pool = self.pool._asyncpg_pool
        conn = triopg._triopg.TrioConnectionProxy()
        conn._asyncpg_conn = await pool.acquire()
        return conn

    @trio_asyncio.aio_as_trio
    async def release(self, conn: triopg._triopg.TrioConnectionProxy):
        assert self.pool is not None, "Database is not connected"
        conn = conn._asyncpg_conn
        pool = self.pool._asyncpg_pool
        await pool.release(conn)
