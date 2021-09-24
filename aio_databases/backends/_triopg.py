from __future__ import annotations

import typing as t

import asyncpg
import trio
import trio_asyncio
import triopg

from . import ABCDatabaseBackend, ABCConnection, ABCTransaction, RE_PARAM
from .common import PGReplacer, pg_parse_status


class Transaction(ABCTransaction):

    _trans: t.Optional[asyncpg.transactions.Transaction] = None

    @property
    def trans(self) -> asyncpg.transactions.Transaction:
        if self._trans is None:
            self._trans = self.connection._conn._asyncpg_conn.transaction()
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
        status = await self._conn.execute(query, *params, **options)
        return pg_parse_status(status)

    @trio_asyncio.aio_as_trio
    async def _executemany(self, query: str, *params, **options) -> t.Any:
        return await self._conn.executemany(query, params, **options)

    @trio_asyncio.aio_as_trio
    async def _fetchall(self, query: str, *params, **options) -> t.List[asyncpg.Record]:
        return await self._conn.fetch(query, *params, **options)

    @trio_asyncio.aio_as_trio
    async def _fetchmany(self, size: int, query: str,
                         *params, **options) -> t.List[asyncpg.Record]:
        res = await self._conn.fetch(query, *params, **options)
        return res[:size]

    @trio_asyncio.aio_as_trio
    async def _fetchone(self, query: str, *params, **options) -> t.Optional[asyncpg.Record]:
        return await self._conn.fetchrow(query, *params, **options)

    @trio_asyncio.aio_as_trio
    async def _fetchval(self, query: str, *params, column: t.Any = 0, **options) -> t.Any:
        return await self._conn.fetchval(query, *params, column=column, **options)

    async def _iterate(self, query: str, *params, **options) -> t.AsyncIterator[asyncpg.Record]:
        conn = self._conn
        async with conn.transaction():
            async for rec in conn.cursor(query, *params):
                yield rec


class Backend(ABCDatabaseBackend):

    name = 'triopg'
    db_type = 'postgresql'
    connection_cls = Connection

    pool: t.Optional[triopg._triopg.TrioPoolProxy] = None

    def __init__(self, *args, **kwargs):
        super(Backend, self).__init__(*args, **kwargs)
        self.pool_options = {
            name: self.options.pop(name)
            for name in ('min_size', 'max_size', 'max_queries',
                         'max_inactive_connection_lifetime', 'setup', 'init')
            if name in self.options
        }

    def __convert_sql__(self, sql: t.Any) -> str:
        sql = str(sql)
        if self.convert_params:
            sql = RE_PARAM.sub(PGReplacer(), sql)

        return sql

    async def connect(self) -> None:
        self.pool: triopg._triopg.TrioPoolProxy = triopg.create_pool(
            **self.options, **self.pool_options,
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
    async def _acquire(self) -> triopg._triopg.TrioConnectionProxy:
        conn = triopg._triopg.TrioConnectionProxy()
        if self.pool is None:
            conn._asyncpg_conn = await asyncpg.connect(
                **self.options,
                host=self.url.hostname,
                port=self.url.port,
                user=self.url.username,
                password=self.url.password,
                database=self.url.path.strip('/'),
            )
        else:
            pool = self.pool._asyncpg_pool
            conn._asyncpg_conn = await pool.acquire()
        return conn

    @trio_asyncio.aio_as_trio
    async def release(self, conn: triopg._triopg.TrioConnectionProxy):
        conn = conn._asyncpg_conn
        if self.pool is None:
            await conn.close()
        else:
            pool = self.pool._asyncpg_pool
            await pool.release(conn)
