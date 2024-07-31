from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

import asyncpg
import trio
import trio_asyncio
import triopg

from . import RE_PARAM, ABCConnection, ABCDatabaseBackend, ABCTransaction
from .common import PGReplacer, pg_parse_status

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from asyncpg.transaction import Transaction as AsyncPGTransaction


class Transaction(ABCTransaction[triopg._triopg.TrioConnectionProxy]):
    _trans: Optional[AsyncPGTransaction] = None

    @property
    def trans(self) -> AsyncPGTransaction:
        if self._trans is None:
            conn = self.connection._conn
            assert conn is not None
            self._trans = conn._asyncpg_conn.transaction()
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


class Connection(ABCConnection[triopg._triopg.TrioConnectionProxy]):
    transaction_cls = Transaction
    lock_cls = trio.Lock  # type: ignore[assignment]

    @trio_asyncio.aio_as_trio
    async def _execute(self, query: str, *params, **options) -> Any:
        conn = self._conn
        assert conn is not None, "Connection is not established"
        status = await conn.execute(query, *params, **options)
        return pg_parse_status(status)

    @trio_asyncio.aio_as_trio
    async def _executemany(self, query: str, *params, **options) -> Any:
        conn = self._conn
        assert conn is not None, "Connection is not established"
        return await conn.executemany(query, params, **options)

    @trio_asyncio.aio_as_trio
    async def _fetchall(self, query: str, *params, **options) -> list[asyncpg.Record]:
        conn = self._conn
        assert conn is not None, "Connection is not established"
        return await conn.fetch(query, *params, **options)

    @trio_asyncio.aio_as_trio
    async def _fetchmany(self, size: int, query: str, *params, **options) -> list[asyncpg.Record]:
        conn = self._conn
        assert conn is not None, "Connection is not established"
        res = await conn.fetch(query, *params, **options)
        return res[:size]

    @trio_asyncio.aio_as_trio
    async def _fetchone(self, query: str, *params, **options) -> Optional[asyncpg.Record]:
        conn = self._conn
        assert conn is not None, "Connection is not established"
        return await conn.fetchrow(query, *params, **options)

    @trio_asyncio.aio_as_trio
    async def _fetchval(self, query: str, *params, column: Any = 0, **options) -> Any:
        conn = self._conn
        assert conn is not None, "Connection is not established"
        return await conn.fetchval(query, *params, column=column, **options)

    async def _iterate(self, query: str, *params, **_) -> AsyncIterator[asyncpg.Record]:
        conn = self._conn
        assert conn is not None, "Connection is not established"
        async with conn.transaction():
            async for rec in conn.cursor(query, *params):
                yield rec


class Backend(ABCDatabaseBackend[triopg._triopg.TrioConnectionProxy]):
    name = "triopg"
    db_type = "postgresql"
    connection_cls = Connection

    pool: Optional[triopg._triopg.TrioPoolProxy] = None

    def __init__(self, *args, **kwargs):
        super(Backend, self).__init__(*args, **kwargs)
        self.pool_options = {
            name: self.options.pop(name)
            for name in (
                "min_size",
                "max_size",
                "max_queries",
                "max_inactive_connection_lifetime",
                "setup",
                "init",
            )
            if name in self.options
        }

    def __convert_sql__(self, sql: Any) -> str:
        sql = str(sql)
        if self.convert_params:
            sql = RE_PARAM.sub(PGReplacer(), sql)

        return sql

    async def connect(self) -> None:
        self.pool = triopg.create_pool(
            **self.options,
            **self.pool_options,
            host=self.url.hostname,
            port=self.url.port,
            user=self.url.username,
            password=self.url.password,
            database=self.url.path.strip("/"),
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
                database=self.url.path.strip("/"),
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
