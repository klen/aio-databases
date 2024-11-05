from __future__ import annotations

from json import dumps, loads
from typing import TYPE_CHECKING, Any, Optional

import asyncpg

from . import RE_PARAM, ABCConnection, ABCDatabaseBackend, ABCTransaction
from .common import PGReplacer, pg_parse_status

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from asyncpg.transaction import Transaction as AsyncPGTransaction


class Transaction(ABCTransaction[asyncpg.Connection]):
    _trans: Optional[AsyncPGTransaction] = None

    @property
    def trans(self) -> AsyncPGTransaction:
        if self._trans is None:
            conn = self.connection._conn
            assert conn is not None
            self._trans = conn.transaction()
        return self._trans

    async def _start(self):
        self.connection.logger.debug(("BEGIN",))
        return await self.trans.start()

    async def _commit(self):
        self.connection.logger.debug(("COMMIT",))
        return await self.trans.commit()

    async def _rollback(self):
        self.connection.logger.debug(("ROLLBACK",))
        return await self.trans.rollback()


class Connection(ABCConnection[asyncpg.Connection]):
    transaction_cls = Transaction

    async def _execute(self, query: str, *params, **options) -> Any:
        conn = self._conn
        assert conn is not None
        status = await conn.execute(query, *params, **options)
        return pg_parse_status(status)

    async def _executemany(self, query: str, *params, **options) -> Any:
        conn = self._conn
        assert conn is not None
        return await conn.executemany(query, params, **options)

    async def _fetchall(self, query: str, *params, **options) -> list[asyncpg.Record]:
        conn = self._conn
        assert conn is not None
        return await conn.fetch(query, *params, **options)

    async def _fetchmany(self, size: int, query: str, *params, **_) -> list[asyncpg.Record]:
        conn = self._conn
        assert conn is not None
        async with conn.transaction():
            cur = await conn.cursor(query, *params)
            return await cur.fetch(size)

    async def _fetchone(self, query: str, *params, **options) -> Optional[asyncpg.Record]:
        conn = self._conn
        assert conn is not None
        return await conn.fetchrow(query, *params, **options)

    async def _fetchval(self, query: str, *params, column: Any = 0, **options) -> Any:
        conn = self._conn
        assert conn is not None
        return await conn.fetchval(query, *params, column=column, **options)

    async def _iterate(self, query: str, *params, **_) -> AsyncIterator[asyncpg.Record]:
        conn = self._conn
        assert conn is not None
        async with conn.transaction():
            async for rec in conn.cursor(query, *params):
                yield rec


class Backend(ABCDatabaseBackend[asyncpg.Connection]):
    name = "asyncpg"
    db_type = "postgresql"
    connection_cls = Connection

    def __init__(self, url, *, json=False, **kwargs):
        super(Backend, self).__init__(url, **kwargs)
        url = self.url
        self.options["host"] = url.hostname
        self.options["port"] = url.port
        self.options["user"] = url.username
        self.options["password"] = url.password
        self.options["database"] = url.path.strip("/")
        if json:
            self.init = asyncpg_init_json

    def __convert_sql__(self, sql: Any) -> str:
        sql = str(sql)
        if self.convert_params:
            sql = RE_PARAM.sub(PGReplacer(), sql)

        return sql

    async def _acquire(self) -> asyncpg.Connection:
        return await asyncpg.connect(**self.options)

    async def release(self, conn: asyncpg.Connection):
        await conn.close()


class PoolBackend(Backend):
    name = "asyncpg+pool"
    _pool: Optional[asyncpg.Pool] = None

    def __init__(self, *args, **kwargs):
        super(PoolBackend, self).__init__(*args, **kwargs)
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

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(**self.options, **self.pool_options)

    async def disconnect(self) -> None:
        await self.pool.close()

    async def _acquire(self) -> asyncpg.Connection:
        return await self.pool.acquire()

    async def release(self, conn: asyncpg.Connection):
        await self.pool.release(conn)


async def asyncpg_init_json(conn: asyncpg.Connection):
    await conn.set_type_codec("json", encoder=dumps, decoder=loads, schema="pg_catalog")
    return conn
