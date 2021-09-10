from __future__ import annotations

import typing as t

import asyncpg
from re import compile as re

from . import ABCDatabaseBackend, ABCConnection, ABCTransaction, RE_PARAM


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

    async def _execute(self, query: str, *params, **options) -> t.Any:
        conn: asyncpg.Connection = self.conn
        status = await conn.execute(query, *params, **options)
        return parse_status(status)

    async def _executemany(self, query: str, *params, **options) -> t.Any:
        conn: asyncpg.Connection = self.conn
        return await conn.executemany(query, params, **options)

    async def _fetchall(self, query: str, *params, **options) -> t.List[asyncpg.Record]:
        conn: asyncpg.Connection = self.conn
        return await conn.fetch(query, *params, **options)

    async def _fetchmany(self, size: int, query: str,
                         *params, **options) -> t.List[asyncpg.Record]:
        conn: asyncpg.Connection = self.conn
        async with conn.transaction():
            cur = await conn.cursor(query, *params)
            return await cur.fetch(size)

    async def _fetchone(self, query: str, *params, **options) -> t.Optional[asyncpg.Record]:
        conn: asyncpg.Connection = self.conn
        return await conn.fetchrow(query, *params, **options)

    async def _fetchval(self, query: str, *params, column: t.Any = 0, **options) -> t.Any:
        conn: asyncpg.Connection = self.conn
        return await conn.fetchval(query, *params, column=column, **options)

    async def _iterate(self, query: str, *params, **options) -> t.AsyncIterator[asyncpg.Record]:
        conn: asyncpg.Connection = self.conn
        async with conn.transaction():
            async for rec in conn.cursor(query, *params):
                yield rec


class Backend(ABCDatabaseBackend):

    name = 'asyncpg'
    db_type = 'postgresql'
    connection_cls = Connection

    pool: t.Optional[asyncpg.Pool] = None

    def __convert_sql__(self, sql: t.Any) -> str:
        sql = str(sql)
        if self.convert_params:
            sql = RE_PARAM.sub(Replacer(), sql)

        return sql

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


class Replacer:

    __slots__ = ('num',)

    def __init__(self):
        self.num = 0

    def __call__(self, match):
        self.num += 1
        return f"{match.group(1)}${self.num}"


def parse_status(status: str) -> t.Union[str, int]:
    operation, params = status.split(' ', 1)
    if operation in {'INSERT'}:
        return params.split()[0]

    if operation in {'UPDATE', 'DELETE'}:
        return int(params.split()[0])

    return status
