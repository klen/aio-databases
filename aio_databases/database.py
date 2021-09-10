from __future__ import annotations

import typing as t

import logging

from contextvars import ContextVar
from urllib.parse import urlsplit

from . import logger
from .backends import BACKENDS, SHORTCUTS, ABCDatabaseBackend, ABCConnection, ABCTransaction


class Database:

    url: str
    backend: ABCDatabaseBackend
    is_connected: bool = False

    def __init__(self, url: str, *, logger: logging.Logger = logger, **options):
        parsed_url = urlsplit(url)

        scheme = parsed_url.scheme
        scheme = SHORTCUTS.get(scheme, scheme)
        for backend_cls in BACKENDS:
            if backend_cls.name == scheme or backend_cls.db_type == scheme:
                break
        else:
            raise ValueError(
                f"Unsupported backend: '{scheme}', please install a required database driver")

        self.url = url
        self.logger = logger
        self.backend: ABCDatabaseBackend = backend_cls(parsed_url, logger=self.logger, **options)
        self._conn_ctx: ContextVar = ContextVar('connection')

    async def connect(self) -> Database:
        """Connect the database."""
        if not self.is_connected:
            self.logger.info(f'Database connect: {self.url}')
            await self.backend.connect()
            self.is_connected = True

        return self

    __aenter__ = connect

    async def disconnect(self, *args) -> None:
        """Disconnect the database."""
        conn = self._conn_ctx.get(None)
        if conn is not None:
            await conn.release()

        if self.is_connected:
            self.logger.info(f'Database disconnect: {self.url}')
            await self.backend.disconnect()
            self.is_connected = False

        self._conn_ctx.set(None)

    __aexit__ = disconnect

    def connection(self, create: bool = True) -> ABCConnection:
        """Get/create a connection from/to the current context."""
        conn = self._conn_ctx.get(None)
        if conn is None or create:
            conn = self.backend.connection()
            self._conn_ctx.set(conn)

        return conn

    def transaction(self, create: bool = False) -> ABCTransaction:
        return self.connection(create).transaction()

    async def execute(self, query: t.Any, *params, **options) -> t.Any:
        conn = await self.connection(False).acquire()
        return await conn.execute(query, *params)

    async def executemany(self, query: t.Any, *params, **options) -> t.Any:
        conn = await self.connection(False).acquire()
        return await conn.executemany(query, *params, **options)

    async def fetchall(self, query: t.Any, *params, **options) -> t.List[t.Mapping]:
        conn = await self.connection(False).acquire()
        return await conn.fetchall(query, *params, **options)

    async def fetchmany(self, size: int, query: t.Any, *params, **options) -> t.List[t.Mapping]:
        conn = await self.connection(False).acquire()
        return await conn.fetchmany(size, query, *params, **options)

    async def fetchone(self, query: t.Any, *params, **options) -> t.Optional[t.Mapping]:
        conn = await self.connection(False).acquire()
        return await conn.fetchone(query, *params)

    async def fetchval(self, query: t.Any, *params, column: t.Any = 0, **options) -> t.Any:
        conn = await self.connection(False).acquire()
        return await conn.fetchval(query, *params, column=column, **options)

    async def iterate(self, query: t.Any, *params, **options) -> t.AsyncIterator:
        """Iterate through rows."""
        conn = await self.connection(False).acquire()
        async for res in conn.iterate(query, *params, **options):
            yield res
