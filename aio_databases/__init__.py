"""Async support for various databases."""

from __future__ import annotations

import typing as t
import logging

from contextvars import ContextVar
from urllib.parse import urlsplit

from .backends import BACKENDS, ABCDatabaseBackend, ABCConnection, ABCTransaction


__version__ = '0.0.16'


logger = logging.getLogger('aio-databases')
logger.addHandler(logging.NullHandler())


class Database:

    url: str
    backend: ABCDatabaseBackend
    is_connected: bool = False

    def __init__(self, url: str, *, logger: logging.Logger = logger, **options):
        parsed_url = urlsplit(url)

        scheme = parsed_url.scheme
        for backend_cls in BACKENDS:
            if backend_cls.name == scheme or backend_cls.db_type == scheme:
                break
        else:
            raise ValueError(
                f"Unsupported backend: '{scheme}', please install a required database driver")

        self.url = url
        self.backend: ABCDatabaseBackend = backend_cls(parsed_url, **options)
        self.logger = logger
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

    __aexit__ = disconnect

    @property
    def connection(self) -> ABCConnection:
        """Get/create a connection from/to the current context."""
        conn = self._conn_ctx.get(None)
        if conn is None:
            conn = self.backend.connection()
            self._conn_ctx.set(conn)

        return conn

    def transaction(self) -> ABCTransaction:
        return self.connection.transaction()

    async def execute(self, query: t.Any, *args, **params) -> t.Any:
        conn = await self.connection.acquire()
        sql = str(query)
        async with conn._lock:
            self.logger.debug((sql, *args))
            return await conn.execute(sql, *args, **params)

    async def executemany(self, query: t.Any, *args, **params) -> t.Any:
        conn = await self.connection.acquire()
        sql = str(query)
        async with conn._lock:
            self.logger.debug((sql, *args))
            return await conn.executemany(sql, *args, **params)

    async def fetchall(self, query: t.Any, *args, **params) -> t.List[t.Mapping]:
        conn = await self.connection.acquire()
        sql = str(query)
        async with conn._lock:
            self.logger.debug((sql, *args))
            return await conn.fetchall(sql, *args, **params)

    async def fetchone(self, query: t.Any, *args, **params) -> t.Optional[t.Mapping]:
        conn = await self.connection.acquire()
        sql = str(query)
        async with conn._lock:
            self.logger.debug((sql, *args))
            return await conn.fetchone(sql, *args, **params)

    async def fetchval(self, query: t.Any, *args, column: t.Any = 0, **params) -> t.Any:
        conn = await self.connection.acquire()
        sql = str(query)
        async with conn._lock:
            self.logger.debug((sql, *args))
            return await conn.fetchval(sql, *args, **params)
