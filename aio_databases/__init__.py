"""Async support for various databases."""

from __future__ import annotations

import typing as t
import logging

from contextvars import ContextVar
from urllib.parse import urlsplit

from .backends import BACKENDS, ABCDabaseBackend, ABCConnection


__version__ = '0.0.8'


logger = logging.getLogger('aiodb')


class Database:

    url: str
    backend: ABCDabaseBackend
    is_connected: bool = False

    def __init__(self, url: str, **options):
        parsed_url = urlsplit(url)

        try:
            backend_cls = BACKENDS[parsed_url.scheme]
        except KeyError:
            raise ValueError(f"Unsupported backend: {parsed_url.scheme}")

        self.url = url
        self.backend: ABCDabaseBackend = backend_cls(parsed_url, **options)
        self._conn_ctx: ContextVar = ContextVar('connection')

    async def connect(self) -> Database:
        """Connect the database."""
        if not self.is_connected:
            logger.info(f'Database connect: {self.url}')
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
            logger.info(f'Database disconnect: {self.url}')
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

    def transaction(self):
        return self.connection.transaction()

    async def execute(self, query: t.Any, *args, **params) -> t.Any:
        conn = await self.connection.acquire()
        async with conn._lock:
            return await conn.execute(f"{query}", *args, **params)

    async def executemany(self, query: t.Any, *args, **params) -> t.Any:
        conn = await self.connection.acquire()
        async with conn._lock:
            return await conn.executemany(f"{query}", *args, **params)

    async def fetch(self, query: t.Any, *args, **params) -> t.List[t.Tuple]:
        conn = await self.connection.acquire()
        async with conn._lock:
            return await conn.fetch(f"{query}", *args, **params)

    async def fetchrow(self, query: t.Any, *args, **params) -> t.Optional[t.Tuple]:
        conn = await self.connection.acquire()
        async with conn._lock:
            return await conn.fetchrow(f"{query}", *args, **params)

    async def fetchval(self, query: t.Any, *args, column: t.Any = 0, **params) -> t.Any:
        conn = await self.connection.acquire()
        async with conn._lock:
            return await conn.fetchval(f"{query}", *args, **params)
