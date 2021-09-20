from __future__ import annotations

import typing as t

import logging

from urllib.parse import urlsplit

from . import logger, current_conn
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

    async def connect(self) -> Database:
        """Open the database's pool."""
        if not self.is_connected:
            self.logger.info(f'Database connect: {self.url}')
            await self.backend.connect()
            self.is_connected = True

        return self

    __aenter__ = connect

    async def disconnect(self, *args) -> None:
        """Close connections and the database's pool."""
        self.logger.info(f'Database disconnect: {self.url}')

        # Release connections
        #  while self.current_conn:
        #      await self.current_conn.release()

        if self.is_connected:
            await self.backend.disconnect()
            self.is_connected = False

    __aexit__ = disconnect

    @property
    def current_conn(self) -> t.Optional[ABCConnection]:
        return current_conn.get()

    def connection(self, create: bool = True) -> ConnectionContext:
        """Get/create a connection from/to the current context."""
        return ConnectionContext(self.backend, use_existing=not create)

    def transaction(self, create: bool = False) -> TransactionContext:
        """Create a transaction."""
        return TransactionContext(self.backend, use_existing=not create)

    async def execute(self, query: t.Any, *params, **options) -> t.Any:
        """Execute a query."""
        async with self.connection(False) as conn:
            return await conn.execute(query, *params)

    async def executemany(self, query: t.Any, *params, **options) -> t.Any:
        """Execute a query many times."""
        async with self.connection(False) as conn:
            return await conn.executemany(query, *params, **options)

    async def fetchall(self, query: t.Any, *params, **options) -> t.List[t.Mapping]:
        """Fetch all rows."""
        async with self.connection(False) as conn:
            return await conn.fetchall(query, *params, **options)

    async def fetchmany(self, size: int, query: t.Any, *params, **options) -> t.List[t.Mapping]:
        """Fetch rows."""
        async with self.connection(False) as conn:
            return await conn.fetchmany(size, query, *params, **options)

    async def fetchone(self, query: t.Any, *params, **options) -> t.Optional[t.Mapping]:
        """Fetch a row."""
        async with self.connection(False) as conn:
            return await conn.fetchone(query, *params)

    async def fetchval(self, query: t.Any, *params, column: t.Any = 0, **options) -> t.Any:
        """Fetch a value."""
        async with self.connection(False) as conn:
            return await conn.fetchval(query, *params, column=column, **options)

    async def iterate(self, query: t.Any, *params, **options) -> t.AsyncIterator:
        """Iterate through results."""
        async with self.connection(False) as conn:
            async for res in conn.iterate(query, *params, **options):
                yield res


class ConnectionContext:

    __slots__ = 'release', 'conn', 'token'

    def __init__(self, backend: ABCDatabaseBackend, *, use_existing: bool = False):
        conn = current_conn.get()
        if conn is None or not use_existing:
            conn = backend.connection()
            self.release = True
        else:
            self.release = False

        self.conn = conn

    async def __aenter__(self):
        conn = self.conn
        await conn.acquire()
        self.token = current_conn.set(conn)
        return conn

    async def __aexit__(self, *args):
        if self.release:
            current_conn.reset(self.token)
            await self.conn.release()


class TransactionContext(ConnectionContext):

    __slots__ = ConnectionContext.__slots__ + ('trans',)

    def __init__(self, backend: ABCDatabaseBackend, *, use_existing: bool = True):
        super(TransactionContext, self).__init__(backend, use_existing=use_existing)
        self.trans = self.conn.transaction()

    async def __aenter__(self):
        await super(TransactionContext, self).__aenter__()
        await self.trans.__aenter__()
        return self.trans

    async def __aexit__(self, *args):
        await self.trans.__aexit__(*args)
        await super(TransactionContext, self).__aexit__(*args)
