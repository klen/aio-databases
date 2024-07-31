from __future__ import annotations

from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import urlsplit

from .backends import BACKENDS, SHORTCUTS, ABCConnection, ABCDatabaseBackend, ABCTransaction
from .log import logger

if TYPE_CHECKING:
    import logging
    from collections.abc import AsyncIterator

    from .types import TRecord

current_conn: ContextVar[Optional[ABCConnection]] = ContextVar("current_conn", default=None)


class Database:
    url: str
    backend: ABCDatabaseBackend
    is_connected: bool = False

    def __init__(self, url: str, *, logger: logging.Logger = logger, **options):
        parsed_url = urlsplit(url)

        scheme = parsed_url.scheme
        scheme = SHORTCUTS.get(scheme, scheme)
        for backend_cls in BACKENDS:
            if scheme in (backend_cls.name, backend_cls.db_type):
                break
        else:
            raise ValueError(f"Unknown backend: '{scheme}' or driver is not installed")

        self.url = url
        self.logger = logger
        self.backend: ABCDatabaseBackend = backend_cls(parsed_url, logger=self.logger, **options)

    async def connect(self) -> Database:
        """Open the database's pool."""
        if not self.is_connected:
            self.logger.info("Database connect: %s", self.url)
            await self.backend.connect()
            self.is_connected = True

        return self

    __aenter__ = connect

    async def disconnect(self, *exit_args) -> None:
        """Close connections and the database's pool."""
        self.logger.info("Database disconnect: %s", self.url)

        # Release connection
        cur_conn = self.current_conn
        if cur_conn and cur_conn.is_ready:
            await cur_conn.release()
            current_conn.set(None)

        if self.is_connected:
            await self.backend.disconnect()
            self.is_connected = False

    __aexit__ = disconnect

    @property
    def current_conn(self) -> Optional[ABCConnection]:
        return current_conn.get()

    def connection(self, *, create: bool = True, **params) -> ConnectionContext:
        """Get/create a connection from/to the current context."""
        return ConnectionContext(self.backend, use_existing=not create, **params)

    def transaction(self, *, create: bool = False, **params) -> TransactionContext:
        """Create a transaction."""
        return TransactionContext(self.backend, use_existing=not create, **params)

    async def execute(self, query: Any, *params, **options) -> Any:
        """Execute a query."""
        async with self.connection(create=False) as conn:
            return await conn.execute(query, *params, **options)

    async def executemany(self, query: Any, *params, **options) -> Any:
        """Execute a query many times."""
        async with self.connection(create=False) as conn:
            return await conn.executemany(query, *params, **options)

    async def fetchall(self, query: Any, *params, **options) -> list[TRecord]:
        """Fetch all rows."""
        async with self.connection(create=False) as conn:
            return await conn.fetchall(query, *params, **options)

    async def fetchmany(self, size: int, query: Any, *params, **options) -> list[TRecord]:
        """Fetch rows."""
        async with self.connection(create=False) as conn:
            return await conn.fetchmany(size, query, *params, **options)

    async def fetchone(self, query: Any, *params, **options) -> Optional[TRecord]:
        """Fetch a row."""
        async with self.connection(create=False) as conn:
            return await conn.fetchone(query, *params, **options)

    async def fetchval(self, query: Any, *params, column: Any = 0, **options) -> Any:
        """Fetch a value."""
        async with self.connection(create=False) as conn:
            return await conn.fetchval(query, *params, column=column, **options)

    async def iterate(self, query: Any, *params, **options) -> AsyncIterator[TRecord]:
        """Iterate through results."""
        async with self.connection(create=False) as conn:
            async for res in conn.iterate(query, *params, **options):
                yield res


class ConnectionContext:
    __slots__ = "create_conn", "conn", "token"

    if TYPE_CHECKING:
        conn: ABCConnection

    def __init__(self, backend: ABCDatabaseBackend, *, use_existing: bool = False, **params):
        conn = current_conn.get()
        self.create_conn = not (conn and conn.is_ready and use_existing)
        if self.create_conn:
            conn = backend.connection(**params)

        self.conn = conn  # type: ignore[assignment]

    async def __aenter__(self):
        conn = self.conn
        if self.create_conn:
            await conn.acquire()
            self.token = current_conn.set(conn)
        return conn

    async def __aexit__(self, *_):
        if self.create_conn:
            current_conn.reset(self.token)
            await self.conn.release()

    async def acquire(self) -> ABCConnection:
        await self.conn.acquire()
        return self.conn

    async def release(self, *args):
        await self.conn.release(*args)


class TransactionContext(ConnectionContext):
    __slots__ = "release_conn", "conn", "token", "trans"

    if TYPE_CHECKING:
        trans: ABCTransaction

    def __init__(self, backend: ABCDatabaseBackend, *, use_existing: bool = True, **params):
        super(TransactionContext, self).__init__(backend, use_existing=use_existing)
        self.trans = self.conn.transaction(**params)

    async def __aenter__(self):
        await super(TransactionContext, self).__aenter__()
        await self.trans.__aenter__()
        return self.trans

    async def __aexit__(self, *args):
        await self.trans.__aexit__(*args)
        await super(TransactionContext, self).__aexit__(*args)
