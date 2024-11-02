from __future__ import annotations

import abc
import asyncio
from contextlib import suppress
from re import compile as re
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Optional
from urllib.parse import SplitResult, parse_qsl

from aio_databases.log import logger as base_logger
from aio_databases.types import TVConnection

if TYPE_CHECKING:
    import logging
    from collections.abc import AsyncIterator

    from aio_databases.types import TInitConnection, TRecord

BACKENDS = []
SHORTCUTS = {
    "sqllite": "sqlite",
    "postgres": "postgresql",
    "postgressql": "postgresql",
}
RE_PARAM = re(r"([^%])(%s)")


class ABCTransaction(abc.ABC, Generic[TVConnection]):
    __slots__ = "connection", "silent"

    def __init__(self, connection: ABCConnection[TVConnection], *, silent: bool = False):
        """Initialize the transaction.
        :param silent: Do not raise an error for commit/rollback
        """
        self.connection = connection
        self.silent = silent

    @abc.abstractmethod
    async def _start(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def _commit(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def _rollback(self):
        raise NotImplementedError

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        if self in self.connection.transactions:
            if exc_type is not None:
                await self.rollback()
            else:
                await self.commit()

    async def start(self):
        connection = self.connection
        if not connection.is_ready:
            raise RuntimeError("There is no an acquired connection to start transactions")

        await self._start()
        connection.transactions.add(self)

    async def commit(self, *, silent: Optional[bool] = None):
        """Commit the transaction.
        :param silent: Do not raise an error when the connection is closed
        """
        connection = self.connection
        connection.transactions.discard(self)
        if connection.is_ready:
            return await self._commit()

        silent = self.silent if silent is None else silent
        if not silent:
            raise RuntimeError("There is no an acquired connection to commit the transaction")
        return None

    async def rollback(self, *, silent: Optional[bool] = None):
        """Rollback the transaction.
        :param silent: Do not raise an error when the connection is closed
        """
        connection = self.connection
        connection.transactions.discard(self)
        if connection.is_ready:
            return await self._rollback()

        silent = self.silent if silent is None else silent
        if not silent:
            raise RuntimeError("There is no an acquired connection to rollback the transaction")
        return None


class ABCConnection(abc.ABC, Generic[TVConnection]):
    transaction_cls: ClassVar[type[ABCTransaction]]
    lock_cls: type[asyncio.Lock] = asyncio.Lock

    __slots__ = "backend", "logger", "transactions", "_conn", "_lock"

    def __init__(self, backend: ABCDatabaseBackend, **unsupported_params):
        self.backend = backend
        self.logger: logging.Logger = backend.logger
        self.transactions: set[ABCTransaction] = set()
        self._conn: Optional[TVConnection] = None
        self._lock = self.lock_cls()

    @property
    def is_ready(self) -> bool:
        return self._conn is not None

    async def acquire(self):
        if self._conn is None:
            async with self._lock:
                self._conn = await self.backend.acquire()

    async def release(self, *_):
        if self._conn is not None:
            async with self._lock:
                conn, self._conn = self._conn, None
                await self.backend.release(conn)

    async def execute(self, query: Any, *params, **options) -> Any:
        sql = self.backend.__convert_sql__(query)
        self.logger.debug((sql, *params))
        async with self._lock:
            return await self._execute(sql, *params, **options)

    async def executemany(self, query: Any, *params, **options) -> Any:
        sql = self.backend.__convert_sql__(query)
        self.logger.debug((sql, *params))
        async with self._lock:
            return await self._executemany(sql, *params, **options)

    async def fetchall(self, query: Any, *params, **options) -> list[TRecord]:
        sql = self.backend.__convert_sql__(query)
        self.logger.debug((sql, *params))
        async with self._lock:
            return await self._fetchall(sql, *params, **options)

    async def fetchmany(self, size: int, query: Any, *params, **options) -> list[TRecord]:
        sql = self.backend.__convert_sql__(query)
        self.logger.debug((sql, *params))
        async with self._lock:
            return await self._fetchmany(size, sql, *params, **options)

    async def fetchone(self, query: Any, *params, **options) -> Optional[TRecord]:
        sql = self.backend.__convert_sql__(query)
        self.logger.debug((sql, *params))
        async with self._lock:
            return await self._fetchone(sql, *params, **options)

    async def fetchval(self, query: Any, *params, column: Any = 0, **options) -> Any:
        sql = self.backend.__convert_sql__(query)
        self.logger.debug((sql, *params))
        async with self._lock:
            return await self._fetchval(sql, *params, column=column, **options)

    async def iterate(self, query: Any, *params, **options) -> AsyncIterator[TRecord]:
        sql = self.backend.__convert_sql__(query)
        self.logger.debug((sql, *params))
        async with self._lock:
            async for res in self._iterate(sql, *params, **options):
                yield res

    @abc.abstractmethod
    async def _execute(self, query: str, *params, **options) -> Any:
        raise NotImplementedError

    @abc.abstractmethod
    async def _executemany(self, query: str, *params, **options) -> Any:
        raise NotImplementedError

    @abc.abstractmethod
    async def _fetchall(self, query: str, *params, **options) -> list[TRecord]:
        raise NotImplementedError

    @abc.abstractmethod
    async def _fetchmany(self, size: int, query: str, *params, **options) -> list[TRecord]:
        raise NotImplementedError

    @abc.abstractmethod
    async def _fetchone(self, query: str, *params, **options) -> Optional[TRecord]:
        raise NotImplementedError

    @abc.abstractmethod
    async def _fetchval(self, query: str, *params, column: Any = 0, **options) -> Any:
        raise NotImplementedError

    @abc.abstractmethod
    def _iterate(self, query: str, *params, **options) -> AsyncIterator[TRecord]:
        raise NotImplementedError

    def transaction(self, **params) -> ABCTransaction[TVConnection]:
        return self.transaction_cls(self, **params)


class ABCDatabaseBackend(abc.ABC, Generic[TVConnection]):
    name: ClassVar[str]
    db_type: str
    _pool: Any

    connection_cls: ClassVar[type[ABCConnection]]

    def __init__(
        self,
        url: SplitResult,
        *,
        logger: logging.Logger = base_logger,
        convert_params: bool = False,
        init: Optional[TInitConnection] = None,
        **options,
    ):
        self.url = url
        self.init = init
        self.logger = logger
        self.convert_params = convert_params
        self.options: dict[str, Any] = dict(parse_qsl(url.query), **options)

    def __init_subclass__(cls, *args, **kwargs):
        """Register a new backend class."""
        BACKENDS.append(cls)
        return super().__init_subclass__(*args, **kwargs)

    def __str__(self):
        return self.db_type

    def __repr__(self):
        return f"<Backend {self}>"

    def __convert_sql__(self, sql: Any) -> str:
        return str(sql)

    @property
    def pool(self) -> Any:
        _pool = self._pool
        assert self._pool is not None, "Database is not connected"
        return _pool

    @pool.setter
    def pool(self, value):
        self._pool = value

    async def acquire(self) -> Any:
        conn = await self._acquire()
        init = self.init
        if init is not None:
            return await init(conn)
        return conn

    async def connect(self) -> None:
        self.logger.info("Connecting to %s", self.url)

    async def disconnect(self) -> None:
        self.logger.info("Disconnecting from %s", self.url)

    @abc.abstractmethod
    async def _acquire(self) -> TVConnection:
        raise NotImplementedError

    @abc.abstractmethod
    async def release(self, conn: TVConnection) -> None:
        raise NotImplementedError

    def connection(self, **params) -> ABCConnection[TVConnection]:
        return self.connection_cls(self, **params)


#  Import available backends
#  -------------------------

from ._dummy import Backend as DummyBackend  # A dummy backend for testing

assert issubclass(DummyBackend, ABCDatabaseBackend)

with suppress(ImportError):
    from ._aiosqlite import Backend as AIOSQLiteBackend

    assert issubclass(AIOSQLiteBackend, ABCDatabaseBackend)

with suppress(ImportError):
    from ._aiopg import Backend as AIOPGBackend
    from ._aiopg import PoolBackend as AIOPGPoolBackend

    assert issubclass(AIOPGBackend, ABCDatabaseBackend)
    assert issubclass(AIOPGPoolBackend, ABCDatabaseBackend)

with suppress(ImportError):
    from ._asyncpg import Backend as AsyncPGBackend
    from ._asyncpg import PoolBackend as AsyncPGPoolBackend

    assert issubclass(AsyncPGBackend, ABCDatabaseBackend)
    assert issubclass(AsyncPGPoolBackend, ABCDatabaseBackend)

with suppress(ImportError):
    from ._aiomysql import Backend as AIOMySQLBackend
    from ._aiomysql import PoolBackend as AIOMySQLPoolBackend

    assert issubclass(AIOMySQLBackend, ABCDatabaseBackend)
    assert issubclass(AIOMySQLPoolBackend, ABCDatabaseBackend)

with suppress(ImportError):
    from ._aioodbc import Backend as AIOODBCBackend

    assert issubclass(AIOODBCBackend, ABCDatabaseBackend)

with suppress(ImportError):
    from ._trio_mysql import Backend as TrioMySQLBackend

    assert issubclass(TrioMySQLBackend, ABCDatabaseBackend)

with suppress(ImportError):
    from ._triopg import Backend as TrioPGBackend

    assert issubclass(TrioPGBackend, ABCDatabaseBackend)

# ruff: noqa: E402
