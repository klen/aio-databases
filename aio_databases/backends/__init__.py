from __future__ import annotations

import typing as t

import abc
import asyncio
import logging
from re import compile as re
from urllib.parse import SplitResult, parse_qsl

from .. import logger


BACKENDS = []
SHORTCUTS = {
    'postgres': 'postgresql',
    'postgressql': 'postgresql',
    'sqllite': 'sqlite',
}
RE_PARAM = re(r'([^%])(%s)')


class ABCTransaction(abc.ABC):

    is_finished: bool = False

    def __init__(self, connection: ABCConnection):
        self.connection = connection

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

    async def __aexit__(self, exc_type: t.Type[BaseException] = None, *args):
        if not self.is_finished:
            if exc_type is not None:
                await self.rollback()
            else:
                await self.commit()

    async def start(self):
        connection = self.connection
        if not connection.is_ready:
            raise RuntimeError('There is no an acquired connection to make a transaction')

        async with connection._lock:
            await self._start()
            connection.transactions.append(self)

    async def commit(self):
        connection = self.connection
        async with connection._lock:
            await self._commit()
            connection.transactions.remove(self)
            self.is_finished = True

    async def rollback(self):
        connection = self.connection
        async with connection._lock:
            await self._rollback()
            connection.transactions.remove(self)
            self.is_finished = True


class ABCConnection(abc.ABC):

    transaction_cls: t.ClassVar[t.Type[ABCTransaction]]
    lock_cls: t.Type[asyncio.Lock] = asyncio.Lock

    __slots__ = 'database', 'logger', 'transactions', '_conn', '_lock'

    def __init__(self, database: ABCDatabaseBackend):
        self.database = database
        self.logger: logging.Logger = database.logger
        self.transactions: t.List[ABCTransaction] = []
        self._conn = None
        self._lock: asyncio.Lock = self.lock_cls()

    @property
    def conn(self):
        assert self.is_ready, "Connection is not acquired"
        return self._conn

    @property
    def is_ready(self) -> bool:
        return self._conn is not None

    async def acquire(self) -> ABCConnection:
        if self._conn is None:
            async with self._lock:
                self._conn = await self.database.acquire()

        return self

    __aenter__ = acquire

    async def release(self, *args):
        async with self._lock:
            conn, self._conn = self._conn, None
            if conn:
                await self.database.release(conn)

    __aexit__ = release

    def execute(self, query: t.Any, *params, **options) -> t.Awaitable:
        sql = self.database.__convert_sql__(query)
        self.logger.debug((sql, *params))
        return self._execute(sql, *params, **options)

    def executemany(self, query: t.Any, *params, **options) -> t.Awaitable:
        sql = self.database.__convert_sql__(query)
        self.logger.debug((sql, *params))
        return self._executemany(sql, *params, **options)

    def fetchall(self, query: t.Any, *params, **options) -> t.Awaitable:
        sql = self.database.__convert_sql__(query)
        self.logger.debug((sql, *params))
        return self._fetchall(sql, *params, **options)

    def fetchmany(self, size: int, query: t.Any, *params, **options) -> t.Awaitable:
        sql = self.database.__convert_sql__(query)
        self.logger.debug((sql, *params))
        return self._fetchmany(size, sql, *params, **options)

    def fetchone(self, query: t.Any, *params, **options) -> t.Awaitable:
        sql = self.database.__convert_sql__(query)
        self.logger.debug((sql, *params))
        return self._fetchone(sql, *params, **options)

    def fetchval(self, query: t.Any, *params, column: t.Any = 0, **options) -> t.Awaitable:
        sql = self.database.__convert_sql__(query)
        self.logger.debug((sql, *params))
        return self._fetchval(sql, *params, column=column, **options)

    def iterate(self, query: t.Any, *params, **options) -> t.AsyncIterator:
        sql = self.database.__convert_sql__(query)
        self.logger.debug((sql, *params))
        return self._iterate(sql, *params, **options)

    @abc.abstractmethod
    async def _execute(self, query: str, *params, **options) -> t.Any:
        raise NotImplementedError

    @abc.abstractmethod
    async def _executemany(self, query: str, *params, **options) -> t.Any:
        raise NotImplementedError

    @abc.abstractmethod
    async def _fetchall(self, query: str, *params, **options) -> t.List[t.Mapping]:
        raise NotImplementedError

    @abc.abstractmethod
    async def _fetchmany(self, size: int, query: str, *params, **options) -> t.List[t.Mapping]:
        raise NotImplementedError

    @abc.abstractmethod
    async def _fetchone(self, query: str, *params, **options) -> t.Optional[t.Mapping]:
        raise NotImplementedError

    @abc.abstractmethod
    async def _fetchval(self, query: str, *params, column: t.Any = 0, **options) -> t.Any:
        raise NotImplementedError

    @abc.abstractmethod
    async def _iterate(self, query: str, *params, **options) -> t.AsyncIterator:
        raise NotImplementedError
        yield

    def transaction(self) -> ABCTransaction:
        return self.transaction_cls(self)


class ABCDatabaseBackend(abc.ABC):

    name: t.ClassVar[str]
    db_type: t.ClassVar[str]

    connection_cls: t.ClassVar[t.Type[ABCConnection]]

    __slots__ = 'url', 'logger', 'convert_params', 'options'

    def __init__(self, url: SplitResult, logger: logging.Logger = logger,
                 convert_params: bool = False, **options):
        self.url = url
        self.logger = logger
        self.convert_params = convert_params
        self.options = dict(parse_qsl(url.query), **options)

    def __init_subclass__(cls, *args, **kwargs):
        """Register a backend."""
        BACKENDS.append(cls)
        return super().__init_subclass__(*args, **kwargs)

    def __str__(self):
        return self.db_type

    def __repr__(self):
        return f"<Backend {self}>"

    def __convert_sql__(self, sql: t.Any) -> str:
        return str(sql)

    @abc.abstractmethod
    async def connect(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def disconnect(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def acquire(self) -> t.Any:
        raise NotImplementedError

    @abc.abstractmethod
    async def release(self, conn: t.Any) -> None:
        raise NotImplementedError

    def connection(self) -> ABCConnection:
        return self.connection_cls(self)


#  Import available backends
#  -------------------------

from ._dummy import Backend as DummyBackend  # A dummy backend for testing  # noqa

try:
    from ._aiosqlite import Backend as AIOSQLiteBackend  # noqa
except ImportError:
    pass

try:
    from ._aiopg import Backend as AIOPGBackend  # noqa
except ImportError:
    pass

try:
    from ._asyncpg import Backend as AsyncPGBackend  # noqa
except ImportError:
    pass

try:
    from ._aiomysql import Backend as AIOMySQLBackend  # noqa
except ImportError:
    pass

try:
    from ._aioodbc import Backend as AIOODBCBackend  # noqa
except ImportError:
    pass

try:
    from ._trio_mysql import Backend as TrioMySQLBackend  # noqa
except ImportError:
    pass

try:
    from ._triopg import Backend as TrioPGBackend  # noqa
except ImportError:
    pass
