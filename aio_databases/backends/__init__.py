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

    __slots__ = 'connection',

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

    def check_conn(self) -> ABCConnection:
        connection = self.connection
        if not connection.is_ready:
            raise RuntimeError('There is no an acquired connection to start a transaction')
        return connection

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type: t.Type[BaseException] = None, *args):
        connection = self.check_conn()
        if self in connection.transactions:
            if exc_type is not None:
                await self.rollback()
            else:
                await self.commit()

    async def start(self):
        connection = self.check_conn()
        await self._start()
        connection.transactions.add(self)

    async def commit(self):
        connection = self.check_conn()
        connection.transactions.remove(self)
        await self._commit()

    async def rollback(self):
        connection = self.check_conn()
        connection.transactions.remove(self)
        await self._rollback()


class ABCConnection(abc.ABC):

    transaction_cls: t.ClassVar[t.Type[ABCTransaction]]
    lock_cls: t.Type[asyncio.Lock] = asyncio.Lock

    __slots__ = 'backend', 'logger', 'transactions', '_conn', '_lock'

    def __init__(self, backend: ABCDatabaseBackend):
        self.backend = backend
        self.logger: logging.Logger = backend.logger
        self.transactions: t.Set[ABCTransaction] = set()
        self._conn: t.Any = None
        self._lock: asyncio.Lock = self.lock_cls()

    @property
    def is_ready(self) -> bool:
        return self._conn is not None

    async def acquire(self):
        if self._conn is None:
            async with self._lock:
                self._conn = await self.backend.acquire()

    async def release(self, *args):
        if self._conn is not None:
            async with self._lock:
                conn, self._conn = self._conn, None
                await self.backend.release(conn)

    async def execute(self, query: t.Any, *params, **options) -> t.Any:
        sql = self.backend.__convert_sql__(query)
        self.logger.debug((sql, *params))
        async with self._lock:
            return await self._execute(sql, *params, **options)

    async def executemany(self, query: t.Any, *params, **options) -> t.Any:
        sql = self.backend.__convert_sql__(query)
        self.logger.debug((sql, *params))
        async with self._lock:
            return await self._executemany(sql, *params, **options)

    async def fetchall(self, query: t.Any, *params, **options) -> t.List[t.Mapping]:
        sql = self.backend.__convert_sql__(query)
        self.logger.debug((sql, *params))
        async with self._lock:
            return await self._fetchall(sql, *params, **options)

    async def fetchmany(self, size: int, query: t.Any, *params, **options) -> t.List[t.Mapping]:
        sql = self.backend.__convert_sql__(query)
        self.logger.debug((sql, *params))
        async with self._lock:
            return await self._fetchmany(size, sql, *params, **options)

    async def fetchone(self, query: t.Any, *params, **options) -> t.Optional[t.Mapping]:
        sql = self.backend.__convert_sql__(query)
        self.logger.debug((sql, *params))
        async with self._lock:
            return await self._fetchone(sql, *params, **options)

    async def fetchval(self, query: t.Any, *params, column: t.Any = 0, **options) -> t.Any:
        sql = self.backend.__convert_sql__(query)
        self.logger.debug((sql, *params))
        async with self._lock:
            return await self._fetchval(sql, *params, column=column, **options)

    async def iterate(self, query: t.Any, *params, **options) -> t.AsyncIterator:
        sql = self.backend.__convert_sql__(query)
        self.logger.debug((sql, *params))
        async with self._lock:
            async for res in self._iterate(sql, *params, **options):
                yield res

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

    __slots__ = 'url', 'logger', 'convert_params', 'options', 'init'

    def __init__(self, url: SplitResult,
                 logger: logging.Logger = logger, convert_params: bool = False,
                 init: t.Callable[[t.Any], t.Awaitable[t.Any]] = None, **options):
        self.url = url
        self.init = init
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

    async def acquire(self) -> t.Any:
        conn = await self._acquire()
        init = self.init
        if init is not None:
            return await init(conn)
        return conn

    @abc.abstractmethod
    async def connect(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def disconnect(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def _acquire(self) -> t.Any:
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
