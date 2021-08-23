from __future__ import annotations

import typing as t

import abc
import asyncio
from urllib.parse import SplitResult, parse_qsl

from ..record import Record


BACKENDS = {}


class ABCDabaseBackend(abc.ABC):

    name: t.ClassVar[str]
    record_cls = Record

    def __init__(self, url: SplitResult, **options):
        self.url = url
        self.options = dict(parse_qsl(url.query), **options)

    def __init_subclass__(cls, *args, **kwargs):
        """Register a backend."""
        super().__init_subclass__(*args, **kwargs)
        BACKENDS[cls.name] = cls

    @abc.abstractmethod
    async def connect(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def disconnect(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def connection(self) -> ABCConnection:
        raise NotImplementedError

    @abc.abstractmethod
    async def acquire(self) -> t.Any:
        raise NotImplementedError

    @abc.abstractmethod
    async def release(self, conn: t.Any) -> None:
        raise NotImplementedError


class ABCConnection(abc.ABC):

    def __init__(self, database: ABCDabaseBackend):
        self.database = database
        self.transactions: t.List[ABCTransaction] = []
        self._conn = None
        self._lock = asyncio.Lock()

    @property
    def conn(self):
        assert self._conn is not None, "Connection is not acquired"
        return self._conn

    async def acquire(self) -> ABCConnection:
        if self._conn is None:
            async with self._lock:
                self._conn = await self.database.acquire()

        return self

    __aenter__ = acquire

    async def release(self, *args) -> ABCConnection:
        async with self._lock:
            conn, self._conn = self._conn, None
            if conn:
                await self.database.release(conn)
            return self

    __aexit__ = release

    @abc.abstractmethod
    async def execute(self, query: str, *args, **params) -> t.Any:
        raise NotImplementedError

    @abc.abstractmethod
    async def executemany(self, query: str, *args, **params) -> t.Any:
        raise NotImplementedError

    @abc.abstractmethod
    async def fetchall(self, query: str, *args, **params) -> t.List[t.Mapping]:
        raise NotImplementedError

    @abc.abstractmethod
    async def fetchone(self, query: str, *args, **params) -> t.Optional[t.Mapping]:
        raise NotImplementedError

    @abc.abstractmethod
    async def fetchval(self, query: str, *args, column: t.Any = 0, **params) -> t.Any:
        raise NotImplementedError

    def transaction(self) -> ABCTransaction:
        raise NotImplementedError


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


from .sqlite import *  # noqa

try:
    from .postgres import *  # noqa
except ImportError:
    pass

try:
    from .mysql import *  # noqa
except ImportError:
    pass
