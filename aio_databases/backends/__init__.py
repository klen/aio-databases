from __future__ import annotations

import typing as t

import abc
import asyncio
from urllib.parse import SplitResult, parse_qsl


BACKENDS = {}


class ABCDabaseBackend(abc.ABC):

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
    def connection(self) -> ABCConnectionBackend:
        raise NotImplementedError

    @abc.abstractmethod
    async def acquire(self) -> t.Any:
        raise NotImplementedError

    @abc.abstractmethod
    async def release(self, conn: t.Any) -> None:
        raise NotImplementedError


class ABCConnectionBackend(abc.ABC):

    def __init__(self, database: ABCDabaseBackend):
        self.database = database
        self._conn = None
        self._lock = asyncio.Lock()

    @property
    def conn(self):
        assert self._conn is not None, "Connection is not acquired"
        return self._conn

    async def acquire(self) -> ABCConnectionBackend:
        if self._conn is None:
            async with self._lock:
                self._conn = await self.database.acquire()

        return self

    __aenter__ = acquire

    async def release(self, *args) -> ABCConnectionBackend:
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
    async def fetch(self, query: str, *args, **params) -> t.List[t.Tuple]:
        raise NotImplementedError

    @abc.abstractmethod
    async def fetchrow(self, query: str, *args, **params) -> t.Optional[t.Tuple]:
        raise NotImplementedError

    @abc.abstractmethod
    async def fetchval(self, query: str, *args, column: t.Any = 0, **params) -> t.Any:
        raise NotImplementedError


from .sqlite import *  # noqa

try:
    from .postgres import *  # noqa
except ImportError:
    pass

try:
    from .mysql import *  # noqa
except ImportError:
    pass
