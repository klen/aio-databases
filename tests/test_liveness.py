from __future__ import annotations

import pytest

from aio_databases import Database
from aio_databases.backends._asyncpg import Connection as AsyncPGConnection
from aio_databases.backends._dummy import Backend as DummyBackend
from aio_databases.backends._dummy import Connection as DummyConnection


class RawConn:
    """Simulate a raw driver connection."""

    dead = False


class LivenessConnection(DummyConnection):
    """Report a dead raw connection as not ready."""

    @property
    def is_ready(self) -> bool:
        conn = self._conn
        return conn is not None and not conn.dead

    async def _fetchval(self, query: str, *params, column=0, **options):
        conn = self._conn
        assert conn is not None
        if conn.dead:
            raise RuntimeError("connection is closed")
        return 42


class LivenessBackend(DummyBackend):
    name = "liveness"
    connection_cls = LivenessConnection
    connection_errors = (RuntimeError,)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.acquired = 0
        self.released: list[RawConn] = []

    async def _acquire(self):
        self.acquired += 1
        return RawConn()

    async def release(self, conn):
        self.released.append(conn)


@pytest.fixture
def aiolib():
    """There is only backend for asyncio."""
    return ("asyncio", {"loop_factory": None})


@pytest.fixture
def backend():
    return "aiosqlite"


@pytest.fixture
def db():
    return Database("dummy://")


async def test_proactive_reconnect_heals_dead_connection():
    ldb = Database("liveness://")
    async with ldb, ldb.connection(reconnect=True) as conn:
        assert await ldb.fetchval("select 42") == 42

        # The raw connection dies while idle
        conn._conn.dead = True
        assert not conn.is_ready

        # The context detects it and re-acquires before running the query
        assert await ldb.fetchval("select 42") == 42
        assert conn.is_ready
        assert not conn._conn.dead
        assert ldb.backend.released


def test_asyncpg_connection_is_ready():
    class FakeRaw:
        def __init__(self, closed):
            self.closed = closed

        def is_closed(self):
            return self.closed

    class DetachedProxy:
        def is_closed(self):
            raise AttributeError("connection has been released")

    conn = AsyncPGConnection.__new__(AsyncPGConnection)
    conn._conn = None
    assert conn.is_ready is False

    conn._conn = FakeRaw(closed=False)
    assert conn.is_ready is True

    conn._conn = FakeRaw(closed=True)
    assert conn.is_ready is False

    conn._conn = DetachedProxy()
    assert conn.is_ready is False
