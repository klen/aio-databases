from __future__ import annotations

import pytest

from aio_databases import Database
from aio_databases.backends._dummy import Backend as DummyBackend
from aio_databases.backends._dummy import Connection as DummyConnection


class FakeConnError(Exception):
    """Simulate a driver-level connection error."""


class RawConn:
    """Simulate a raw driver connection."""

    dead = False


class FlakyConnection(DummyConnection):
    """Fail queries with a connection error while the raw connection is dead."""

    async def _fetchval(self, query: str, *params, column=0, **options):
        conn = self._conn
        assert conn is not None
        if conn.dead:
            raise FakeConnError("connection is closed")
        return 42


class FlakyBackend(DummyBackend):
    name = "flaky"
    connection_cls = FlakyConnection
    connection_errors = (FakeConnError,)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.acquire_attempts = 0
        self.acquire_fails = 0
        self.released = []

    async def _acquire(self):
        self.acquire_attempts += 1
        if self.acquire_fails:
            self.acquire_fails -= 1
            raise FakeConnError("cannot connect")
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
    return Database("flaky://")


async def test_acquire_failure_propagates(db: Database):
    db.backend.acquire_fails = 2
    with pytest.raises(FakeConnError, match="cannot connect"):
        async with db.connection(reconnect=True):
            pass

    assert db.backend.acquire_attempts == 1


async def test_reconnect_recovers_dead_connection(db: Database):
    async with db.connection(reconnect=True) as conn:
        assert await conn.fetchval("select 42") == 42

        # The raw connection dies: the current query fails and propagates...
        conn._conn.dead = True
        with pytest.raises(FakeConnError, match="connection is closed"):
            await conn.fetchval("select 42")

        # ...and the broken connection is re-acquired eagerly
        assert conn.is_ready
        assert db.backend.released

        # ...and the next query gets a fresh one
        assert await conn.fetchval("select 42") == 42
        assert conn.is_ready


async def test_reconnect_heals_context_connection(db: Database):
    async with db.connection(reconnect=True):
        assert await db.fetchval("select 42") == 42

        conn = db.current_conn
        conn._conn.dead = True
        with pytest.raises(FakeConnError, match="connection is closed"):
            await db.fetchval("select 42")

        # The next query reuses and revives the same connection from the context
        assert await db.fetchval("select 42") == 42
        assert db.current_conn is conn
        assert conn.is_ready


async def test_dead_connection_stays_dead_without_reconnect(db: Database):
    async with db.connection() as conn:
        assert await conn.fetchval("select 42") == 42

        conn._conn.dead = True
        with pytest.raises(FakeConnError, match="connection is closed"):
            await conn.fetchval("select 42")

        # Old behavior: the connection is not dropped and keeps failing
        assert conn.is_ready
        with pytest.raises(FakeConnError, match="connection is closed"):
            await conn.fetchval("select 42")
