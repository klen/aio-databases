from datetime import datetime, timezone

import pytest
from pypika_orm import Manager, Model

from aio_databases import Database


@pytest.fixture
def aiolib():
    """There is only backend for asyncio."""
    return ("asyncio", {"loop_factory": None})


@pytest.fixture
def backend():
    return "aiosqlite"


@pytest.fixture
def manager():
    return Manager(dialect="sqlite")


async def test_database(tmp_path):

    pragmas = (
        ("journal_mode", "WAL"),
        ("synchronous", "NORMAL"),
    )

    db = Database("sqlite:///example.db")
    assert db
    assert db.backend.url

    async with Database("sqlite:///:memory:", pragmas=pragmas) as db, db.connection():
        assert await db.fetchval("select 1")

    def date_part(lookup_type, dtstr):
        dt = datetime.strptime(dtstr, "%Y-%m-%d %H:%M:%S")  # noqa: DTZ007
        return getattr(dt, lookup_type)

    async with (
        Database(
            f"sqlite:///{tmp_path / 'db.sqlite'}",
            pragmas=pragmas,
        ) as db,
        db.connection(),
    ):
        assert await db.fetchval("select 1")

    async with (
        Database(
            f"sqlite:///{tmp_path / 'db.sqlite'}", functions=(("date_part", 2, date_part),)
        ) as db,
        db.connection(),
    ):
        res = await db.fetchval("select date_part('day', datetime())")
        assert res == datetime.now(tz=timezone.utc).day


async def test_sqlite_three_slash_url_is_absolute(tmp_path):
    # Three-slash URLs (sqlite:///path) should resolve to absolute paths.
    # The leading '/' in url.path must not be stripped.
    db_path = tmp_path / "absolute.db"
    # Remove leading '/' so the URL has exactly 3 slashes; urlsplit
    # will add it back into url.path.
    relative_from_root = str(db_path).lstrip("/")
    db = Database(f"sqlite:///{relative_from_root}")
    async with db, db.connection():
        await db.execute("CREATE TABLE t (x INT)")
    assert db_path.exists()


async def test_persistent_db(tmp_path, user_cls: Model, manager: Manager):

    user_manager = manager(user_cls)

    async with Database(f"sqlite:///{tmp_path / 'db.sqlite'}") as db:
        async with db.connection():
            await db.execute(user_manager.create_table())
            assert await db.execute(user_manager.insert(name="Tom", fullname="Tom Smith"))
            assert await db.fetchall(user_manager.select())

        async with db.connection():
            assert await db.fetchall(user_manager.select())


async def test_uri_mode_shared_memory():
    """Support aiosqlite URI filenames with uri=True."""
    db = Database("aiosqlite://file:memdb1?mode=memory&cache=shared", uri=True)
    async with db, db.connection():
        await db.execute("CREATE TABLE t (x INT)")
        await db.execute("INSERT INTO t VALUES (1)")
        # Nested second connection to the same shared in-memory DB sees the data.
        async with db.connection() as conn2:
            assert await conn2.fetchval("SELECT x FROM t") == 1


async def test_uri_mode_memory():
    """uri=True works with :memory: style URLs."""
    db = Database("aiosqlite:///:memory:", uri=True)
    async with db, db.connection():
        await db.execute("CREATE TABLE t (x INT)")
        assert await db.fetchval("SELECT 1") == 1
