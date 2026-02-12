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


async def test_persistent_db(tmp_path, user_cls: Model, manager: Manager):

    user_manager = manager(user_cls)

    async with Database(f"sqlite:///{tmp_path / 'db.sqlite'}") as db:
        async with db.connection():
            await db.execute(user_manager.create_table())
            assert await db.execute(user_manager.insert(name="Tom", fullname="Tom Smith"))
            assert await db.fetchall(user_manager.select())

        async with db.connection():
            assert await db.fetchall(user_manager.select())
