import pytest
from pypika_orm import Manager, Model


@pytest.fixture()
def aiolib():
    """There is only backend for asyncio."""
    return ("asyncio", {"use_uvloop": False})


@pytest.fixture()
def backend():
    return "aiosqlite"


@pytest.fixture()
def manager():
    return Manager(dialect="sqlite")


async def test_database(tmp_path):
    from aio_databases import Database

    pragmas = (
        ("journal_mode", "WAL"),
        ("synchronous", "NORMAL"),
    )

    db = Database("sqlite:///example.db")
    assert db
    assert db.backend.url

    async with Database("sqlite:///:memory:", pragmas=pragmas) as db:
        async with db.connection():
            assert await db.fetchval("select 1")

    async with Database(f"sqlite:///{tmp_path / 'db.sqlite'}", pragmas=pragmas) as db:
        async with db.connection():
            assert await db.fetchval("select 1")


async def test_persistent_db(tmp_path, user_cls: Model, manager: Manager):
    from aio_databases import Database

    user_manager = manager(user_cls)

    async with Database(f"sqlite:///{tmp_path / 'db.sqlite'}") as db:
        async with db.connection():
            await db.execute(user_manager.create_table())
            assert await db.execute(user_manager.insert(name="Tom", fullname="Tom Smith"))
            assert await db.fetchall(user_manager.select())

        async with db.connection():
            assert await db.fetchall(user_manager.select())
