import pytest

from aio_databases.database import Database


@pytest.fixture
def aiolib():
    """There is only backend for asyncio."""
    return ("asyncio", {"use_uvloop": False})


@pytest.fixture
def backend():
    return "asyncpg"


@pytest.fixture
def dbparams():
    return {"json": True}


async def test_json(db: Database):
    await db.execute("drop table if exists test_json")
    await db.execute("create table test_json (data json)")
    await db.execute("insert into test_json (data) values ($1)", {"a": 1, "b": 2})
    res = await db.fetchval("select data from test_json")
    assert res == {"a": 1, "b": 2}
    await db.execute("drop table if exists test_json")
