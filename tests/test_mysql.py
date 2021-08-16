import os
import pytest


DB_HOST = os.environ.get('MYSQL_HOST', 'localhost')


@pytest.fixture
async def db():
    from aio_databases import Database

    async with Database(f"mysql://test:test@{DB_HOST}:3306/tests") as db:
        yield db


async def test_base(db):
    await db.execute('select %s', '1')

    await db.executemany('select %s', '1', '2', '3')

    res = await db.fetch('select 1')
    assert res == [(1,)]

    res = await db.fetchrow('select 1')
    assert res == (1,)

    res = await db.fetchval('select 2 + %s', 2)
    assert res == 4
