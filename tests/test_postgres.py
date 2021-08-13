import pytest
from sqlalchemy.schema import CreateTable, DropTable


@pytest.fixture
async def db():
    from aio_databases import Database

    async with Database('postgres://postgres:postgres@localhost:5432/test') as db:
        yield db


async def test_base(db):
    await db.execute('select $1', '1')

    await db.executemany('select $1', '1', '2', '3')

    res = await db.fetch('select 1')
    assert res == [(1,)]

    res = await db.fetchrow('select 1')
    assert res == (1,)

    res = await db.fetchval('select 2 + $1', 2)
    assert res == 4


async def test_tables(db, engine, users, addresses, caplog):
    await db.execute(CreateTable(users).compile(engine))

    res = await db.execute(
        'INSERT INTO users (name, fullname) VALUES ($1, $2)', 'jim', 'Jim Jones')
    assert res

    res = await db.execute(
        'INSERT INTO users (name, fullname) VALUES ($1, $2)', 'tom', 'Tom Smith')
    assert res

    res = await db.fetch(users.select())
    assert res
    assert res == [(1, 'jim', 'Jim Jones'), (2, 'tom', 'Tom Smith')]

    await db.execute(DropTable(users).compile(engine))
