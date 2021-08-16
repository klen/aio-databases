import pytest

from sqlalchemy.schema import CreateTable, DropTable


@pytest.fixture
async def db():
    from aio_databases import Database

    async with Database('postgresql://test:test@localhost:5432/tests') as db:
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


async def test_db(db, engine, users, addresses, caplog):
    await db.execute(CreateTable(users).compile(engine))

    async with db.transaction() as main_trans:
        assert main_trans

        res = await db.execute(
            'INSERT INTO users (name, fullname) VALUES ($1, $2)', 'jim', 'Jim Jones')
        assert res

        async with db.transaction() as trans2:
            assert trans2

            res = await db.execute(
                'INSERT INTO users (name, fullname) VALUES ($1, $2)', 'tom', 'Tom Smith')
            assert res

            await trans2.rollback()

    res = await db.fetch(users.select())
    assert res
    assert res == [(1, 'jim', 'Jim Jones')]

    await db.execute(DropTable(users).compile(engine))
