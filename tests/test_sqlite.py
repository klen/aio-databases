import pytest

from sqlalchemy.schema import CreateTable


@pytest.fixture
async def db():
    from aio_databases import Database

    async with Database('sqlite:///:memory:') as db:
        yield db


async def test_base(db):
    await db.execute('select $1', '1')

    await db.executemany('select $1', '1', '2', '3')

    res = await db.fetchall('select (2 * $1) res', 2)
    assert res == [(4,)]

    res = await db.fetchone('select (2 * $1) res', 2)
    assert res == (4,)
    assert isinstance(res, db.backend.record_cls)

    res = await db.fetchval('select 2 * $1', 2)
    assert res == 4


async def test_db(db, engine, users, addresses, caplog):
    Record = db.backend.record_cls

    await db.execute(CreateTable(users).compile(engine))

    async with db.transaction() as main_trans:
        assert main_trans

        res = await db.execute(
            'INSERT INTO users (name, fullname) VALUES (?, ?)', 'jim', 'Jim Jones')
        assert res

        async with db.transaction() as trans2:
            assert trans2

            res = await db.execute(
                'INSERT INTO users (name, fullname) VALUES (?, ?)', 'tom', 'Tom Smith')
            assert res

            await trans2.rollback()

    res = await db.fetchall(users.select())
    assert res
    assert res == [Record.from_dict({'id': 1, 'name': 'jim', 'fullname': 'Jim Jones'})]

    res = await db.fetchone(users.select().where(users.c.id == 100), 100)
    assert res is None
