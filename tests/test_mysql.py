import pytest

from sqlalchemy import create_engine
from sqlalchemy.schema import CreateTable, DropTable


@pytest.fixture
async def db():
    from aio_databases import Database

    async with Database('mysql://root@127.0.0.1:3306/tests') as db:
        yield db


@pytest.fixture
def engine(db):
    return create_engine(db.url.replace('mysql', 'mysql+pymysql'), echo=True)


async def test_base(db):
    await db.execute('select %s', '1')

    await db.executemany('select %s', '1', '2', '3')

    res = await db.fetchall('select (2 * %s) res', 2)
    assert res == [(4,)]

    res = await db.fetchone('select (2 * %s) res', 2)
    assert res == (4,)
    assert isinstance(res, db.backend.record_cls)

    res = await db.fetchval('select 2 + %s', 2)
    assert res == 4


async def test_db(db, engine, users, addresses, caplog):
    try:
        await db.execute(DropTable(users).compile(engine))
    except Exception:
        pass

    Record = db.backend.record_cls

    await db.execute(CreateTable(users).compile(engine))

    async with db.transaction() as main_trans:
        assert main_trans

        res = await db.execute(
            'INSERT INTO users (name, fullname) VALUES (%s, %s)', 'jim', 'Jim Jones')
        assert res

        async with db.transaction() as trans2:
            assert trans2

            res = await db.execute(
                'INSERT INTO users (name, fullname) VALUES (%s, %s)', 'tom', 'Tom Smith')
            assert res

            await trans2.rollback()

    res = await db.fetchall(users.select())
    assert res
    assert res == [Record.from_dict({'id': 1, 'name': 'jim', 'fullname': 'Jim Jones'})]

    await db.execute(DropTable(users).compile(engine))
