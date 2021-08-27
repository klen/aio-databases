import pytest


@pytest.fixture(scope='module')
def db_url():
    return 'postgresql://test:test@localhost:5432/tests'


@pytest.fixture(scope='module')
async def dialect():
    return 'postgressql'


@pytest.fixture
async def db(db_url):
    from aio_databases import Database

    async with Database(db_url) as db:
        yield db


async def test_transaction(db_url):
    from aio_databases import Database

    async with Database(db_url) as db:
        async with db.transaction() as trans:
            res = await db.fetchval('select 2 * $1', 2)
            assert res == 4
            await trans.commit()


async def test_base(db):
    await db.execute('select $1', '1')

    await db.executemany('select $1', '1', '2', '3')

    res = await db.fetchall('select (2 * $1) res', 2)
    assert res == [(4,)]

    res = await db.fetchone('select (2 * $1) res', 2)
    assert res == (4,)
    assert isinstance(res, db.backend.record_cls)

    res = await db.fetchval('select 2 + $1', 2)
    assert res == 4


async def test_db(db, User, manager):
    UserManager = manager(User)

    await db.execute(UserManager.drop_table().if_exists())
    await db.execute(UserManager.create_table())

    async with db.transaction() as main_trans:
        assert main_trans

        res = await db.execute(UserManager.insert(name='jim', fullname='Jim Jones'))
        assert res

        async with db.transaction() as trans2:
            assert trans2

            res = await db.execute(UserManager.insert(name='tom', fullname='Tom Smith'))
            assert res

            res = await db.fetchall(UserManager.select())
            assert res
            assert len(res) == 2

            await trans2.rollback()

    res = await db.fetchall(UserManager.select())
    assert res
    assert len(res) == 1

    [user] = res
    assert user
    assert user['id'] == 1
    assert user['name'] == 'jim'
    assert user['fullname'] == 'Jim Jones'

    res = await db.fetchone(UserManager.select().where(User.id == 100))
    assert res is None

    await db.execute(UserManager.drop_table().if_exists())
