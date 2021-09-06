import pytest

from pypika_orm import Manager


@pytest.fixture
def backend():
    return 'aiosqlite'


@pytest.fixture
def manager():
    return Manager(dialect='sqlite')


async def test_database(tmp_path):
    from aio_databases import Database

    async with Database("sqlite:///:memory:") as db:
        async with db.connection():
            assert await db.fetchval('select 1')

    async with Database(f"sqlite:///{tmp_path / 'db.sqlite'}") as db:
        async with db.connection():
            assert await db.fetchval('select 1')


async def test_persistent_db(tmp_path, User, manager):
    from aio_databases import Database

    UserManager = manager(User)

    async with Database(f"sqlite:///{tmp_path / 'db.sqlite'}") as db:
        async with db.connection():
            await db.execute(UserManager.create_table())
            assert await db.execute(UserManager.insert(name='Tom', fullname='Tom Smith'))
            assert await db.fetchall(UserManager.select())

        async with db.connection():
            assert await db.fetchall(UserManager.select())
