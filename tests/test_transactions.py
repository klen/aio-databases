from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pypika_orm import Manager, Model

    from aio_databases import Database


async def test_base(db: Database):
    res = None
    async with db.connection(), db.transaction():
        res = await db.fetchval("select 1")

    assert res == 1

    with pytest.raises(RuntimeError):
        async with db.connection(create=False) as conn:
            async with db.transaction():
                await conn.release()

    async with db.connection(create=False) as conn, db.transaction(silent=True):
        await conn.release()


async def test_child_tasks(db: Database, aiolib: str):
    if aiolib[0] == "trio":
        return pytest.skip()

    res = await db.fetchval("select 1")
    assert res == 1

    async def process():
        async with db.connection(), db.transaction():
            return await db.fetchval("select 1")

    res = await asyncio.gather(process(), process(), process(), process())
    assert res == [1, 1, 1, 1]


@pytest.mark.parametrize("backend", ["aiopg", "aiosqlite", "asyncpg"])
async def test_auto_rollback(db: Database, user_cls: Model, manager: Manager):
    user_manager = manager(user_cls)

    await db.execute(user_manager.create_table().if_not_exists())

    with pytest.raises(Exception, match="test"):
        async with db.transaction():
            await db.execute(user_manager.insert(name="Jim", fullname="Jim Jones"))
            res = await db.fetchall(user_manager.select())
            assert len(res) == 1
            raise Exception("test")  # noqa:  TRY002

    res = await db.fetchall(user_manager.select())
    assert len(res) == 0


async def test_nested(pool: Database, user_cls: Model, manager: Manager):
    db = pool
    user_manager = manager(user_cls)
    await db.execute(user_manager.create_table().if_not_exists())
    await db.execute(user_manager.delete())

    async with db.transaction() as main_trans:
        assert main_trans

        res = await db.execute(user_manager.insert(name="Jim", fullname="Jim Jones"))
        assert res

        res = await db.fetchall(user_manager.select())
        assert res
        assert len(res) == 1

        async with db.transaction() as trans2:
            assert trans2

            res = await db.execute(user_manager.insert(name="Tom", fullname="Tom Smith"))
            assert res

            res = await db.fetchall(user_manager.select())
            assert res
            assert len(res) == 2

            async with db.transaction() as trans3:
                assert trans3

                res = await db.execute(user_manager.insert(name="Jerry", fullname="Jerry Mitchel"))
                assert res

                res = await db.fetchall(user_manager.select())
                assert res
                assert len(res) == 3

                await trans3.rollback()

            res = await db.fetchall(user_manager.select())
            assert res
            assert len(res) == 2

            await trans2.rollback()

        res = await db.fetchall(user_manager.select())
        assert res
        assert len(res) == 1

        await main_trans.rollback()

    res = await db.fetchall(user_manager.select())
    assert not res

    await db.execute(user_manager.drop_table().if_exists())


async def test_connections(db: Database):
    async with db.transaction() as trans1:
        assert trans1

        async with db.transaction() as trans2:
            assert trans2
            assert trans1.connection is trans2.connection

            async with db.transaction(create=True) as trans3:
                assert trans3
                assert trans3.connection is not trans2.connection


@pytest.mark.skip("not sure the lib has to implement it")
@pytest.mark.parametrize("aiolib", ["asyncio"])
async def test_concurency(db: Database, manager: Manager, user_cls: Model):

    user_manager = manager(user_cls)
    async with db.connection():
        await db.execute(user_manager.create_table().if_not_exists())
        await db.execute(user_manager.insert(name="Tom", fullname="Tom Smith"))

        async def task1():
            async with db.transaction():
                await asyncio.sleep(1e-2)
                qs = user_manager.update().set(user_cls.name, "Jack").where(user_cls.name == "Tom")
                await db.execute(qs)

        async def task2():
            qs = user_manager.update().set(user_cls.name, "Mike").where(user_cls.name == "Tom")
            await db.execute(qs)

        await asyncio.gather(task1(), task2())

        user = await db.fetchone(user_manager.select())
        assert user is not None
        assert user["name"] == "Jack"

        await db.execute(user_manager.drop_table().if_exists())
