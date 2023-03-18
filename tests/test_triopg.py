from typing import Any

import pytest
import trio_asyncio
from pypika import Parameter
from pypika_orm import Manager, Model


@pytest.fixture()
def aiolib():
    return "trio"


async def test_base(user_cls: Model):
    from aio_databases import Database

    async with trio_asyncio.open_loop():
        async with Database("triopg://test:test@localhost:5432/tests", convert_params=True) as db:
            db.backend.name = "postgresql"  # type: ignore[misc]  # fix for pika orm
            manager = Manager(db)
            user_manager = manager(user_cls)

            await db.execute("select %s", "1")

            res: Any = await db.fetchall("select (2 * %s) res", 2)
            assert [tuple(r) for r in res] == [(4,)]

            res = await db.fetchmany(10, "select (2 * %s) res", 2)
            assert [tuple(r) for r in res] == [(4,)]

            res = await db.fetchone("select (2 * %s) res", 2)
            assert tuple(res) == (4,)

            res = await db.fetchval("select 2 + %s", 2)
            assert res == 4

            await db.execute(user_manager.drop_table().if_exists())
            await db.execute(user_manager.create_table().if_not_exists())

            async with db.transaction() as main_trans:
                assert main_trans

                res = await db.execute(user_manager.insert(name="Jim", fullname="Jim Jones"))
                assert res

                async with db.transaction() as trans2:
                    assert trans2

                    res = await db.execute(user_manager.insert(name="Tom", fullname="Tom Smith"))
                    assert res

                    res = await db.fetchall(user_manager.select())
                    assert res
                    assert len(res) == 2

                    await trans2.rollback()

            res = await db.fetchall(user_manager.select())
            assert res
            assert len(res) == 1

            [user] = res
            assert user
            assert user["id"]
            assert user["name"] == "Jim"
            assert user["fullname"] == "Jim Jones"

            res = await db.execute(user_manager.update().set(user_cls.name, "Newbie"))
            assert res == (1, None)

            res = await db.fetchone(user_manager.select().where(user_cls.id == 100))
            assert res is None

            await db.execute(user_manager.delete())
            qs = user_manager.insert(name=Parameter("$1"), fullname=Parameter("$2"))
            await db.executemany(qs, ("Jim", "Jim Jones"), ("Tom", "Tom Smith"))
            res = await db.fetchall(user_manager.select())
            assert res
            assert len(res) == 2
            u1, u2 = res
            assert u1["name"] == "Jim"
            assert u2["name"] == "Tom"

            async for rec in db.iterate(user_manager.select()):
                assert rec["name"] in {"Jim", "Tom"}

            await db.execute(user_manager.drop_table().if_exists())
