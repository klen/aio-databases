import pytest
import logging

from pypika_orm import Manager, Model, fields


@pytest.fixture(scope='session')
def aiolib():
    """Support asyncio only. Disable uvloop on tests it brokes breakpoints."""
    return ('asyncio', {'use_uvloop': False})


@pytest.fixture
def dialect():
    return 'sqllite'


@pytest.fixture(scope='session', autouse=True)
def setup_logging():
    logger = logging.getLogger('aio-databases')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())


@pytest.fixture
def manager(dialect):
    return Manager(dialect=dialect)


@pytest.fixture
def User():

    class User(Model):
        id = fields.Auto()
        name = fields.Varchar()
        fullname = fields.Varchar()

    return User


@pytest.fixture
async def db(db_url):
    from aio_databases import Database

    async with Database(db_url) as db:
        yield db


@pytest.fixture
def Comment(User):

    class Comment(Model):
        id = fields.Auto()
        body = fields.Varchar()

        user_id = fields.ForeignKey(User.id)

    return Comment
