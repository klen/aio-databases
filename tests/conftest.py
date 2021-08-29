import pytest
import logging

from pypika_orm import Manager, Model, fields


BACKEND_PARAMS = {
    'aiomysql': ('aiomysql://root@127.0.0.1:3306/tests', {'maxsize': 2}),
    'aiopg': ('aiopg://test:test@localhost:5432/tests', {'maxsize': 2}),
    'aiosqlite': ('aiosqlite:///:memory:', {}),
    'asyncpg': ('asyncpg://test:test@localhost:5432/tests', {'min_size': 2, 'max_size': 2}),
}


@pytest.fixture(scope='session')
def aiolib():
    """Support asyncio only. Disable uvloop on tests it brokes breakpoints."""
    return ('asyncio', {'use_uvloop': False})


@pytest.fixture(scope='session', params=['aiomysql', 'aiopg', 'aiosqlite', 'asyncpg'])
def backend(request):
    return request.param


@pytest.fixture
def param(backend):
    if backend == 'aiosqlite':
        return lambda: '?'

    if backend in ('aiomysql', 'aiopg'):
        return lambda: '%s'

    gen = (f"${n}" for n in range(1, 10))
    return lambda: next(gen)


@pytest.fixture
async def db(backend):
    from aio_databases import Database

    url, params = BACKEND_PARAMS[backend]
    async with Database(url, **params) as db:
        yield db


@pytest.fixture(scope='session', autouse=True)
def setup_logging():
    logger = logging.getLogger('aio-databases')
    logger.setLevel(logging.DEBUG)


@pytest.fixture
def manager(db):
    return Manager(dialect=db.backend.db_type)


@pytest.fixture
def User():

    class User(Model):
        id = fields.Auto()
        name = fields.Varchar()
        fullname = fields.Varchar()

    return User


@pytest.fixture
def Comment(User):

    class Comment(Model):
        id = fields.Auto()
        body = fields.Varchar()

        user_id = fields.ForeignKey(User.id)

    return Comment
