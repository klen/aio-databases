import pytest

from pypika_orm import Manager, Model, fields


@pytest.fixture(scope='session')
def aiolib():
    """Support asyncio only. Disable uvloop on tests it brokes breakpoints."""
    return ('asyncio', {'use_uvloop': False})


@pytest.fixture
def dialect():
    return 'sqllite'


@pytest.fixture
def manager(dialect):
    return Manager(dialect=dialect)


@pytest.fixture
def User():

    class User(Model):
        id = fields.Auto()
        name = fields.Varchar()
        fullname = fields.Varchar()

        class Meta:
            primary_key = 'id',

    return User
