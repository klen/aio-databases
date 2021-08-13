import pytest


def test_backends():
    from aio_databases.backends import BACKENDS

    assert BACKENDS
    assert BACKENDS['sqlite']
    assert BACKENDS['postgres']


def test_database():
    from aio_databases import Database

    with pytest.raises(ValueError):
        db = Database('unknown://db.sqlite')

    db = Database('sqlite://db.sqlite')
    assert db
