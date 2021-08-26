# AIO-Databases

The package gives you asycio support for a range of databases (SQLite,
PostgreSQL, MySQL).

[![Tests Status](https://github.com/klen/aio-databases/workflows/tests/badge.svg)](https://github.com/klen/aio-databases/actions)
[![PYPI Version](https://img.shields.io/pypi/v/aio-databases)](https://pypi.org/project/aio-databases/)
[![Python Versions](https://img.shields.io/pypi/pyversions/aio-databases)](https://pypi.org/project/aio-databases/)


## Requirements

* python >= 3.7

## Installation

**aio-databases** should be installed using pip:

```shell
$ pip install aio-databases
```

You can install the required database drivers with:

```shell
$ pip install aio-databases[sqlite]
$ pip install aio-databases[postgresql]
$ pip install aio-databases[mysql]
```


## Usage

* Init a database

```python
    from aio_databases import Database

    db = Database('sqlite:///:memory:')
```

* Prepare the database to work

```python
    async def my_app_starts():
        # Initialize a database's pool
        await db.connect()

    async def my_app_ends():
        # Close pool/connections
        await db.disconnect()

    # As an alternative users are able to use the database
    # as a async context manager

    async with db:
        await my_main_coroutine()
```

* Run SQL queries

```python
    await db.execute('select $1', '1')
    await db.executemany('select $1', '1', '2', '3')

    records = await db.fetchall('select (2 * $1) res', 2)
    assert records == [(4,)]

    record = await db.fetchone('select (2 * $1) res', 2)
    assert record == (4,)
    assert record['res'] == 4

    result = await db.fetchval('select 2 * $1', 2)
    assert result == 4
```

* Use transactions

```python
    async with db.transaction() as trans1:
        # do some work ...

        async with db.transaction() as trans2:
            # do some work ...
            await trans2.rollback()
```

## Bug tracker

If you have any suggestions, bug reports or annoyances please report them to
the issue tracker at https://github.com/klen/aio-databases/issues


## Contributing

Development of the project happens at: https://github.com/klen/aio-databases


## License

Licensed under a [MIT License](http://opensource.org/licenses/MIT)
