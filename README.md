# AIO-Databases

The package gives you asycio support for a range of databases (SQLite,
PostgreSQL, MySQL).

[![Tests Status](https://github.com/klen/aio-databases/workflows/tests/badge.svg)](https://github.com/klen/aio-databases/actions)
[![PYPI Version](https://img.shields.io/pypi/v/aio-databases)](https://pypi.org/project/aio-databases/)
[![Python Versions](https://img.shields.io/pypi/pyversions/aio-databases)](https://pypi.org/project/aio-databases/)

## Features

* Has no dependencies (except databases drivers)
* Supports [aiosqlite](https://github.com/omnilib/aiosqlite),
  [aiomysql](https://github.com/aio-libs/aiomysql),
  [aiopg](https://github.com/aio-libs/aiopg),
  [asyncpg](https://github.com/MagicStack/asyncpg)
* Manage pools of connections
* Manage transactions

## Requirements

* python >= 3.7

## Installation

**aio-databases** should be installed using pip:

```shell
$ pip install aio-databases
```

You have to choose and install the required database drivers with:

```shell
# To support SQLite
$ pip install aio-databases[aiosqlite]

# To support MySQL
$ pip install aio-databases[aiomysql]

# To support PostgreSQL (choose one)
$ pip install aio-databases[aiopg]
$ pip install aio-databases[asyncpg]
```


## Usage

* Init a database

```python
    from aio_databases import Database

    db = Database('sqlite:///:memory:')
```

* Prepare the database to work

```python
    # Initialize a database's pool
    async def my_app_starts():
        await db.connect()

    # Close the pool
    async def my_app_ends():
        await db.disconnect()

    # As an alternative users are able to use the database
    # as an async context manager

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

* Manage connections (please ensure that you have released a connection after
  acquiring)

```python

    # Create a new connection object
    conn = db.connection()

    # or use the existing which one is binded for the current task
    conn = db.connection(False)

    # Acquire DB connection
    await conn.acquire()
    # ...
    # Release DB connection
    await conn.relese()

    # an alternative (acquire/release as an async context)
    async with db.connection():
        # ...
```

* Use transactions

```python
    async with db.transaction() as trans1:
        # do some work ...

        async with db.transaction() as trans2:
            # do some work ...
            await trans2.rollback()

        # unnessesary, the transaction will be commited on exit from the
        # current context

        await trans1.commit()
```

## Bug tracker

If you have any suggestions, bug reports or annoyances please report them to
the issue tracker at https://github.com/klen/aio-databases/issues


## Contributing

Development of the project happens at: https://github.com/klen/aio-databases


## License

Licensed under a [MIT License](http://opensource.org/licenses/MIT)
