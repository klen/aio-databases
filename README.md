# AIO-Databases

The package gives you async support for a range of databases (SQLite,
PostgreSQL, MySQL).

[![Tests Status](https://github.com/klen/aio-databases/workflows/tests/badge.svg)](https://github.com/klen/aio-databases/actions)
[![PYPI Version](https://img.shields.io/pypi/v/aio-databases)](https://pypi.org/project/aio-databases/)
[![Python Versions](https://img.shields.io/pypi/pyversions/aio-databases)](https://pypi.org/project/aio-databases/)

## Features

* Has no dependencies (except databases drivers)
* Supports [asyncio](https://docs.python.org/3/library/asyncio.html) and [trio](https://github.com/python-trio/trio)
* Supports [aiosqlite](https://github.com/omnilib/aiosqlite),
  [aiomysql](https://github.com/aio-libs/aiomysql),
  [aiopg](https://github.com/aio-libs/aiopg),
  [asyncpg](https://github.com/MagicStack/asyncpg),
  [triopg](https://github.com/python-trio/triopg),
  [trio_mysql](https://github.com/python-trio/trio-mysql)
* Manage pools of connections
* Manage transactions

## Requirements

* python >= 3.9

## Installation

**aio-databases** should be installed using pip:

```shell
$ pip install aio-databases
```

You have to choose and install the required database drivers with:

```shell
# To support SQLite
$ pip install aio-databases[aiosqlite]  # asyncio

# To support MySQL
$ pip install aio-databases[aiomysql]   # asyncio
$ pip install aio-databases[trio_mysql] # trio

# To support PostgreSQL (choose one)
$ pip install aio-databases[aiopg]      # asyncio
$ pip install aio-databases[asyncpg]    # asyncio
$ pip install aio-databases[triopg]     # trio

# To support ODBC (alpha state)
$ pip install aio-databases[aioodbc]    # asyncio
```


## Usage

### Init a database

```python
    from aio_databases import Database

    # Initialize a database
    db = Database('sqlite:///:memory:')  # with default driver

    # Flesh out the driver
    db = Database('asyncpg+pool://test:test@localhost:5432/tests', maxsize=10)
```

### Supported schemas

- `aiomyql`
- `aiomyql+pool`
- `aiopg`
- `aiopg+pool`
- `asyncpg`
- `asyncpg+pool`
- `aioodbc`
- `aioodbc+pool`
- `aiosqlite`
- `trio-mysql`
- `triopg`

### Setup a pool of connections (optional)

Setup a pool of connections

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

### Get a connection

```python
    # Acquire and release (on exit) a connection
    async with db.connection():
        await my_code()

    # Acquire a connection only if it not exist
    async with db.connection(False):
        await my_code()
```

If a pool is setup it will be used

### Run SQL queries

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

* Iterate through rows one by one

```python

    async for rec in db.iterate('select name from users'):
        print(rec)

```

### Manage connections

By default the database opens and closes a connection for a query.

```python
    # Connection will be acquired and released for the query
    await db.fetchone('select %s', 42)

    # Connection will be acquired and released again
    await db.fetchone('select %s', 77)
```

Manually open and close a connection

```python

    # Acquire a new connection object
    async with db.connection():
        # Only one connection will be used
        await db.fetchone('select %s', 42)
        await db.fetchone('select %s', 77)
        # ...

    # Acquire a new connection or use an existing
    async with db.connection(False):
        # ...
```

If there any connection already `db.method` would be using the current one
```python
    async with db.connection(): # connection would be acquired here
        await db.fetchone('select %s', 42)  # the connection is used
        await db.fetchone('select %s', 77)  # the connection is used

    # the connection released there
```

### Manage transactions

```python
    # Start a tranction using the current connection
    async with db.transaction() as trans1:
        # do some work ...

        async with db.transaction() as trans2:
            # do some work ...
            await trans2.rollback()

        # unnessesary, the transaction will be commited on exit from the
        # current context

        await trans1.commit()

    # Create a new connection and start a transaction
    async with db.tranction(True) as trans:
        # do some work ...
```

## Bug tracker

If you have any suggestions, bug reports or annoyances please report them to
the issue tracker at https://github.com/klen/aio-databases/issues


## Contributing

Development of the project happens at: https://github.com/klen/aio-databases


## License

Licensed under a [MIT License](http://opensource.org/licenses/MIT)
