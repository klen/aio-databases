"""Setup the package."""


# Parse requirements
# ------------------
import pkg_resources
import pathlib


def parse_requirements(path: str) -> 'list[str]':
    with pathlib.Path(path).open() as requirements:
        return [str(req) for req in pkg_resources.parse_requirements(requirements)]


# Setup package
# -------------

from setuptools import setup

DRIVERS = ['asyncpg', 'aiopg', 'aiomysql', 'aiosqlite', 'aioodbc', 'triopg', 'trio_mysql']


setup(
    extras_require=dict(
        {driver: [driver] for driver in DRIVERS},
        tests=parse_requirements('requirements/requirements-tests.txt'),
    )
)

# pylama:ignore=E402,D
