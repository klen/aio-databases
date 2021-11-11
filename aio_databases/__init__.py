"""Async support for various databases."""

from __future__ import annotations

import logging


__version__ = '0.13.0'


logger: logging.Logger = logging.getLogger('aio-databases')
logger.addHandler(logging.NullHandler())


from .database import Database, current_conn

assert Database
assert current_conn
