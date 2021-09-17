"""Async support for various databases."""

from __future__ import annotations

import logging


__version__ = '0.9.0'


logger: logging.Logger = logging.getLogger('aio-databases')
logger.addHandler(logging.NullHandler())


from .database import Database  # noqa
