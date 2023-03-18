import logging

logger: logging.Logger = logging.getLogger("aio-databases")
logger.addHandler(logging.NullHandler())
