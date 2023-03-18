from collections.abc import Awaitable, Callable, Mapping
from typing import Any, TypeVar

TRecord = Mapping[str, Any]
TInitConnection = Callable[[Any], Awaitable[Any]]
TVConnection = TypeVar("TVConnection", bound=Any)
