from typing import Any, Awaitable, Callable, Mapping, TypeVar

TRecord = Mapping[str, Any]
TInitConnection = Callable[[Any], Awaitable[Any]]
TVConnection = TypeVar("TVConnection", bound=Any)
