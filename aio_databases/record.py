from __future__ import annotations

from collections.abc import ItemsView, Iterator, KeysView, Mapping, Sequence, ValuesView
from typing import Any, Union, cast


class Record(Mapping):
    __slots__ = "_values", "_keys"

    def __init__(self, values: tuple, description: Sequence[Sequence]):
        self._values = values
        self._keys = tuple(d[0] for d in description)

    @classmethod
    def from_dict(cls, val: dict) -> Record:
        keys, values = zip(*val.items())
        return cls(values, [[name] for name in keys])

    def keys(self):
        return cast(KeysView, self._keys)

    def values(self):
        return cast(ValuesView, self._values)

    def items(self):
        return cast(ItemsView, zip(self._keys, self._values))

    def __len__(self) -> int:
        return len(self._values)

    def __getitem__(self, idx: Union[str, int]) -> Any:
        if isinstance(idx, int):
            return self._values[idx]

        for key, value in self.items():
            if key == idx:
                return value

        raise KeyError

    def __contains__(self, idx) -> bool:
        return idx in self._keys

    def __iter__(self) -> Iterator:
        for val in self._values:
            yield val

    def __str__(self) -> str:
        return " ".join(f"{name}={value!r}" for name, value in self.items())

    def __repr__(self) -> str:
        return f"<Record {self}>"

    def __eq__(self, obj):
        return self._values == tuple(obj)
