from __future__ import annotations

import typing as t

from collections.abc import Mapping


class Record(Mapping):

    __slots__ = '_values', '_keys'

    def __init__(self, values: t.Tuple, description: t.Sequence[t.Sequence]):
        self._values = values
        self._keys = tuple(d[0] for d in description)

    def __len__(self) -> int:
        return len(self._values)

    def __getitem__(self, idx: t.Union[str, int]) -> t.Any:
        if isinstance(idx, int):
            return self._values[idx]

        for key, value in self.items():
            if key == idx:
                return value

        raise KeyError

    def __contains__(self, idx) -> bool:
        return idx in self._keys

    def __iter__(self) -> t.Iterator:
        for val in self._values:
            yield val

    def __str__(self) -> str:
        return ' '.join(f"{name}={value!r}" for name, value in self.items())
        return f"{dict(self)}"

    def __repr__(self) -> str:
        return f"<Record {self}>"

    @classmethod
    def from_dict(cls, val: t.Dict) -> Record:
        keys, values = zip(*val.items())
        return cls(values, [[name] for name in keys])

    def values(self) -> t.Tuple:  # type: ignore
        return self._values

    def keys(self) -> t.Tuple:  # type: ignore
        return self._keys

    def items(self) -> zip:  # type: ignore
        return zip(self._keys, self._values)

    def __eq__(self, obj: t.Any):
        return self._values == tuple(obj)
