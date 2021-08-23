from __future__ import annotations

import typing as t

from collections.abc import Mapping


class Record(Mapping):

    __slots__ = '_row', '_map'

    def __init__(self, row: t.Tuple, description: t.Sequence[t.Sequence]):
        self._row = row
        self._map = dict((name, val) for (name, *_), val in zip(description, row))

    def __len__(self) -> int:
        return len(self._row)

    def __getitem__(self, idx: t.Union[str, int]) -> t.Any:
        if isinstance(idx, int):
            return self._row[idx]

        return self._map[idx]

    def __contains__(self, idx) -> bool:
        return idx in self._map

    def __iter__(self) -> t.Iterator:
        for val in self._row:
            yield val

    def __str__(self) -> str:
        return ' '.join(f"{name}={value!r}" for name, value in self.items())
        return f"{dict(self)}"

    def __repr__(self) -> str:
        return f"<Record {self}>"

    @classmethod
    def from_dict(cls, val: t.Dict) -> Record:
        names, row = zip(*val.items())
        return cls(row, [[name] for name in names])

    def values(self) -> t.Tuple:  # type: ignore
        return self._row

    def keys(self) -> t.Tuple:  # type: ignore
        return tuple(self._map.keys())

    def items(self) -> t.Tuple:  # type: ignore
        return tuple(zip(self._map.keys(), self._row))

    def __eq__(self, obj: t.Any):
        return self._row == tuple(obj)
