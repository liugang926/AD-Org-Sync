from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict


class MappingLikeModel:
    __slots__ = ()

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:
        setattr(self, key, value)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def items(self):
        return self.to_dict().items()

    def keys(self):
        return self.to_dict().keys()

    def values(self):
        return self.to_dict().values()

    def __contains__(self, key: str) -> bool:
        return hasattr(self, key)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
