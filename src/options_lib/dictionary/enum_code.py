"""Enum code class realisation"""
import enum
from typing import Self, Type


class EnumCode(enum.Enum):
    """Add code to enum"""

    def __new__(cls, value: str, code: str | None = None) -> Self:
        obj = object.__new__(cls)
        obj._value_: str = value
        obj.code: str | None = code
        return obj

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return self.value


class EnumDataFrameColumn(enum.Enum):
    """Add code and type to enum"""

    def __new__(cls, value: str, pd_type: Type | None = None, resample_func: str | None = None) -> Self:
        obj = object.__new__(cls)
        obj._value_: str = value
        obj.nm: str = value
        obj.type: Type = pd_type
        obj.resample_func: str | None = resample_func
        return obj

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return self.value


class EnumMultiplier(enum.Enum):
    """Add multiplier value to to enum"""

    def __new__(cls, value: str, mult: int | float | None = None) -> Self:
        obj = object.__new__(cls)
        obj._value_: str = value
        obj.mult: int | float | None = mult
        obj.nm: str = value
        return obj

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return self.value
