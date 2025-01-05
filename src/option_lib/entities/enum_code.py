"""Enum code class realisation"""
import enum
from typing import Type


class EnumCode(str, enum.Enum):
    """Add code to enum"""

    def __new__(cls, value: str, code: str | None = None):
        obj = str.__new__(cls, [value])
        obj._value_ = value
        obj.code = code
        return obj

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value


class EnumColumnType(str, enum.Enum):
    """Add code and type to enum"""

    def __new__(cls, value: str, pd_type: Type | None = None):
        obj = str.__new__(cls, [value])
        obj._value_ = value
        obj.type = pd_type
        obj.nm = value
        return obj

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value
