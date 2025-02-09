"""Enum code class realisation"""
import enum
from typing import Type



# class EnumCode_prev(str, enum.Enum):
class EnumCode(enum.Enum):
    """Add code to enum"""

    def __new__(cls, value: str, code: str | None = None):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.code = code
        return obj

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value


# class EnumColumnType(str, enum.Enum):
class EnumDataFrameColumn(enum.Enum):
    """Add code and type to enum"""

    def __new__(cls, value: str, pd_type: Type, resample_func: str | None = None):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.nm = value
        obj.type = pd_type
        obj.resample_func = resample_func
        return obj

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value


# class EnumMultiplier(str, enum.Enum):
class EnumMultiplier(enum.Enum):
    """Add multiplier value to to enum"""

    def __new__(cls, value: str, mult: int | float | None = None):
        # obj = str.__new__(cls, [value])
        obj = object.__new__(cls)
        obj._value_ = value
        obj.mult = mult
        obj.nm = value
        return obj

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value
