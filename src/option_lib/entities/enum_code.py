"""Enum code class realisation"""
import enum


class EnumCode(str, enum.Enum):
    """Add code value to enum"""
    def __new__(cls, value, code):
        obj = str.__new__(cls, [value])
        obj._value_ = value
        obj.code = code
        return obj

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value
