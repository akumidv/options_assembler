"""Core (domain-neutral) dictionary: the single data-term registry (``Term``, R4.3 — one
canonical name per concept, used as column / variable / parameter) and the neutral
classification-axis enums (R4.5)."""

from alphavar.core.dictionary._classification import AssetClass, InstrumentKind
from alphavar.core.dictionary._registry import assert_unique, column_names
from alphavar.core.dictionary._result_terms import ResultTerm
from alphavar.core.dictionary._terms import Term

__all__ = [
    "Term",
    "ResultTerm",
    "column_names",
    "assert_unique",
    "InstrumentKind",
    "AssetClass",
]
