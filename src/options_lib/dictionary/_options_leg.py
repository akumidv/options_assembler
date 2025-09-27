"""Option leg data class"""
import enum
from options_lib.dictionary.enum_code import EnumCode

from options_lib.dictionary._asset_types import AssetKind
from options_lib.dictionary._options_types import OptionsType


@enum.unique
class LegType(EnumCode):
    """
    Usage code for legs
    """
    OPTIONS_CALL = OptionsType.CALL.value, OptionsType.CALL.code
    OPTIONS_PUT = OptionsType.PUT.value, OptionsType.PUT.code
    FUTURES = AssetKind.FUTURES.value, AssetKind.FUTURES.code
