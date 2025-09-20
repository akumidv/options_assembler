"""Option leg data class"""
import enum
from pydantic import BaseModel
from options_lib.entities.enum_code import EnumCode

from options_lib.entities._asset_types import AssetKind
from options_lib.entities._option_types import OptionsType


@enum.unique
class LegType(EnumCode):
    """
    Usage code for legs
    """
    OPTIONS_CALL = OptionsType.CALL.value, OptionsType.CALL.code
    OPTIONS_PUT = OptionsType.PUT.value, OptionsType.PUT.code
    FUTURES = AssetKind.FUTURES.value, AssetKind.FUTURES.code


class OptionsLeg(BaseModel):
    """Option leg structure"""
    strike: float
    lots: int
    type: LegType
