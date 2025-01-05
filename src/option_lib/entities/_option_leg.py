"""Option leg data class"""
import enum
from pydantic import BaseModel
from option_lib.entities.enum_code import EnumCode

from option_lib.entities._asset_types import AssetType
from option_lib.entities._option_types import OptionType


@enum.unique
class LegType(EnumCode):
    """
    Usage code for legs
    """
    OPTION_CALL = OptionType.CALL.value, OptionType.CALL.code
    OPTION_PUT = OptionType.PUT.value, OptionType.PUT.code
    FUTURE = AssetType.FUTURE.value, AssetType.FUTURE.code


class OptionLeg(BaseModel):
    """Option leg structure"""
    strike: float
    lots: int
    type: LegType
