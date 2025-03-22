"""Option leg data class"""
import enum
from pydantic import BaseModel
from options_assembler.entities.enum_code import EnumCode

from options_assembler.entities._asset_types import AssetKind
from options_assembler.entities._option_types import OptionType


@enum.unique
class LegType(EnumCode):
    """
    Usage code for legs
    """
    OPTION_CALL = OptionType.CALL.value, OptionType.CALL.code
    OPTION_PUT = OptionType.PUT.value, OptionType.PUT.code
    FUTURE = AssetKind.FUTURE.value, AssetKind.FUTURE.code


class OptionLeg(BaseModel):
    """Option leg structure"""
    strike: float
    lots: int
    type: LegType
