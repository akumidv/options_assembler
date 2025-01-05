"""Option types"""
import enum
from option_lib.entities.enum_code import EnumCode


@enum.unique
class OptionType(EnumCode):
    """
    Usage code "c" instead of value "call" for filter dataframe reduce time for ~5%-30% and memory usage
    """
    CALL = "call", "c"
    PUT = "put", "p"


@enum.unique
class OptionPriceStatus(EnumCode):
    """Option in at or out money status"""
    ATM = 'ATM', 'atm'
    ITM = 'ITM', 'itm'
    OTM = 'OTM', 'otm'
