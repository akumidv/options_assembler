"""Option types"""
import enum
from options_lib.entities.enum_code import EnumCode


@enum.unique
class OptionsType(EnumCode):
    """
    Usage code "c" instead of value "call" for filter dataframe reduce time for ~5%-30% and memory usage
    """
    CALL = "call", "c"
    PUT = "put", "p"


@enum.unique
class OptionsStyle(EnumCode):
    """
    Usage code "c" instead of value "call" for filter dataframe reduce time for ~5%-30% and memory usage
    """
    AMERICAN = "american", "a"
    EUROPEAN = "european", "e"


@enum.unique
class OptionsPriceStatus(EnumCode):
    """Option in at or out money status"""
    ATM = 'ATM', 'atm'
    ITM = 'ITM', 'itm'
    OTM = 'OTM', 'otm'
