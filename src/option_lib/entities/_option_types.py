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
class OptionMoneyStatus(EnumCode):
    ATM = 'ATM', 'atm'
    ITM = 'ITM', 'itm'
    OTM = 'OTM', 'otm'
