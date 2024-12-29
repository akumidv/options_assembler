import enum
from option_lib.entities.enum_code import EnumCode


@enum.unique
class InstrumentType(EnumCode):
    """
    Usage code for database and parquet to reduce time for filtering
    """
    OPTION = 'option', 'o'
    FUTURE = 'future', "f"
    STOCK = 'stock', "s"
