from enum import Enum


class OptionType(str, Enum):
    CALL = "call", "c"
    PUT = "put", "p"

    def __new__(cls, value, code):
        obj = str.__new__(cls, [value])
        obj._value_ = value
        obj.code = code
        return obj


class AssetType(str, Enum):
    FUTURE = "future", 'f'
    SHARE = "share", 's'
    STOCK = "stock", 's'
    CRYPTO = "cryptocurrency", 'x'
    COMMODITIES = "commodities", 'g' # g goods
    CURRENCY = 'currency', 'm' # money
    INDEX = 'index', 'i'

    def __new__(cls, value, code):
        obj = str.__new__(cls, [value])
        obj._value_ = value
        obj.code = code
        return obj


OPTION_TYPE_MAP = {
    OptionType.CALL.code: OptionType.CALL,
    OptionType.PUT.code: OptionType.PUT,
}


ASSET_TYPE_MAP = {
    AssetType.FUTURE.code: AssetType.FUTURE,
    AssetType.SHARE.code: AssetType.SHARE,
    AssetType.CRYPTO.code: AssetType.CRYPTO,
    AssetType.COMMODITIES.code: AssetType.COMMODITIES,
    AssetType.INDEX.code: AssetType.INDEX,
}

