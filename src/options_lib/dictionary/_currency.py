"""CURRENCIES"""
from options_lib.dictionary.enum_code import EnumCode


class Currency(EnumCode):
    """Currencies"""
    RUB = 'RUB', 'rub'
    USD = 'USD', 'usd'
    EUR = 'EUR', 'eur'
    # Crypto
    USDT = 'USDT', 'usdt'
    USDC = 'USDC', 'usdc'
