"""CURRENCIES"""
import enum
from option_lib.entities.enum_code import EnumCode



# @enum.unique
class Currency(EnumCode):
    RUB = 'RUB', 'rub'
    USD = 'USD', 'usd'
    EUR = 'EUR', 'eur'
    # Crypto
    USDT = 'USDT', 'usdt'
    USDC = 'USDC', 'usdc'
