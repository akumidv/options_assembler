"""Update history timeframes from timeframe updates"""
from option_lib.provider import FileProvider

# TODO Выбор таймфрейма в инициализации минимального
# Метод минимального таймфрейма из которого готовятся все остальные более старшие таймфреймы

def prepare_options_from_increments():
    """Load history dataframe and load list of increments and update by them
    - may be additional class or function"""

    # self._store_timeframes: set = {timeframe} if store_timeframes is None else {store_timeframes + [timeframe]}
