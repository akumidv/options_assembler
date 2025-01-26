"""Update history timeframes from timeframe updates"""
from option_lib.provider import PandasLocalFileProvider
from option_lib.entities import Timeframe


# Метод минимального таймфрейма из которого готовятся все остальные более старшие таймфреймы, а лучше данные
# всех нижеследующих таймфреймов

class EtlHistory:
    def __init__(self, exchange_code: str, data_path: str, timeframe: Timeframe, full_history: bool = False):
        self._exchange_code = data_path
        self._exchange_data_path = data_path
        self._timeframe = timeframe
        self._full_history = full_history # Is force to process full history or only update from last stored for timeframe date?
        self.provider = PandasLocalFileProvider(exchange_code, data_path)

    def get_symbols(self):
        """Get processed symbols"""

    def get_update_years(self):
        """Get years of updates"""

    def get_timeframes(self):
        """Get timeframes"""

    def get_filelist(self):
        """List of files for timeframe by year"""

    def prepare(self):
        """Load history dataframe and load list of increments and update by them
        - may be additional class or function"""
