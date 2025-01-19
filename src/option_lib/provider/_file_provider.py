"""
Local provider
Data should be organized like EXCHANGE_CODE/EXCHANGE_SYMBOL/TRADE_TYPE/TIMEFRAME_CODE/YEAR.parquet
Example: LME/WTI/option/EOD/2024.year
TRADE_TYPE can be: option, future, asset (asset - mean tangible assets: currency, stock, crypto)

dataframe columns:
"""

import os
import datetime
from abc import ABC
import pandas as pd
from pydantic import validate_call
from option_lib.entities import Timeframe, AssetKind
from option_lib.provider._abstract_provider_class import AbstractProvider


class FileProvider(AbstractProvider, ABC):
    """Load data from files"""

    def __init__(self, exchange_code: str, data_path: str):
        exchange_data_path = os.path.normpath(os.path.abspath(os.path.join(data_path, exchange_code)))
        if not os.path.isdir(exchange_data_path):
            raise FileNotFoundError(f'Folder {exchange_data_path} is not exist')
        self.exchange_data_path = exchange_data_path
        super().__init__(exchange_code=exchange_code)

    def get_symbols_list(self, asset_kind: AssetKind):
        """Prepare list of underlying assets symbols"""
        symbols = []
        for symbol in os.listdir(self.exchange_data_path):
            asset_kinds = os.listdir(os.path.join(self.exchange_data_path, symbol))
            if asset_kind.value in asset_kinds:
                symbols.append(symbol)
        return symbols

    def fn_path_prepare(self, symbol: str, asset_kind: AssetKind, timeframe: Timeframe, year: int):
        """Prepare path for files"""
        return f'{self.exchange_data_path}/{symbol}/{asset_kind.value}/{timeframe.value}/{year}.parquet'

    @validate_call
    def load_option_chain(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                          expiration_date: datetime.datetime | None = None,
                          timeframe: Timeframe = Timeframe.EOD,
                          columns: list | None = None) -> pd.DataFrame | None:
        """Providing option chain by local file system is not supported return None"""
        return None

    def load_option_book(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                         timeframe: Timeframe = Timeframe.EOD, columns: list | None = None) -> pd.DataFrame:
        """Providing option book by local file system is not supported return None"""
        return None

    def load_future_book(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                         timeframe: Timeframe = Timeframe.EOD, columns: list | None = None) -> pd.DataFrame:
        """Providing option book by local file system is not supported return None"""
        return None
