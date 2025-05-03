"""
Local provider
Data should be organized like EXCHANGE_CODE/EXCHANGE_SYMBOL/TRADE_TYPE/TIMEFRAME_CODE/YEAR.parquet
Example: LME/WTI/option/EOD/2024.year
TRADE_TYPE can be: option, future, asset (asset - mean tangible assets: currency, stock, crypto)

dataframe columns:
"""

import os
import builtins
import datetime
import pandas as pd
from pydantic import validate_call
from option_lib.entities import Timeframe, AssetKind
from options_assembler.provider._file_provider import FileProvider
from options_assembler.provider._provider_entities import RequestParameters


class PandasLocalFileProvider(FileProvider):
    """Load data from files by Pandas"""

    def __init__(self, exchange_code: str, data_path: str):
        exchange_data_path = os.path.normpath(os.path.abspath(os.path.join(data_path, exchange_code)))
        if not os.path.isdir(exchange_data_path):
            raise FileNotFoundError(f'Folder {exchange_data_path} is not exist')
        self.exchange_data_path = exchange_data_path
        super().__init__(exchange_code=exchange_code, data_path=data_path)

    def _fn_path_prepare(self, symbol: str, asset_kind: AssetKind, timeframe: Timeframe, year: int):
        return super().fn_path_prepare(symbol, asset_kind, timeframe, year)


    def _load_data_for_period(self, asset_kind: AssetKind, symbol: str,
                              params: RequestParameters, columns: list) -> pd.DataFrame:
        if params.period_from is None:
            match type(params.period_to):
                case None:
                    raise NotImplementedError('Load all data')
                case builtins.int:
                    fn_path = self._fn_path_prepare(symbol, asset_kind, params.timeframe, params.period_to)
                    return pd.read_parquet(fn_path, columns=columns)
                case datetime.date:
                    fn_path = self._fn_path_prepare(symbol, asset_kind, params.timeframe, params.period_to.year)
                    df_hist = pd.read_parquet(fn_path, columns=columns)
                    return df_hist[df_hist['datetime'] == params.period_to].reset_index(drop=True)
                case datetime.datetime:
                    raise NotImplementedError('to period type datetime.datetime')
                case _:
                    raise TypeError(f'period_to have incorrect type {type(params.period_to)}')
        else:
            match type(params.period_from):
                case builtins.int:
                    if params.period_to is None:
                        raise NotImplementedError('load from period_from to last year')
                    if isinstance(params.period_to, int):
                        raise NotImplementedError('Load from year to year')
                    raise TypeError(f'Mismatch types period_from {type(params.period_from)} '
                                    f'and period_to {type(params.period_to)}')
                case datetime.date:
                    if params.period_to is None:
                        raise NotImplementedError('load from period_from to last year')
                    if isinstance(params.period_to, datetime.date):
                        raise NotImplementedError('Load from date to date')
                    raise TypeError(f'Mismatch types period_from {type(params.period_from)} '
                                    f'and period_to {type(params.period_to)}')
                case datetime.datetime:
                    raise NotImplementedError('period from datatime')
                case _:
                    raise TypeError(f'period_from have incorrect type {type(params.period_from)}')

    @validate_call
    def load_option_history(self, symbol: str, params: RequestParameters | None = None,
                            columns: list | None = None) -> pd.DataFrame:
        """Load option by period, timeframe"""
        if params is None:
            params = RequestParameters()
        if columns is None:
            columns = self.option_columns
        df_hist = self._load_data_for_period(asset_kind=AssetKind.OPTION, symbol=symbol,
                                             params=params, columns=columns)
        return df_hist

    @validate_call
    def load_future_history(self, symbol: str, params: RequestParameters | None = None,
                            columns: list | None = None) -> pd.DataFrame:
        if params is None:
            params = RequestParameters()
        if columns is None:
            columns = self.future_columns
        df_fut = self._load_data_for_period(asset_kind=AssetKind.FUTURES, symbol=symbol,
                                            params=params, columns=columns)
        return df_fut
