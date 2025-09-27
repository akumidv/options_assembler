"""
Local provider
Data should be organized like EXCHANGE_CODE/EXCHANGE_SYMBOL/TRADE_TYPE/TIMEFRAME_CODE/YEAR.parquet
Example: LME/WTI/option/EOD/2024.year
TRADE_TYPE can be: option, future, asset (asset - mean tangible assets: currency, stock, crypto)

dataframe columns:
"""

import builtins
import datetime
import pandas as pd
from pydantic import validate_call
from options_lib.dictionary import Timeframe, AssetKind
from provider._file_provider import AbstractFileProvider
from provider._provider_entities import RequestParameters


class PandasLocalFileProvider(AbstractFileProvider):
    """Load data from files by Pandas"""

    def _fn_path_prepare(
        self, asset_code: str, asset_kind: AssetKind, timeframe: Timeframe, year: int
    ):
        return super().fn_path_prepare(asset_code, asset_kind, timeframe, year)

    def _load_data_for_period(
        self,
        asset_kind: AssetKind,
        asset_code: str,
        params: RequestParameters,
        columns: list,
    ) -> pd.DataFrame:
        if params.period_from is None and params.period_to is None:
            raise NotImplementedError("Load all data")
        if params.period_from is None:
            match type(params.period_to):
                case builtins.int:
                    fn_path = self._fn_path_prepare(
                        asset_code, asset_kind, params.timeframe, params.period_to
                    )
                    return pd.read_parquet(fn_path, columns=columns)
                case datetime.date:
                    fn_path = self._fn_path_prepare(
                        asset_code, asset_kind, params.timeframe, params.period_to.year
                    )
                    df_hist = pd.read_parquet(fn_path, columns=columns)
                    return df_hist[df_hist["datetime"] == params.period_to].reset_index(
                        drop=True
                    )
                case datetime.datetime:
                    raise NotImplementedError("to period type datetime.datetime")
                case _:
                    raise TypeError(
                        f"period_to have incorrect type {type(params.period_to)}"
                    )
        else:
            match type(params.period_from):
                case builtins.int:
                    if params.period_to is None:
                        raise NotImplementedError("load from period_from to last year")
                    if isinstance(params.period_to, int):
                        raise NotImplementedError("Load from year to year")
                    raise TypeError(
                        f"Mismatch types period_from {type(params.period_from)} "
                        f"and period_to {type(params.period_to)}"
                    )
                case datetime.date:
                    if params.period_to is None:
                        raise NotImplementedError("load from period_from to last year")
                    if isinstance(params.period_to, datetime.date):
                        raise NotImplementedError("Load from date to date")
                    raise TypeError(
                        f"Mismatch types period_from {type(params.period_from)} "
                        f"and period_to {type(params.period_to)}"
                    )
                case datetime.datetime:
                    raise NotImplementedError("period from datatime")
                case _:
                    raise TypeError(
                        f"period_from have incorrect type {type(params.period_from)}"
                    )

    @validate_call
    def load_options_history(
        self,
        asset_code: str,
        params: RequestParameters | None = None,
        columns: list | None = None,
    ) -> pd.DataFrame:
        """Load option by period, timeframe"""
        if params is None:
            params = RequestParameters()
        if columns is None:
            columns = self.options_columns
        df_hist = self._load_data_for_period(
            asset_kind=AssetKind.OPTIONS,
            asset_code=asset_code,
            params=params,
            columns=columns,
        )
        return df_hist

    def load_options_book(
        self,
        asset_code: str,
        settlement_datetime: datetime.datetime | None = None,
        timeframe: Timeframe = Timeframe.EOD,
        columns: list | None = None,
    ) -> pd.DataFrame:
        """Provide options for datetime, timeframe"""
        raise NotImplementedError

    @validate_call
    def load_futures_history(
        self,
        asset_code: str,
        params: RequestParameters | None = None,
        columns: list | None = None,
    ) -> pd.DataFrame:
        """Load futures data for asset code"""
        if params is None:
            params = RequestParameters()
        if columns is None:
            columns = self.futures_columns
        df_fut = self._load_data_for_period(
            asset_kind=AssetKind.FUTURES,
            asset_code=asset_code,
            params=params,
            columns=columns,
        )
        return df_fut

    @validate_call
    def load_futures_book(
        self,
        asset_code: str,
        settlement_datetime: datetime.datetime | None = None,
        timeframe: Timeframe = Timeframe.EOD,
        columns: list | None = None,
    ) -> pd.DataFrame:
        """Provide futures for datetime, timeframe"""
        raise NotImplementedError

    @validate_call
    def load_options_chain(
        self,
        asset_code: str,
        settlement_datetime: datetime.datetime | None = None,
        expiration_date: datetime.datetime | None = None,
        timeframe: Timeframe = Timeframe.EOD,
        columns: list | None = None,
    ) -> pd.DataFrame | None:
        """Provide options chain by request to api if supported. Otherwise, return None"""
        raise NotImplementedError