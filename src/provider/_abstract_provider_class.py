"""
Abstract class with mandatory interfaces for every provider

Provider limits:
- provide data if it has it. For example if exchange provider do not support history it will not return option_history
- if local files do not contain chain it will not return data
- if request is wrong - for example there is not option for expiration_date it throw an error
It is not planned that provider level will be contained any data logic. It should be in option data class
"""

from typing import Any
from abc import ABC, abstractmethod
import datetime
import pandas as pd

from options_lib.entities import OptionsColumns, FuturesColumns, Timeframe, AssetKind
from provider._provider_entities import RequestParameters


class AbstractProvider(ABC):
    """Provider interfaces"""

    exchange_code: str
    options_columns: list = [
        OptionsColumns.TIMESTAMP.nm,
        OptionsColumns.STRIKE.nm,
        OptionsColumns.EXPIRATION_DATE.nm,
        OptionsColumns.OPTION_TYPE.nm,
        OptionsColumns.PRICE.nm,
        OptionsColumns.UNDERLYING_EXPIRATION_DATE.nm,
        OptionsColumns.UNDERLYING_PRICE.nm,
    ]
    futures_columns: list = [
        FuturesColumns.TIMESTAMP.nm,
        FuturesColumns.EXPIRATION_DATE.nm,
        FuturesColumns.PRICE.nm,
    ]

    def __init__(self, exchange_code: str, **kwargs: Any) -> None:
        self.exchange_code = exchange_code
        super().__init__(**kwargs)

    @abstractmethod
    def get_assets_list(self, asset_kind: AssetKind) -> list[str]:
        """List of symbols"""

    @abstractmethod
    def get_asset_history_years(
        self, asset_code: str, asset_kind: AssetKind, timeframe: Timeframe
    ) -> list[int]:
        """List of history years"""

    @abstractmethod
    def load_options_history(
        self,
        asset_code: str,
        params: RequestParameters,
        columns: list | None = None,
    ) -> pd.DataFrame:
        """Provide options by period, timeframe"""

    @abstractmethod
    def load_options_book(
        self,
        asset_code: str,
        settlement_datetime: datetime.datetime | None = None,
        timeframe: Timeframe = Timeframe.EOD,
        columns: list | None = None,
    ) -> pd.DataFrame:
        """Provide options for datetime, timeframe"""

    @abstractmethod
    def load_futures_history(
        self,
        asset_code: str,
        params: RequestParameters,
        columns: list | None = None,
    ) -> pd.DataFrame:
        """Provide future by period, timeframe"""

    @abstractmethod
    def load_futures_book(
        self,
        asset_code: str,
        settlement_datetime: datetime.datetime | None = None,
        timeframe: Timeframe = Timeframe.EOD,
        columns: list | None = None,
    ) -> pd.DataFrame:
        """Provide futures for datetime, timeframe"""

    @abstractmethod
    def load_options_chain(
        self,
        asset_code: str,
        settlement_datetime: datetime.datetime | None = None,
        expiration_date: datetime.datetime | None = None,
        timeframe: Timeframe = Timeframe.EOD,
        columns: list | None = None,
    ) -> pd.DataFrame | None:
        """Provide options chain by request to api if supported. Otherwise, return None"""
