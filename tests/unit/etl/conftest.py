import datetime

import pandas as pd
import pytest

from option_lib.entities import Timeframe, AssetKind
from exchange import DeribitExchange
from options_etl.etl_class import EtlOptions, AssetBookData, SaveTask



FUT_YEAR_SYMBOLS_CACHE = None



class TestEtl(EtlOptions):
    """Test ETL class"""

    def __init__(self, exchange: DeribitExchange, asset_names: list[str] | None, timeframe: Timeframe, data_path: str):
        super().__init__(exchange, asset_names, timeframe, data_path)

    def _save_tasks_dataframes_job(self):
        """Stop saving test tasks during tests"""

    def get_symbols_books_snapshot(self, asset_name: str, request_timestamp: pd.Timestamp) -> AssetBookData:
        return AssetBookData(asset_name=asset_name, request_timestamp=request_timestamp, option=None, future=None,
                             spot=None)


@pytest.fixture(name='etl_options')
def etl_options_fixture(deribit_client, data_path):
    """Fixture for Test ETL"""
    etl = TestEtl(deribit_client, None, Timeframe.EOD, data_path)
    return etl


@pytest.fixture(name='fut_year_symbols')
def year_symbols_fixture(etl_history):
    global FUT_YEAR_SYMBOLS_CACHE
    if FUT_YEAR_SYMBOLS_CACHE is None:
        FUT_YEAR_SYMBOLS_CACHE = etl_history._get_asset_history_years(AssetKind.FUTURE)
    return FUT_YEAR_SYMBOLS_CACHE
