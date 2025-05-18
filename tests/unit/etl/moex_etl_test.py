import datetime

import pandas as pd
import pytest
from option_lib.entities import Timeframe, OptionColumns as OCl, FutureColumns as FCl, SpotColumns as SCl
from options_etl.etl_class import AssetBookData
from options_etl.moex_etl import EtlMoex


class TestEtlMoex(EtlMoex):
    def _save_tasks_dataframes_job(self):
        """Stop saving test tasks during tests"""

@pytest.fixture(name='etl_moex')
def etl_moex_fixture(moex_exchange, data_path):
    """Fixture for Moex ETL"""
    etl_moex = TestEtlMoex(moex_exchange, None, Timeframe.EOD, data_path)
    return etl_moex


def test_moex_get_symbols_books_snapshot(etl_moex, moex_asset_code):
    request_timestamp = pd.Timestamp.now(tz=datetime.UTC)
    book_data = etl_moex.get_symbols_books_snapshot(moex_asset_code, request_timestamp)
    assert isinstance(book_data, AssetBookData)
    assert book_data.asset_name == moex_asset_code
    assert book_data.request_timestamp == request_timestamp
    assert isinstance(book_data.option, pd.DataFrame)


def test__save_timeframe_book_update(etl_moex):
    options_df = pd.DataFrame(
        {f'{OCl.BASE_CODE.nm}': ['SI', 'SI', 'YDEX', 'YDEX'], f'{OCl.PRICE.nm}': [10, 10, 50, 50]})
    future_df = pd.DataFrame({f'{OCl.BASE_CODE.nm}': ['SI', 'SI'], f'{FCl.PRICE.nm}': [80, 80]})
    spot_df = pd.DataFrame({f'{OCl.BASE_CODE.nm}': ['YDEX', 'YDEX'], f'{OCl.PRICE.nm}': [4000, 4000]})
    saved_tasks = len(etl_moex._save_tasks)
    book_data = AssetBookData(asset_name='BTC', request_timestamp=pd.Timestamp.now(tz=datetime.UTC),
                                     option=options_df, future=future_df, spot=spot_df)
    etl_moex._save_timeframe_book_update(book_data)  # pylint: disable=protected-access
    assert len(etl_moex._save_tasks) == saved_tasks + len(options_df[OCl.BASE_CODE.nm].unique()) + \
        len(future_df[OCl.BASE_CODE.nm].unique()) + len(spot_df[OCl.BASE_CODE.nm].unique())
    etl_moex._save_tasks = []
