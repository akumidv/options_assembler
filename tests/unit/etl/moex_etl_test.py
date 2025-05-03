import datetime

import pandas as pd
import pytest
from option_lib.entities import Timeframe, OptionColumns as OCl

from exchange import DeribitExchange
from options_etl.moex_etl import EtlMoex, MoexAssetBookData


class TestEtlMoex(EtlMoex):
    def _save_tasks_dataframes_job(self):
        """Stop saving test tasks during tests"""


@pytest.fixture(name='etl_moex')
def etl_deribit_fixture(moex_client, data_path):
    """Fixture for Deribit ETL"""
    etl_deribit = TestEtlMoex(moex_client, None, Timeframe.EOD, data_path)
    return etl_deribit


def test_get_symbols_books_snapshot(etl_moex):
    currency_symbol = DeribitExchange.CURRENCIES[0]
    request_timestamp = pd.Timestamp.now(tz=datetime.UTC)
    book_data = etl_moex.get_symbols_books_snapshot(currency_symbol, request_timestamp)
    assert isinstance(book_data, MoexAssetBookData)
    assert book_data.asset_name == currency_symbol
    assert book_data.request_timestamp == request_timestamp
    assert isinstance(book_data.option, pd.DataFrame)
    assert isinstance(book_data.future, pd.DataFrame)
    assert isinstance(book_data.spot, pd.DataFrame)
    assert isinstance(book_data.future_combo, pd.DataFrame)
    assert isinstance(book_data.option_combo, pd.DataFrame)


def test__save_timeframe_book_update(etl_moex):
    options_df = pd.DataFrame(
        {f'{OCl.BASE_CODE.nm}': ['BTC-USD', 'BTC-USD', 'ETH-USD', 'ETH-USD'], f'{OCl.PRICE.nm}': [100, 100, 50, 50]})
    future_df = pd.DataFrame({f'{OCl.BASE_CODE.nm}': ['BTC', 'BTC', 'ETH', 'ETH'], f'{OCl.PRICE.nm}': [100, 100, 50, 50]})
    spot_df = pd.DataFrame({f'{OCl.BASE_CODE.nm}': ['BTC', 'BTC', 'ETH', 'ETH'], f'{OCl.PRICE.nm}': [100, 100, 50, 50]})
    futures_combo_df = pd.DataFrame(
        {f'{OCl.BASE_CODE.nm}': ['BTC', 'BTC', 'ETH', 'ETH'], f'{OCl.PRICE.nm}': [100, 100, 50, 50]})
    option_combo_df = pd.DataFrame(
        {f'{OCl.BASE_CODE.nm}': ['BTC', 'BTC', 'ETH', 'ETH'], f'{OCl.PRICE.nm}': [100, 100, 50, 50]})
    saved_tasks = len(etl_moex._save_tasks)
    book_data = MoexAssetBookData(asset_name='BTC', request_timestamp=pd.Timestamp.now(tz=datetime.UTC),
                                     option=options_df, future=future_df, spot=spot_df, future_combo=futures_combo_df,
                                     option_combo=option_combo_df)
    etl_moex._save_timeframe_book_update(book_data)  # pylint: disable=protected-access
    assert len(etl_moex._save_tasks) == 10 + saved_tasks
    etl_moex._save_tasks = []
