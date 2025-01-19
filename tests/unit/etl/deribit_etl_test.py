import datetime

import pandas as pd
import pytest
from option_lib.entities import Timeframe
from option_lib.etl.deribit_etl import EtlDeribit
from option_lib.provider.exchange import DeribitExchange
from option_lib.etl.etl_class import AssetBookData


@pytest.fixture(name='etl_deribit')
def etl_deribit_fixture(deribit_client, data_path):
    """Fixture for Derebit ETL"""
    etl_deribit = EtlDeribit(deribit_client, None, Timeframe.EOD, data_path)
    return etl_deribit


def test_get_symbols_books_snapshot(etl_deribit):
    currency_symbol = DeribitExchange.CURRENCIES[0]
    request_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
    book_data = etl_deribit.get_symbols_books_snapshot(currency_symbol, request_datetime)
    assert isinstance(book_data, AssetBookData)
    assert book_data.asset_name == currency_symbol
    assert book_data.request_datetime == request_datetime
    assert isinstance(book_data.option, pd.DataFrame)
    assert isinstance(book_data.future, pd.DataFrame)
    assert isinstance(book_data.spot, pd.DataFrame)


def test___save_timeframe_book_update(etl_deribit):
    options_df = pd.DataFrame()
    future_df = pd.DataFrame()
    spot_df = pd.DataFrame()
    book_data = AssetBookData(asset_name='BTC', request_datetime=datetime.datetime.now(tz=datetime.timezone.utc),
                             option=options_df, future=future_df, spot=spot_df)
    etl_deribit._save_timeframe_book_update(book_data)
    # TODO
