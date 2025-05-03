import datetime
from functools import lru_cache
import pandas as pd
import pytest
from option_lib.entities import Timeframe, AssetKind
from option_lib.entities import OptionColumns as OCl
from options_etl.etl_updates_to_history import EtlHistory
from exchange.exchange_entities import ExchangeCode


START_TS = None

FUT_YEAR_SYMBOLS_CACHE = None
@pytest.fixture(name='etl_history')
@lru_cache(maxsize=2)
def etl_history_fixture(data_path, update_path):
    etl_history = EtlHistory(exchange_code=ExchangeCode.DERIBIT, history_path=data_path, update_path=update_path,
                             timeframe=Timeframe.EOD)
    return etl_history


@pytest.fixture(name='etl_history_future')
@lru_cache(maxsize=2)
def etl_btc_future_history_fixture(data_path, option_symbol, update_path):
    etl_history = EtlHistory(exchange_code=ExchangeCode.DERIBIT, history_path=data_path, update_path=update_path,
                             timeframe=Timeframe.EOD, symbols=[option_symbol], asset_kinds=[AssetKind.FUTURES])
    return etl_history

@pytest.fixture(name='etl_history_option')
@lru_cache(maxsize=2)
def etl_btc_option_history_fixture(data_path, option_symbol, update_path):
    etl_history = EtlHistory(exchange_code=ExchangeCode.DERIBIT, history_path=data_path, update_path=update_path,
                             timeframe=Timeframe.EOD, symbols=[option_symbol], asset_kinds=[AssetKind.OPTION])
    return etl_history

@pytest.fixture(name='etl_history_spot')
@lru_cache(maxsize=2)
def etl_spot_history_fixture(data_path, option_symbol, update_path):
    etl_history = EtlHistory(exchange_code=ExchangeCode.DERIBIT, history_path=data_path, update_path=update_path,
                             timeframe=Timeframe.EOD, symbols=None, asset_kinds=[AssetKind.SPOT])
    return etl_history



@pytest.fixture(name='etl_start_ts')
def year_symbols_fixture(etl_history, fut_year_symbols):
    global START_TS
    if START_TS is None:
        START_TS = etl_history._get_start_timestamp(fut_year_symbols, AssetKind.FUTURES)
    return START_TS


def test__get_asset_history_years(etl_history):
    year_symbols = etl_history._get_asset_history_years(AssetKind.FUTURES)
    assert isinstance(year_symbols, dict)
    if year_symbols:
        years = list(year_symbols.keys())
        assert isinstance(years[0], int)
        assert len(str(years[0])) == 4
        assert isinstance(year_symbols[years[0]], list)
        assert isinstance(year_symbols[years[0]][0], str)


def test__get_start_timestamp(etl_history, fut_year_symbols):
    global START_TS
    fut_start_ts = etl_history._get_start_timestamp(fut_year_symbols, AssetKind.FUTURES)
    START_TS = fut_start_ts
    max_year = max(fut_year_symbols.keys())
    assert fut_start_ts is not None
    assert max_year == fut_start_ts.year


def test_detect_last_update(etl_history):
    start_ts = etl_history.detect_last_update()
    assert start_ts is not None
    assert isinstance(start_ts, pd.Timestamp)


def test__parse_fn_timestamp(etl_history):
    files = [
        ('25-01-22T00-06.parquet', pd.Timestamp('2025-01-22T00:06:00', tz=datetime.UTC)),
        ('25-01-22T04.parquet', pd.Timestamp('2025-01-22T04:00:00', tz=datetime.UTC)),
        ('25-01-22.parquet', pd.Timestamp('2025-01-22T00:00:00', tz=datetime.UTC)),
        ('2025-01-22.parquet', pd.Timestamp('2025-01-22T00:00:00', tz=datetime.UTC)),
    ]
    for fn, ts in files:
        parsed_ts = etl_history._parse_fn_timestamp(fn)
        assert parsed_ts == ts, f'{parsed_ts} is different for {ts} for file {fn}'


def test__filter_fn(etl_history):
    start_ts = pd.Timestamp('2025-01-25', tz=datetime.UTC)
    files = ['25-01-22T00-06.parquet', '25-01-25T10-00.parquet', '2025-01-30.parquet']
    update_files = etl_history._filter_files(files, start_ts)
    assert isinstance(update_files, list)
    assert len(update_files) == 2
    assert update_files == files[1:]


def test_get_symbols_asset_by_timeframes_updates_fn(etl_history, option_symbol):
    start_ts = None  # pd.Timestamp.now(tz=datetime.UTC) - pd.Timedelta(days=3)
    updates_files = etl_history.get_symbols_asset_by_timeframes_updates_fn(start_ts)
    _check_update_files_dict(updates_files, option_symbol, etl_history.update_path, etl_history._exchange_code,
                             etl_history._asset_kinds[0].value)


def _check_update_files_dict(updates_files, option_symbol, update_path, exchange_code, asset_kind_value):
    assert isinstance(updates_files, dict)
    symbols = list(updates_files.keys())
    assert option_symbol in symbols
    asset_kinds_values = list(updates_files[option_symbol].keys())
    assert len(asset_kinds_values) > 0
    assert asset_kind_value in asset_kinds_values
    asset_kind_some_value = asset_kinds_values[-1]
    assert AssetKind(asset_kind_some_value)
    timeframes_values = list(updates_files[option_symbol][asset_kind_value].keys())
    timeframe_value = timeframes_values[0]
    assert Timeframe(timeframe_value)
    files = updates_files[option_symbol][asset_kind_value][timeframe_value]
    assert isinstance(files, list)
    fn = files[0]
    assert isinstance(fn, str)
    assert fn.endswith('.parquet')
    assert update_path in fn
    assert exchange_code in fn
    assert asset_kind_value in fn


def test_get_symbols_asset_by_timeframes_updates_fn_for_symbol_and_asset(etl_history_future):
    # start_ts = pd.Timestamp.now(tz=datetime.UTC) - pd.Timedelta(days=3)
    updates_files = etl_history_future.get_symbols_asset_by_timeframes_updates_fn(None)
    _check_update_files_dict(updates_files, etl_history_future._symbols[0], etl_history_future.update_path,
                             etl_history_future._exchange_code, etl_history_future._asset_kinds[0].value)


def test_get_update_timeframes_files(etl_history_future):
    # TODO
    updates_files = etl_history_future.get_symbols_asset_by_timeframes_updates_fn(None)
    symbol = etl_history_future._symbols[0]
    asset_kind_value = etl_history_future._asset_kinds[0].value
    _check_update_files_dict(updates_files, symbol, etl_history_future.update_path,
                             etl_history_future._exchange_code, asset_kind_value)
    df = etl_history_future.join_symbols_kind_diff_timeframes_update_files(updates_files[symbol][asset_kind_value],
                                                                           symbol, asset_kind_value)



# def test_prepare(etl_history):
#     etl_history.prepare()
