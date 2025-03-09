"""Main test configuration"""
import os
import datetime
import pytest
import pandas as pd
from functools import lru_cache

from option_lib.option_data_class import OptionData
from exchange.deribit import DeribitExchange
from option_lib.enrichment import join_option_with_future
from option_lib.chain.chain_selector import select_chain, get_max_settlement_valid_expired_date
from option_lib.chain.price_status import get_chain_atm_strike
from option_lib.entities import LegType, OptionLeg, AssetKind, Timeframe, OptionColumns as OCl
from option_lib.provider import PandasLocalFileProvider, RequestParameters

_DATA_PATH = os.path.normpath(os.path.abspath(os.environ.get('DATA_PATH', '../../data')))

pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 200)
pd.set_option('display.max_rows', 300)

_CACHE = {}


@pytest.fixture(name='data_path')
def fixture_data_path() -> str:
    """ Local file storage path """
    return _DATA_PATH


@pytest.fixture(name='update_path')
def fixture_update_data_path() -> str:
    """ Local file storage path """
    return f'{_DATA_PATH}/update'


@pytest.fixture(name='exchange_code')
def fixture_exchange_code() -> str:
    """ In future from combination exchange / symbol? """
    return 'DERIBIT'


@pytest.fixture(name='option_symbol')
def fixture_option_symbol() -> str:
    """ Option symbol for tests"""
    return 'BTC'


@pytest.fixture(name='option_data')
def fixture_option_data(exchange_provider, option_symbol, provider_params):
    """Option data instance"""
    opt_data = OptionData(exchange_provider, option_symbol, provider_params)
    return opt_data


@pytest.fixture(name='exchange_provider')
def fixture_exchange_provider(exchange_code, data_path) -> PandasLocalFileProvider:
    """Local provider"""
    return PandasLocalFileProvider(exchange_code=exchange_code, data_path=data_path)


def _get_update_file_list(base_path: str, asset_kind: AssetKind):
    asset_kind_path = os.path.abspath(
        os.path.normpath(os.path.join(base_path, asset_kind.value)))
    max_depth = 3
    timeframes = os.listdir(asset_kind_path)
    updates_files = []
    for tm in timeframes:
        timeframe_path = os.path.join(asset_kind_path, tm)
        for root_path, dirs, files in os.walk(timeframe_path):
            if root_path[len(timeframe_path):].count(os.sep) > max_depth:
                break
            if not files:
                continue
            updates_files.extend([os.path.join(root_path, fn) for fn in sorted(files)])
            if len(updates_files) > 20:
                break
        if len(updates_files) > 20:
            break
    return updates_files


@pytest.fixture(name='option_update_files')
@lru_cache
def option_update_files_fixture(update_path, exchange_code, option_symbol):
    if _CACHE.get('_update_files') is None:
        _CACHE['_update_files'] = _get_update_file_list(os.path.join(update_path, exchange_code, option_symbol),
                                                        AssetKind.OPTION)
    return _CACHE['_update_files']


@pytest.fixture(name='future_update_files')
@lru_cache
def future_update_files_fixture(update_path, exchange_code, option_symbol):
    updates_files = _get_update_file_list(os.path.join(update_path, exchange_code, option_symbol), AssetKind.FUTURE)
    return updates_files


@pytest.fixture(name='provider_params')
def fixture_provider_params(exchange_provider, option_symbol):
    cur_dt = datetime.date.today()
    fn_path = exchange_provider._fn_path_prepare(option_symbol, AssetKind.OPTION, Timeframe.EOD, cur_dt.year)
    if not os.path.exists(fn_path):
        list_of_files = sorted(os.listdir(os.path.dirname(fn_path)))
        if len(list_of_files) == 0:
            raise FileNotFoundError(f'There is no data for {option_symbol}')
        last_year_fn = list_of_files[-1]
        if not last_year_fn.endswith('parquet'):
            raise FileNotFoundError(f'There is no data for {option_symbol}')
        cur_dt = datetime.date.fromisoformat(f'{last_year_fn.replace(".parquet", "")}-01-01')
    params = RequestParameters(period_from=None, period_to=cur_dt.year,
                               timeframe=Timeframe.EOD)
    return params


@pytest.fixture(name='df_opt_hist')
def fixture_df_hist(option_symbol, exchange_provider, provider_params):
    """Option dataframe"""
    if _CACHE.get('DF_OPT') is None:
        _CACHE['DF_OPT'] = exchange_provider.load_option_history(symbol=option_symbol, params=provider_params,
                                                                 columns=exchange_provider.option_columns)
    return _CACHE['DF_OPT'].copy()


@pytest.fixture(name='df_fut_hist')
def fixture_df_fut(option_symbol, exchange_provider, provider_params):
    """Future dataframe"""
    if _CACHE.get('DF_FUT') is None:
        _CACHE['DF_FUT'] = exchange_provider.load_future_history(symbol=option_symbol, params=provider_params,
                                                                 columns=exchange_provider.future_columns)
    return _CACHE['DF_FUT'].copy()


@pytest.fixture(name='df_ext_hist')
def fixture_df_ext_hist(df_opt_hist, df_fut_hist):
    """Option dataframe with future"""
    if _CACHE.get('DF_EXT_OPT') is None:
        _CACHE['DF_EXT_OPT'] = join_option_with_future(df_opt_hist, df_fut_hist)
    return _CACHE['DF_EXT_OPT'].copy()


@pytest.fixture(name='df_chain')
def fixture_df_chain(df_opt_hist):
    """Option dataframe with future"""
    if _CACHE.get('DF_CHAIN') is None:
        _CACHE['DF_CHAIN'] = select_chain(df_opt_hist)
    return _CACHE['DF_CHAIN'].copy()


@pytest.fixture(name='df_chain_exp_len')
def fixture_df_chain_exp_len(df_opt_hist):
    """Option dataframe with future"""
    if _CACHE.get('MIN_CHAIN_EXPIRATION_LEN') is None:
        expiration_date = get_max_settlement_valid_expired_date(df_opt_hist)
        _CACHE['MIN_CHAIN_EXPIRATION_LEN'] = len(df_opt_hist[df_opt_hist[OCl.EXPIRATION_DATE.nm] ==
                                                             expiration_date][OCl.TIMESTAMP.nm].unique())
    return _CACHE['MIN_CHAIN_EXPIRATION_LEN']


@pytest.fixture(name='atm_strike')
def atm_strike_fixture(df_chain):
    """Current  ATM strike value"""
    if _CACHE.get('ATM_STRIKE') is None:
        _CACHE['ATM_STRIKE'] = get_chain_atm_strike(df_chain)
    return _CACHE['ATM_STRIKE']


@pytest.fixture(name='structure_long_call')
def structure_long_call_fixture(atm_strike):
    """Legs for Naked Long Call"""
    structure_legs = [OptionLeg(strike=atm_strike, lots=10, type=LegType.OPTION_CALL)]
    return structure_legs


@pytest.fixture(name='structure_long_put')
def structure_long_put_fixture(atm_strike):
    """Legs for Naked Long Put"""
    structure_legs = [OptionLeg(strike=atm_strike, lots=10, type=LegType.OPTION_PUT)]
    return structure_legs


@pytest.fixture(name='structure_long_straddle')
def structure_long_straddle_fixture(atm_strike):
    """Legs for Long Straddle"""
    structure_legs = [
        OptionLeg(strike=atm_strike, lots=10, type=LegType.OPTION_CALL),
        OptionLeg(strike=atm_strike, lots=10, type=LegType.OPTION_PUT),
    ]
    return structure_legs


@pytest.fixture(name='deribit_client')
def deribit_client_fixture():
    """Deribit client"""
    deribit = DeribitExchange(api_url=DeribitExchange.TEST_API_URL)
    return deribit
