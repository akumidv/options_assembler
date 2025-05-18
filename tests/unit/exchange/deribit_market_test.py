"""Deribit exchange provider"""
import datetime

import pandas as pd
import pytest

from option_lib.entities import (
    OptionColumns as OCl,
    FutureColumns as FCl,
    SpotColumns as SCl
)
from exchange import RequestClass
from exchange import AbstractExchange
from exchange.deribit import DeribitMarket, DeribitExchange, DeribitAssetKind


@pytest.fixture(name='deribit_market')
def deribit_market_fixture():
    """Deribit market data client"""
    client = RequestClass(api_url=DeribitExchange.TEST_API_URL)
    deribit_market = DeribitMarket(client)
    return deribit_market


def test_deribit_market_init():
    client = RequestClass(api_url=DeribitExchange.TEST_API_URL)
    deribit_market = DeribitMarket(client)
    assert isinstance(deribit_market, DeribitMarket)


def test_get_instruments(deribit_market):
    symbols_df = deribit_market.get_instruments()
    assert isinstance(symbols_df, pd.DataFrame)
    assert len(symbols_df) > 0
    assert f'price_index' in symbols_df.columns
    assert not symbols_df[symbols_df[f'price_index'] == 'btc_usd'].empty


def test_get_book_summary_by_currency(deribit_market):
    book_summary_df = deribit_market.get_book_summary_by_currency(currency=DeribitExchange.CURRENCIES[0])
    assert isinstance(book_summary_df, pd.DataFrame)
    assert len(book_summary_df) > 0
    assert 'base_currency' in book_summary_df.columns
    assert not book_summary_df[
        book_summary_df['base_currency'] == DeribitExchange.CURRENCIES[0]].empty


def test__normalize_book_spot(deribit_market):
    spot_df = pd.DataFrame({'high': [0.0349, 96825.2923, 56363.5, 105141.8784, None],
                            'low': [0.0348, 81192.0, 56363.5, 105141.8784, None],
                            'last': [0.0348, 81192.0, 56363.5, 105141.8784, 22.3286],
                            'instrument_name': ['ETH_BTC', 'BTC_USDC', 'BTC_EURR', 'BTC_USYC', 'BTC_PAXG'],
                            'bid_price': [0.0356, 63933.0, 141.87, None, None],
                            'ask_price': [0.0461, 81192.0, 56363.5, None, 22.0304],
                            'mark_price': [0.033988, 100031.0749, 97099.1846, 93125.797049, 37.0976],
                            'price_change': [-0.2865, 0.0, 0.0, 0.0, None],
                            'volume': [0.3046, 10.5283, 0.0005, 0.0001, 0.0],
                            'base_currency': ['ETH', 'BTC', 'BTC', 'BTC', 'BTC'],
                            'creation_timestamp': [1736991799679, 1736991799679, 1736991799679, 1736991799679,
                                                   1736991799679],
                            'estimated_delivery_price': [0.033988, 100031.0749, 97099.1846, 93125.797049, 37.0976],
                            'quote_currency': ['BTC', 'USDC', 'EURR', 'USYC', 'PAXG'],
                            'volume_usd': [1044.26, 1011551.12, 29.02, 11.3, 0.0],
                            'volume_notional': [0.01061433, 1011146.6566, 28.18175, 10.514188, 0.0],
                            'mid_price': [0.04085, 72562.5, 28252.685, None, None]})
    df = deribit_market._normalize_book(spot_df, pd.Timestamp.now(tz=datetime.UTC))
    assert SCl.ASSET_CODE.nm in df.columns
    assert SCl.ASSET_TYPE.nm in df.columns
    assert list(df[SCl.ASSET_TYPE.nm].unique()) == [DeribitAssetKind.SPOT.code]


def test_get_book_summary_by_currency_option_spot(deribit_market):
    book_summary_df = deribit_market.get_book_summary_by_currency(currency=DeribitExchange.CURRENCIES[0],
                                                                  kind=DeribitAssetKind.SPOT)
    """
          high      low        last instrument_name    bid_price   ask_price    mark_price  price_change  volume base_currency  creation_timestamp  \
0  47274.0  47274.0  47274.0000        BTC_USDT   30898.0000  47274.0000  94949.739300           0.0  0.0603           BTC       1736817367112
1      NaN      NaN      0.0346         ETH_BTC       0.0365      0.0461      0.033236           NaN  0.0000           ETH       1736817367112
2  80684.0  80684.0  80684.0000        BTC_USDC   63933.0000  76485.0000  94911.337800           0.0  0.0960           BTC       1736817367112
3  56363.5  56363.5  56363.5000        BTC_EURR     141.8700  56363.5000  92655.670500           0.0  0.0001           BTC       1736817367112
4      NaN      NaN  67399.2320        BTC_USYC  105141.8784         NaN  88380.887719           NaN  0.0000           BTC       1736817367112
5      NaN      NaN     22.3286        BTC_PAXG          NaN     22.0304     35.537400           NaN  0.0000           BTC       1736817367112
6      NaN      NaN      0.0275        PAXG_BTC       0.0200      0.0290      0.028100           NaN  0.0000          PAXG       1736817367112

  quote_currency  estimated_delivery_price  volume_usd  volume_notional   mid_price
0           USDT              94949.739300     2849.67       2850.62220  39086.0000
1            BTC                  0.033236        0.00          0.00000      0.0413
2           USDC              94911.337800     7747.53       7745.66400  70209.0000
3           EURR              92655.670500        5.76          5.63635  28252.6850
4           USYC              88380.887719        0.00          0.00000         NaN
5           PAXG                 35.537400        0.00          0.00000         NaN
6            BTC                  0.028100        0.00          0.00000      0.0245
    """
    assert isinstance(book_summary_df, pd.DataFrame)
    assert len(book_summary_df) > 0
    assert 'base_currency' in book_summary_df.columns
    assert not book_summary_df[
        book_summary_df['base_currency'] == DeribitExchange.CURRENCIES[0]].empty


def test__normalize_book_future(deribit_market):
    fut_df = pd.DataFrame({'high': [104960.0, 99776.0, 102500.0, 104836.0, 97900.0],
                           'low': [92342.5, 91995.0, 93739.26, 102017.18, 91562.5],
                           'last': [101019.0, 99677.5, 97678.0, 104750.0, 97457.0],
                           'instrument_name': ['BTC-27JUN25', 'BTC-31JAN25', 'BTC-28FEB25', 'BTC-26SEP25',
                                               'BTC-28MAR25'],
                           'bid_price': [90505.0, None, 89782.5, 90505.0, 95900.0],
                           'ask_price': [111500.0, None, 102500.0, 119000.0, 105590.0],
                           'open_interest': [390279240, 46864950, 21660580, 239016160, 472960750],
                           'mark_price': [96760.58, 99400.67, 97707.42, 104378.95, 97477.14],
                           'price_change': [0.5765, 3.7362, 4.2018, 1.0234, 3.2583],
                           'volume': [216.69274001, 129.95052186, 206.28117084, 71.6209281, 374.26395344],
                           'base_currency': ['BTC', 'BTC', 'BTC', 'BTC', 'BTC'],
                           'creation_timestamp': [1736993696792, 1736993696792, 1736993696792, 1736993696792,
                                                  1736993696792],
                           'estimated_delivery_price': [100064.33, 100064.33, 100064.33, 100064.33, 100064.33],
                           'quote_currency': ['USD', 'USD', 'USD', 'USD', 'USD'],
                           'volume_usd': [21776140.0, 12489490.0, 20245610.0, 7494340.0, 36170390.0],
                           'volume_notional': [21776140.0, 12489490.0, 20245610.0, 7494340.0, 36170390.0],
                           'mid_price': [101002.5, None, 96141.25, 104752.5, 100745.0],
                           'current_funding': [None, None, None, None, None],
                           'funding_8h': [None, None, None, None, None]})
    df = deribit_market._normalize_book(fut_df, pd.Timestamp.now(tz=datetime.UTC))
    assert FCl.BASE_CODE.nm in df.columns
    assert FCl.ASSET_TYPE.nm in df.columns
    assert list(df[FCl.ASSET_TYPE.nm].unique()) == [DeribitAssetKind.FUTURE.code]
    assert None not in list(df[FCl.EXPIRATION_DATE.nm].unique())
    assert df[OCl.PRICE.nm].notnull().any()


def test_get_book_summary_by_currency_future(deribit_market):
    """
      high        low      last instrument_name  bid_price  ask_price  open_interest  mark_price  price_change       volume base_currency  \
0  105510.00   99967.97  101440.0     BTC-27JUN25   100232.5   111500.0      389887690   101642.84       -3.8575   182.275303           BTC
1   97181.50   94115.00   95775.5     BTC-31JAN25        NaN    97257.5       48703100    95900.50       -0.5999   100.378640           BTC
2   94115.50   90297.50   92186.5     BTC-28FEB25    90295.0   102500.0       33001880    92416.81        2.0920   138.928180           BTC
3  105530.50  103474.48  105513.5     BTC-26SEP25   104777.5   119000.0      228409500   105539.38        1.8999   140.660747           BTC
4  100777.50   92411.00   97202.5     BTC-28MAR25    96897.5    98237.5      470159290    97493.39        0.8482   188.049248           BTC
5   97740.43   94078.94   96283.0     BTC-24JAN25        NaN        NaN       23384250    96196.61        0.9414   315.707454           BTC

   creation_timestamp quote_currency  estimated_delivery_price   volume_usd  volume_notional  mid_price  current_funding  funding_8h
0       1736816785217            USD                  94662.41   18466880.0       18466880.0  105866.25              NaN         NaN
1       1736816785217            USD                  94662.41    9600050.0        9600050.0        NaN              NaN         NaN
2       1736816785217            USD                  94662.41   12730050.0       12730050.0   96397.50              NaN         NaN
3       1736816785217            USD                  94662.41   14800000.0       14800000.0  111888.75              NaN         NaN
4       1736816785217            USD                  94662.41   18386450.0       18386450.0   97567.50              NaN         NaN
5       1736816785217            USD                  94662.41   30229760.0       30229760.0        NaN              NaN         NaN

    """
    book_summary_df = deribit_market.get_book_summary_by_currency(currency=DeribitExchange.CURRENCIES[0],
                                                                  kind=DeribitAssetKind.FUTURE)
    assert isinstance(book_summary_df, pd.DataFrame)
    assert len(book_summary_df) > 0
    assert 'base_currency' in book_summary_df.columns
    assert not book_summary_df[
        book_summary_df['base_currency'] == DeribitExchange.CURRENCIES[0]].empty
    assert list(book_summary_df[FCl.ASSET_TYPE.nm].unique()) == [DeribitAssetKind.FUTURE.code]
    assert book_summary_df[OCl.PRICE.nm].notnull().any()


def test__normalize_book_future_combo(deribit_market):
    fut_combo_df = pd.DataFrame({'high': [None, None, None, None, None], 'low': [None, None, None, None, None],
                                 'last': [None, None, None, None, None],
                                 'instrument_name': ['BTC-FS-26SEP25_24JAN25', 'BTC-FS-28FEB25_24JAN25',
                                                     'BTC-FS-28FEB25_31JAN25', 'BTC-FS-26DEC25_17JAN25',
                                                     'BTC-FS-28MAR25_17JAN25'],
                                 'bid_price': [None, None, None, None, None],
                                 'ask_price': [None, None, None, None, None],
                                 'mark_price': [2265.11, -4364.62, -1258.25, -792.67, -1001.75],
                                 'price_change': [None, None, None, None, None], 'volume': [0.0, 0.0, 0.0, 0.0, 0.0],
                                 'base_currency': ['BTC', 'BTC', 'BTC', 'BTC', 'BTC'],
                                 'creation_timestamp': [1737073906519, 1737073906519, 1737073906519, 1737073906519,
                                                        1737073906519],
                                 'estimated_delivery_price': [100173.8, 100173.8, 100173.8, 100173.8, 100173.8],
                                 'quote_currency': ['USD', 'USD', 'USD', 'USD', 'USD'],
                                 'volume_usd': [0.0, 0.0, 0.0, 0.0, 0.0], 'volume_notional': [0.0, 0.0, 0.0, 0.0, 0.0],
                                 'mid_price': [None, None, None, None, None]})
    df = deribit_market._normalize_book(fut_combo_df, pd.Timestamp.now(tz=datetime.UTC))
    assert FCl.BASE_CODE.nm in df.columns
    assert FCl.ASSET_TYPE.nm in df.columns
    assert list(df[FCl.ASSET_TYPE.nm].unique()) == [DeribitAssetKind.FUTURE_COMBO.code]
    assert None not in list(df[FCl.EXPIRATION_DATE.nm].unique())


def test_get_book_summary_by_currency_future_combo(deribit_market):
    """
            high     low    last         instrument_name  bid_price  ask_price  mark_price  price_change  volume base_currency  creation_timestamp  \
   0      NaN     NaN     NaN  BTC-FS-26SEP25_24JAN25        NaN        NaN     9345.68           NaN     0.0           BTC       1736816837473
   1      NaN     NaN     NaN  BTC-FS-28FEB25_24JAN25        NaN        NaN    -3747.67           NaN     0.0           BTC       1736816837473
   2      NaN     NaN     NaN  BTC-FS-28FEB25_31JAN25        NaN        NaN    -3438.67           NaN     0.0           BTC       1736816837473
   3      NaN     NaN     NaN  BTC-FS-26DEC25_17JAN25        NaN        NaN     5582.45           NaN     0.0           BTC       1736816837473
   4      NaN     NaN     NaN  BTC-FS-28MAR25_17JAN25        NaN        NaN     4644.21           NaN     0.0           BTC       1736816837473
   5      NaN     NaN     NaN  BTC-FS-26SEP25_17JAN25        NaN        NaN    12641.59           NaN     0.0           BTC       1736816837473
     quote_currency  estimated_delivery_price  volume_usd  volume_notional  mid_price
   0             USD                  94670.71         0.0              0.0        NaN
   1             USD                  94670.71         0.0              0.0        NaN
   2             USD                  94670.71         0.0              0.0        NaN
   3             USD                  94670.71         0.0              0.0        NaN
   4             USD                  94670.71         0.0              0.0        NaN
   5             USD                  94670.71         0.0              0.0        NaN
       """
    book_summary_df = deribit_market.get_book_summary_by_currency(currency=DeribitExchange.CURRENCIES[0],
                                                                  kind=DeribitAssetKind.FUTURE_COMBO)

    assert isinstance(book_summary_df, pd.DataFrame)
    assert len(book_summary_df) > 0
    assert 'base_currency' in book_summary_df.columns
    assert not book_summary_df[
        book_summary_df['base_currency'] == DeribitExchange.CURRENCIES[0]].empty
    assert list(book_summary_df[OCl.ASSET_TYPE.nm].unique()) == [DeribitAssetKind.FUTURE_COMBO.code]
    assert None not in list(book_summary_df[OCl.EXPIRATION_DATE.nm].unique())
    assert book_summary_df[OCl.PRICE.nm].notnull().any()


def test__normalize_book_option(deribit_market):
    opt_df = pd.DataFrame({'high': [None, None, None, None, 0.0145, None, None],
                           'low': [None, None, None, None, 0.0145, None, None],
                           'last': [None, None, 0.0001, None, 0.0145, None, None],
                           'bid_price': [0.101, None, None, None, 0.018, 0.1070, None],
                           'ask_price': [0.2385, None, None, None, 0.019, 0.1460, None],
                           'instrument_name': ['BTC-7FEB25-106000-P', 'BTC-18JAN25-107000-P', 'BTC-24JAN25-60000-P',
                                               'BTC-27JUN25-230000-C', 'BTC-31JAN25-92000-P', 'ETH-18JAN25-3000-C',
                                               'DOGE_USDC-7FEB25-0d4064-C'],
                           'open_interest': [0.0, 0.0, 0.0, 0.0, 135.94, 0.0, 0.0],
                           'mark_price': [0.1042716, 0.05807133, 0.0, 0.01184211, 0.01866323, 0.107292, 0.001296],
                           'price_change': [None, None, None, None, None, None, None],
                           'interest_rate': [0.0, 0.0, 0.0, 0.0, 0.0, None, None],
                           'volume': [0.0, 0.0, 0.0, 0.0, 3.21, 0.0, 0.0],
                           'mark_iv': [60.86, 41.39, 0.0, 74.14, 60.13, 0.0, 0.0],
                           'underlying_price': [98763.21, 101136.99285714286, 102188.24, 96813.04, 99067.37, 3359.61,
                                                0.385358],
                           'underlying_index': ['SYN.BTC-7FEB25', 'SYN.BTC-18JAN25', 'BTC-24JAN25', 'BTC-27JUN25',
                                                'BTC-31JAN25', 'index_price', 'SYN.DOGE_USDC-7FEB25'],
                           'base_currency': ['BTC', 'BTC', 'BTC', 'BTC', 'BTC', 'ETH', 'DOGE'],
                           'creation_timestamp': [1737074222663, 1737074222663, 1737074222663, 1737074222663,
                                                  1737074222663, 1737088726575, 1737088730082],
                           'estimated_delivery_price': [100143.63, 100143.63, 100143.63, 100143.63, 100143.63, 3359.61,
                                                        0.385417],
                           'quote_currency': ['BTC', 'BTC', 'BTC', 'BTC', 'BTC', 'ETH', 'USDC'],
                           'volume_usd': [0.0, 0.0, 0.0, 0.0, 4624.81, 0.0, 0.0],
                           'mid_price': [0.16975, None, None, None, 0.0185, 0.12650, None]})
    df = deribit_market._normalize_book(opt_df, pd.Timestamp.now(tz=datetime.UTC))
    assert OCl.BASE_CODE.nm in df.columns
    assert OCl.ASSET_TYPE.nm in df.columns
    assert list(df[OCl.ASSET_TYPE.nm].unique()) == [DeribitAssetKind.OPTION.code]
    assert None not in list(df[OCl.EXPIRATION_DATE.nm].unique())
    assert None not in list(df[OCl.STRIKE.nm].unique())
    assert None not in list(df[OCl.OPTION_TYPE.nm].unique())
    assert df[OCl.PRICE.nm].notnull().any()


def test_get_book_summary_by_currency_option(deribit_market):
    book_summary_df = deribit_market.get_book_summary_by_currency(currency=DeribitExchange.CURRENCIES[0],
                                                                  kind=DeribitAssetKind.OPTION)
    """
            high     low    last       instrument_name  bid_price  ask_price  open_interest  mark_price  price_change  volume  interest_rate  mark_iv  \
0        NaN     NaN  0.1590  BTC-27JUN25-128000-P        NaN        NaN            0.0    0.349429           NaN     0.0            0.0    66.28
1        NaN     NaN  0.0017  BTC-28FEB25-180000-C     0.0013     0.0016           77.1    0.001230           NaN     0.0            0.0    80.20
2        NaN     NaN  0.4345   BTC-26SEP25-52000-C        NaN        NaN            0.4    0.507668           NaN     0.0            0.0    34.79
3        NaN     NaN  0.6040   BTC-27JUN25-40000-C     0.6115     0.6145           82.8    0.613451           NaN     0.0            0.0    79.44
4        NaN     NaN     NaN  BTC-17JAN25-109000-C        NaN        NaN            0.0    0.000327           NaN     0.0            0.0    74.01

      underlying_price underlying_index base_currency  creation_timestamp quote_currency  estimated_delivery_price  volume_usd  mid_price
0            102124.90      BTC-27JUN25           BTC       1736817040149            BTC                  94892.05        0.00        NaN
1             92648.84      BTC-28FEB25           BTC       1736817040149            BTC                  94892.05        0.00    0.00145
2            105509.29      BTC-26SEP25           BTC       1736817040149            BTC                  94892.05        0.00        NaN
3            102124.90      BTC-27JUN25           BTC       1736817040149            BTC                  94892.05        0.00    0.61300
4             93114.98      BTC-17JAN25           BTC       1736817040149            BTC                  94892.05        0.00        NaN

    """
    assert isinstance(book_summary_df, pd.DataFrame)
    assert len(book_summary_df) > 0
    assert 'base_currency' in book_summary_df.columns
    assert not book_summary_df[
        book_summary_df['base_currency'] == DeribitExchange.CURRENCIES[0]].empty
    assert None not in list(book_summary_df[OCl.EXPIRATION_DATE.nm].unique())
    assert None not in list(book_summary_df[OCl.STRIKE.nm].unique())
    assert None not in list(book_summary_df[OCl.OPTION_TYPE.nm].unique())
    assert book_summary_df[OCl.PRICE.nm].notnull().any()


def test__normalize_book_option_combo(deribit_market):
    opt_combo_df = pd.DataFrame({'high': [None, None, None, None, None], 'low': [None, None, None, None, None],
                                 'last': [None, None, None, None, None],
                                 'instrument_name': ['BTC-CSR13-17JAN25-50000_55000', 'BTC-CSR13-31JAN25-100000_110000',
                                                     'BTC-CSR13-31JAN25-44000_110000', 'BTC-PSR13-31JAN25-96000_94000',
                                                     'BTC-CBUT-28MAR25-90000_95000_100000'],
                                 'bid_price': [None, 0.0001, None, None, 0.0001],
                                 'ask_price': [None, 0.005, None, None, None],
                                 'mark_price': [-0.86082998, -0.02057613, 0.49287308, -0.04157543, 0.00473082],
                                 'price_change': [None, None, None, None, None], 'volume': [0.0, 0.0, 0.0, 0.0, 0.0],
                                 'base_currency': ['BTC', 'BTC', 'BTC', 'BTC', 'BTC'],
                                 'creation_timestamp': [1737074402953, 1737074402953, 1737074402953, 1737074402953,
                                                        1737074402953],
                                 'estimated_delivery_price': [100216.53, 100216.53, 100216.53, 100216.53, 100216.53],
                                 'quote_currency': ['BTC', 'BTC', 'BTC', 'BTC', 'BTC'],
                                 'volume_usd': [0.0, 0.0, 0.0, 0.0, 0.0],
                                 'mid_price': [None, 0.00255, None, None, None]})
    df = deribit_market._normalize_book(opt_combo_df, pd.Timestamp.now(tz=datetime.UTC))
    assert OCl.BASE_CODE.nm in df.columns
    assert OCl.ASSET_TYPE.nm in df.columns
    assert list(df[OCl.ASSET_TYPE.nm].unique()) == [DeribitAssetKind.OPTION_COMBO.code]


def test_get_book_summary_by_currency_option_combo(deribit_market):
    book_summary_df = deribit_market.get_book_summary_by_currency(currency=DeribitExchange.CURRENCIES[0],
                                                                  kind=DeribitAssetKind.OPTION_COMBO)
    """
       high   low    last                             instrument_name  bid_price ask_price  mark_price price_change  volume base_currency  \
0  None  None     NaN             BTC-CSR13-31JAN25-100000_110000        NaN      None   -0.003666         None     0.0           BTC
1  None  None  0.0001   BTC-PCOND-24JAN25-35000_50000_65000_80000        NaN      None    0.002459         None     0.0           BTC
2  None  None     NaN              BTC-CSR12-17JAN25-97000_100000        NaN      None    0.001302         None     0.0           BTC
3  None  None     NaN                       BTC-REV-31JAN25-60000        NaN      None    0.375428         None     0.0           BTC
4  None  None     NaN         BTC-CBUT-28MAR25-90000_95000_100000     0.0001      None    0.003208         None     0.0           BTC
5  None  None     NaN  BTC-ICOND-31JAN25-82000_84000_98000_100000        NaN      None   -0.008752         None     0.0           BTC

   creation_timestamp quote_currency  estimated_delivery_price  volume_usd mid_price
0       1736817078416            BTC                  94961.79         0.0      None
1       1736817078416            BTC                  94961.79         0.0      None
2       1736817078416            BTC                  94961.79         0.0      None
3       1736817078416            BTC                  94961.79         0.0      None
4       1736817078416            BTC                  94961.79         0.0      None
5       1736817078416            BTC                  94961.79         0.0      None
    """
    assert isinstance(book_summary_df, pd.DataFrame)
    assert len(book_summary_df) > 0
    assert 'base_currency' in book_summary_df.columns
    assert not book_summary_df[book_summary_df['base_currency'] == DeribitExchange.CURRENCIES[0]].empty
