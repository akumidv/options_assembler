"""
Deribit api provider
"""
import datetime
import re
import pandas as pd
import concurrent
from concurrent.futures import ThreadPoolExecutor
from option_lib.entities.enum_code import EnumCode
from option_lib.entities import (
    Timeframe, AssetKind, OptionType,
    OptionColumns as OCl,
    FutureColumns as FCl,
    SpotColumns as SCl,
    ALL_COLUMN_NAMES
)
from option_lib.normalization.datetime_conversion import df_columns_to_timestamp
from option_lib.normalization import parse_expiration_date, normalize_timestamp, fill_option_price
from exchange.exchange_entities import ExchangeCode
from exchange._abstract_exchange import AbstractExchange, RequestClass
from provider import DataEngine, RequestParameters
class DeribitAssetKind(EnumCode):
    """Deribit instrument kinds"""
    FUTURE = 'future', AssetKind.FUTURE.code
    OPTION = AssetKind.OPTION.value, AssetKind.OPTION.code
    SPOT = AssetKind.SPOT.value, AssetKind.SPOT.code # TODO Crytpo !
    FUTURE_COMBO = 'future_combo', 'fc'
    OPTION_COMBO = 'option_combo', 'oc'


DOT_STRIKE_REGEXP = re.compile(r'(\d)d(\d)', flags=re.IGNORECASE)
COLUMNS_TO_CURRENCY = [OCl.ASK.nm, OCl.BID.nm, OCl.LAST.nm, OCl.HIGH_24.nm, OCl.LOW_24.nm, OCl.EXCHANGE_PRICE.nm]


class DeribitMarket:
    """Deribit Market data api"""

    def __init__(self, client: RequestClass):
        self.client = client

    def get_instruments(self) -> pd.DataFrame:
        """Retrieves available trading instruments.
        https://docs.deribit.com/#public-get_instruments
        Results
         price_index    rfq    kind  min_trade_amount         instrument_name  maker_commission  taker_commission instrument_type  expiration_timestamp  creation_timestamp  is_active  tick_size  \
0     btcdvol_usdc  False  future            0.1000    BTCDVOL_USDC-30APR25          -0.00001          0.000501          linear         1746000000000       1745827206000       True     0.1000
1         eth_usdc  False  future            1.0000        ETH_USDC-30APR25           0.00010          0.000500          linear         1746000000000       1745827206000       True     0.5000
2     ethdvol_usdc  False  future            0.1000    ETHDVOL_USDC-30APR25           0.00000          0.000510          linear         1746000000000       1745827206000       True     0.1000
3         bnb_usdc  False  option            1.0000  BNB_USDC-30APR25-520-C           0.00010          0.000500          linear         1746000000000       1745827740000       True     0.1000
4         bnb_usdc  False  option            1.0000  BNB_USDC-30APR25-520-P           0.00010          0.000500          linear         1746000000000       1745827740000       True     0.1000
...            ...    ...     ...               ...                     ...               ...               ...             ...                   ...                 ...        ...        ...
5324     usdc_usdt  False    spot            1.0000               USDC_USDT           0.00000          0.000000          linear        32503708800000       1696425537000       True     0.0001
5325     usde_usdc  False    spot            1.0000               USDE_USDC           0.00010          0.000500          linear        32503708800000       1740392798000       True     0.0001
5326     usde_usdt  False    spot            1.0000               USDE_USDT           0.00010          0.000500          linear        32503708800000       1740392791000       True     0.0001
5327     usyc_usdc  False    spot            0.0001               USYC_USDC           0.00010          0.000500          linear        32503708800000       1726743026000       True     0.0001
5328      xrp_usdc  False    spot            0.0001                XRP_USDC           0.00000          0.000000          linear        32503708800000       1708968980000       True     0.0001


 contract_size  instrument_id settlement_period future_type  max_leverage  max_liquidation_commission  block_trade_commission  block_trade_min_trade_amount  block_trade_tick_size  \
0            0.1000         411540               day      linear          10.0                      0.0075                  0.0001                      200000.0                 0.0100
1            1.0000         411541               day      linear          25.0                      0.0075                  0.0001                      200000.0                 0.0100
2            0.1000         411539               day      linear          10.0                      0.0075                  0.0001                      200000.0                 0.0100
3            1.0000         411656               day         NaN           NaN                         NaN                  0.0003                        2500.0                 0.1000
4            1.0000         411657               day         NaN           NaN                         NaN                  0.0003                        2500.0                 0.1000
...             ...            ...               ...         ...           ...                         ...                     ...                           ...                    ...
5324         1.0000         185048               NaN         NaN           NaN                         NaN                  0.0000                      100000.0                 0.0001
5325         1.0000         393674               NaN         NaN           NaN                         NaN                  0.0000                      100000.0                 0.0001
5326         1.0000         393673               NaN         NaN           NaN                         NaN                  0.0000                      100000.0                 0.0001
5327         0.0001         313861               NaN         NaN           NaN                         NaN                  0.0000                      100000.0                 0.0001
5328         0.0001         239125               NaN         NaN           NaN                         NaN                  0.0000                       50000.0                 0.0001

     settlement_currency base_currency counter_currency quote_currency tick_size_steps  strike option_type
0                   USDC       BTCDVOL             USDC           USDC              []     NaN         NaN
1                   USDC           ETH             USDC           USDC              []     NaN         NaN
2                   USDC       ETHDVOL             USDC           USDC              []     NaN         NaN
3                   USDC           BNB             USDC           USDC              []   520.0        call
4                   USDC           BNB             USDC           USDC              []   520.0         put
...                  ...           ...              ...            ...             ...     ...         ...
5324                 NaN          USDC             USDT           USDT              []     NaN         NaN
5325                 NaN          USDE             USDC           USDC              []     NaN         NaN
5326                 NaN          USDE             USDT           USDT              []     NaN         NaN
5327                 NaN          USYC             USDC           USDC              []     NaN         NaN
5328                 NaN           XRP             USDC           USDC              []     NaN         NaN
        """
        response = self.client.request_api('/public/get_instruments')
        symbols_df = pd.DataFrame(response['result'])
        return symbols_df

    def get_book_summary_by_currency(self, currency: str, kind: DeribitAssetKind | None = None) -> pd.DataFrame:
        """Retrieves the summary information for all instruments for the currency (optionally filtered by kind).
        https://docs.deribit.com/#public-get_book_summary_by_currency
 ask base_currency           bid  current_funding  estimated_delivery_price  exchange_iv  exchange_price       exchange_symbol exchange_underlying_symbol           expiration_date  \
0              NaN           BTC           NaN              NaN              94787.240000        47.77    31001.577276  BTC-26DEC25-124000-P                BTC-26DEC25 2025-12-26 00:00:00+00:00
1              NaN           BTC           NaN              NaN              94787.240000         0.00    75928.213383   BTC-27MAR26-20000-C                BTC-27MAR26 2026-03-27 00:00:00+00:00
2     14644.628580           BTC   6398.138700              NaN              94787.240000        42.59     6533.642747  BTC-16MAY25-100000-P            SYN.BTC-16MAY25 2025-05-16 00:00:00+00:00
3        28.436172           BTC      9.478724              NaN              94787.240000        77.03       13.645571    BTC-2MAY25-82000-P                 BTC-2MAY25 2025-05-02 00:00:00+00:00
4              NaN           BTC           NaN              NaN              94787.240000        55.14      268.262107  BTC-30MAY25-126000-C                BTC-30MAY25 2025-05-30 00:00:00+00:00
...            ...           ...           ...              ...                       ...          ...             ...                   ...                        ...                       ...
1485  80305.000000           BTC     15.000000              NaN              94877.967000          NaN    94877.967000              BTC_USDE                        NaN                       NaT
1486      0.023400           ETH      0.010000              NaN                  0.019055          NaN        0.019055               ETH_BTC                        NaN                       NaT
1487  55069.000000           BTC  45000.000000              NaN              94771.108400          NaN    94771.108400              BTC_USDC                        NaN                       NaT
1488           NaN          PAXG      0.027300              NaN                  0.034900          NaN        0.034900              PAXG_BTC                        NaN                       NaT
1489  56363.500000           BTC   9694.000000              NaN              83388.088300          NaN    83388.088300              BTC_EURR                        NaN                       NaT

      funding_8h       high_24  interest_rate kind          last        low_24   mid_price  open_interest option_type               original_timestamp         price  price_change quote_currency  \
0            NaN           NaN            0.0    o  47611.630652           NaN         NaN            2.5           p 2025-04-30 02:52:24.385000+00:00  47611.630652           NaN            BTC
1            NaN           NaN            0.0    o  74967.228116           NaN         NaN           25.0           c 2025-04-30 02:52:24.385000+00:00  74967.228116           NaN            BTC
2            NaN           NaN            0.0    o           NaN           NaN      0.1110            0.0           p 2025-04-30 02:52:24.385000+00:00  10521.383640           NaN            BTC
3            NaN     37.914896            0.0    o      9.478724      9.478724      0.0002           75.4           p 2025-04-30 02:52:24.385000+00:00      9.478724           0.0            BTC
4            NaN           NaN            0.0    o    616.117060           NaN         NaN           12.5           c 2025-04-30 02:52:24.385000+00:00    616.117060           NaN            BTC
...          ...           ...            ...  ...           ...           ...         ...            ...         ...                              ...           ...           ...            ...
1485         NaN           NaN            NaN    s  83979.000000           NaN  40160.0000            NaN         NaN 2025-04-30 02:52:24.399000+00:00  83979.000000           NaN           USDE
1486         NaN           NaN            NaN    s      0.018800           NaN      0.0167            NaN         NaN 2025-04-30 02:52:24.399000+00:00      0.018800           NaN            BTC
1487         NaN  55069.000000            NaN    s  55069.000000  55069.000000  50034.5000            NaN         NaN 2025-04-30 02:52:24.399000+00:00  55069.000000           0.0           USDC
1488         NaN           NaN            NaN    s      0.037100           NaN         NaN            NaN         NaN 2025-04-30 02:52:24.399000+00:00      0.037100           NaN            BTC
1489         NaN           NaN            NaN    s  56363.500000           NaN  33028.7500            NaN         NaN 2025-04-30 02:52:24.399000+00:00  56363.500000           NaN           EURR

                    request_timestamp  source_ask  source_bid  source_exchange_price  source_high_24  source_last  source_low_24    strike    symbol                 timestamp  \
0    2025-04-30 02:52:38.431543+00:00         NaN         NaN               0.327065             NaN       0.5023            NaN  124000.0       BTC 2025-04-30 02:52:24+00:00
1    2025-04-30 02:52:38.431543+00:00         NaN         NaN               0.801038             NaN       0.7909            NaN   20000.0       BTC 2025-04-30 02:52:24+00:00
2    2025-04-30 02:52:38.431543+00:00      0.1545      0.0675               0.068930             NaN          NaN            NaN  100000.0       BTC 2025-04-30 02:52:24+00:00
3    2025-04-30 02:52:38.431543+00:00      0.0003      0.0001               0.000144          0.0004       0.0001         0.0001   82000.0       BTC 2025-04-30 02:52:24+00:00
4    2025-04-30 02:52:38.431543+00:00         NaN         NaN               0.002830             NaN       0.0065            NaN  126000.0       BTC 2025-04-30 02:52:24+00:00
...                               ...         ...         ...                    ...             ...          ...            ...       ...       ...                       ...
1485 2025-04-30 02:52:38.431543+00:00         NaN         NaN                    NaN             NaN          NaN            NaN       NaN  BTC_USDE 2025-04-30 02:52:24+00:00
1486 2025-04-30 02:52:38.431543+00:00         NaN         NaN                    NaN             NaN          NaN            NaN       NaN   ETH_BTC 2025-04-30 02:52:24+00:00
1487 2025-04-30 02:52:38.431543+00:00         NaN         NaN                    NaN             NaN          NaN            NaN       NaN  BTC_USDC 2025-04-30 02:52:24+00:00
1488 2025-04-30 02:52:38.431543+00:00         NaN         NaN                    NaN             NaN          NaN            NaN       NaN  PAXG_BTC 2025-04-30 02:52:24+00:00
1489 2025-04-30 02:52:38.431543+00:00         NaN         NaN                    NaN             NaN          NaN            NaN       NaN  BTC_EURR 2025-04-30 02:52:24+00:00

     underlying_expiration_date  underlying_price  volume  volume_notional  volume_usd
0     2025-12-26 00:00:00+00:00          99016.61   0.000            0.000        0.00
1     2026-03-27 00:00:00+00:00         100520.99   0.000            0.000        0.00
2     2025-05-16 00:00:00+00:00          94995.47   0.000            0.000        0.00
3     2025-05-02 00:00:00+00:00          94798.27  77.300          755.090      755.09
4     2025-05-30 00:00:00+00:00          95215.23   0.000            0.000        0.00
...                         ...               ...     ...              ...         ...
1485                        NaT               NaN   0.000            0.000        0.00
1486                        NaT               NaN   0.000            0.000        0.00
1487                        NaT               NaN   0.004          220.276      220.28
1488                        NaT               NaN   0.000            0.000        0.00
1489                        NaT               NaN   0.000            0.000        0.00

        """
        params = {'currency': currency}
        if kind is not None:
            params['kind'] = kind.value
        request_timestamp = pd.Timestamp.now(tz=datetime.UTC)
        response = self.client.request_api('/public/get_book_summary_by_currency', params=params)
        book_summary_df = pd.DataFrame(response['result'])
        book_summary_df = self._normalize_book(book_summary_df, request_timestamp)
        return book_summary_df

    @staticmethod
    def _kind_enrichment(row: pd.Series) -> pd.Series:
        try:
            exchange_asset_symbol_arr = DOT_STRIKE_REGEXP.sub(r'\1.\2', row[OCl.ASSET_CODE.nm]).split(
                '-')  # for strike DOGE_USDC-7FEB25-0d4064-C  or 3d12
            symbol = exchange_asset_symbol_arr[0]
            row = row.copy(deep=True)
            match len(exchange_asset_symbol_arr):
                case 1:  # SPOT
                    row[SCl.ASSET_CODE.nm] = symbol
                    row[SCl.ASSET_TYPE.nm] = DeribitAssetKind.SPOT.code
                    return row
                case 2:  # FUT
                    row[FCl.BASE_CODE.nm] = symbol
                    expiration_date = parse_expiration_date(exchange_asset_symbol_arr[1])
                    if expiration_date is None and exchange_asset_symbol_arr[1] != 'PERPETUAL':
                        raise SyntaxError(f'Can not parse {exchange_asset_symbol_arr[1]}, '
                                          f'None expiration can be only for PERPETUAL: {row}')
                    row[FCl.EXPIRATION_DATE.nm] = expiration_date
                    row[FCl.ASSET_TYPE.nm] = DeribitAssetKind.FUTURE.code
                    return row
                case 3:  # FUT COMBO
                    # Second value is strategy for combo, for example FS - future spread
                    row[FCl.BASE_CODE.nm] = symbol
                    row[FCl.EXPIRATION_DATE.nm] = parse_expiration_date(exchange_asset_symbol_arr[2].split('_')[0])
                    row[FCl.ASSET_TYPE.nm] = DeribitAssetKind.FUTURE_COMBO.code
                    return row
                case 4:  # OPT AND OPT COMBO
                    row[OCl.BASE_CODE.nm] = symbol
                    expiration_date = parse_expiration_date(exchange_asset_symbol_arr[1])
                    if expiration_date is None:  # OPT COMBO
                        # Second value is strategy for combo, for example PCOND - put condor, CBUT - call butterfly
                        expiration_date = parse_expiration_date(exchange_asset_symbol_arr[2])
                        kind = DeribitAssetKind.OPTION_COMBO.code
                        option_type = None
                        strike = None
                        future_expiration_date = None
                    else:  # OPT
                        kind = DeribitAssetKind.OPTION.code
                        option_type = exchange_asset_symbol_arr[3]
                        if option_type not in ['C', 'P']:
                            raise SyntaxError(f'Unknown option type {option_type}')
                        option_type = OptionType.CALL.code if exchange_asset_symbol_arr[3] == 'C' else \
                            OptionType.PUT.code
                        strike = float(exchange_asset_symbol_arr[2])

                        under_arr = row[OCl.UNDERLYING_CODE.nm].split('-')
                        if len(under_arr) == 2:
                            future_expiration_date = parse_expiration_date(under_arr[1])
                        else:
                            if row[OCl.UNDERLYING_CODE.nm] in ['SYN.EXPIRY',  # Expired already
                                                                          'index_price']:  # index price
                                future_expiration_date = None
                            else:
                                print('Syntax error in row:\n', row)
                                raise SyntaxError(f'Can not get expiration from underlying_index '
                                                  f'{row[OCl.UNDERLYING_CODE.nm]}')
                    row[OCl.OPTION_TYPE.nm] = option_type
                    row[OCl.STRIKE.nm] = strike
                    row[OCl.EXPIRATION_DATE.nm] = expiration_date
                    row[OCl.ASSET_TYPE.nm] = kind
                    row[OCl.UNDERLYING_EXPIRATION_DATE.nm] = future_expiration_date
                    if row['base_currency'] == row['quote_currency'] and 'estimated_delivery_price' in row and \
                        row['estimated_delivery_price']:
                        if row[OCl.PRICE.nm]:
                            row[OCl.PRICE.nm] *= row['estimated_delivery_price']
                        for col in COLUMNS_TO_CURRENCY:
                            if col in row:
                                row[f'{AbstractExchange.SOURCE_PREFIX}_{col}'] = row[col]
                                if row[col]:
                                    row[col] *= row['estimated_delivery_price']
                        if OCl.VOLUME_NOTIONAL.nm in row and 'volume_usd' in row and \
                            pd.isna(row[OCl.VOLUME_NOTIONAL.nm]):
                            row[OCl.VOLUME_NOTIONAL.nm] = row['volume_usd']
                    return row
                case _:
                    raise SyntaxError(f'Can parse instrument_name {row[OCl.ASSET_CODE.nm]}')
        except SyntaxError as err:
            raise err

    def _normalize_book(self, book_summary_df: pd.DataFrame,
                        request_timestamp: pd.Timestamp) -> pd.DataFrame:
        if book_summary_df.empty:
            return book_summary_df
        book_summary_df[OCl.REQUEST_TIMESTAMP.nm] = request_timestamp
        rename_columns = {'creation_timestamp': OCl.ORIGINAL_TIMESTAMP.nm,
                          'instrument_name': OCl.ASSET_CODE.nm,
                          'underlying_index': OCl.UNDERLYING_CODE.nm,
                          'underlying_price': OCl.UNDERLYING_PRICE.nm,
                          'mark_price': OCl.EXCHANGE_PRICE.nm,
                          'mark_iv': OCl.EXCHANGE_IV.nm,
                          'ask_price': OCl.ASK.nm,
                          'bid_price': OCl.BID.nm,
                          'last': OCl.LAST.nm,
                          'high': OCl.HIGH_24.nm,
                          'low': OCl.LOW_24.nm,
                          }
        book_summary_df.rename(columns=rename_columns, inplace=True)
        book_summary_df = df_columns_to_timestamp(book_summary_df, columns=[OCl.ORIGINAL_TIMESTAMP.nm], unit='ms')
        book_summary_df[OCl.TIMESTAMP.nm] = book_summary_df[OCl.ORIGINAL_TIMESTAMP.nm].copy()
        book_summary_df = normalize_timestamp(book_summary_df, columns=[OCl.TIMESTAMP.nm], freq='1s')
        book_summary_df = fill_option_price(book_summary_df)
        book_summary_df = book_summary_df.apply(self._kind_enrichment, axis='columns', result_type='expand')
        return book_summary_df


class DeribitExchange(AbstractExchange):
    """Deribit exchange api"""
    PRODUCT_API_URL: str = 'https://www.deribit.com/api/v2'
    TEST_API_URL: str = 'https://test.deribit.com/api/v2'
    CURRENCIES = ['BTC', 'ETH', 'USDC', 'USDT', 'EURR']
    TASKS_LIMIT: int = 4

    def __init__(self, engine: DataEngine = DataEngine.PANDAS, api_url: str | None = None):
        """Init"""
        api_url = api_url if api_url else self.PRODUCT_API_URL
        super().__init__(engine, ExchangeCode.DERIBIT.name, api_url=api_url)
        self.market = DeribitMarket(self.client)

    def get_assets_list(self, asset_kind: AssetKind) -> list[str]:
        """

        :param asset_kind:
        :return:

        ['BTCDVOL_USDC', 'ETH_USDC', ..., 'BNB_USDC', 'BTC_USD', 'ETH_USD',..., 'USDC_USDT', 'USDE_USDC', 'USDE_USDT', 'USYC_USDC']
        """
        symbols_df = self.market.get_instruments()
        return [symbol.upper() for symbol in symbols_df['price_index'].unique()]

    def get_options_assets_books_snapshot(self, asset_codes: list[str] | str | None = None) -> pd.DataFrame:
        """Get all option snapshot
             ask base_currency         bid  current_funding  estimated_delivery_price  exchange_price     exchange_symbol expiration_date    funding_8h  high_24 kind        last   low_24  \
0     94671.0000           BTC  94620.0000              0.0                94644.5656      94647.9745  BTC_USDT-PERPETUAL             NaT -1.000000e-08  95415.0    f  95401.0000  94726.0
1      1803.6000           ETH   1802.5000              0.0                 1803.4189       1803.0898  ETH_USDT-PERPETUAL             NaT  1.200000e-05      NaN    f   1725.0000      NaN
2      1629.8600         STETH         NaN              NaN                 1799.1453       1799.1453          STETH_USDT             NaT           NaN      NaN    s   1666.1400      NaN
3            NaN          USDE      1.0995              NaN                    0.9987          0.9987           USDE_USDT             NaT           NaN      NaN    s      1.0998      NaN
4     46757.0000           BTC  44020.0000              NaN                94644.5656      94644.5656            BTC_USDT             NaT           NaN  46757.0    s  46757.0000  46757.0
...          ...           ...         ...              ...                       ...             ...                 ...             ...           ...      ...  ...         ...      ...
5463         NaN          USDC      1.0000              NaN                    0.9997          0.9997           USDC_USDT             NaT           NaN      NaN    s      1.0000      NaN
5464  55069.0000           BTC  45000.0000              NaN                94662.8465      94662.8465            BTC_USDC             NaT           NaN  55069.0    s  55069.0000  55069.0
5465         NaN          USDE      1.1272              NaN                    0.9994          0.9994           USDE_USDC             NaT           NaN      NaN    s      1.1000      NaN
5466    615.3000           BNB    590.7000              NaN                  602.7461        602.7461            BNB_USDC             NaT           NaN      NaN    s    615.3000      NaN
5467   1408.6726         STETH   1200.0000              NaN                 1796.6857       1796.6857          STETH_USDC             NaT           NaN      NaN    s   1900.0000      NaN

       mid_price  open_interest               original_timestamp       price  price_change quote_currency                request_timestamp      symbol                 timestamp  volume  \
0     94645.5000        230.406 2025-04-30 02:54:25.292000+00:00  95401.0000        0.7126           USDT 2025-04-30 02:55:02.002339+00:00    BTC_USDT 2025-04-30 02:54:25+00:00   6.861
1      1803.0500       1746.840 2025-04-30 02:54:25.292000+00:00   1725.0000           NaN           USDT 2025-04-30 02:55:02.002339+00:00    ETH_USDT 2025-04-30 02:54:25+00:00   0.000
2            NaN            NaN 2025-04-30 02:54:25.292000+00:00   1666.1400           NaN           USDT 2025-04-30 02:55:02.002339+00:00  STETH_USDT 2025-04-30 02:54:25+00:00   0.000
3            NaN            NaN 2025-04-30 02:54:25.292000+00:00      1.0998           NaN           USDT 2025-04-30 02:55:02.002339+00:00   USDE_USDT 2025-04-30 02:54:25+00:00   0.000
4     45388.5000            NaN 2025-04-30 02:54:25.292000+00:00  46757.0000        0.0000           USDT 2025-04-30 02:55:02.002339+00:00    BTC_USDT 2025-04-30 02:54:25+00:00   1.000
...          ...            ...                              ...         ...           ...            ...                              ...         ...                       ...     ...
5463         NaN            NaN 2025-04-30 02:54:25.413000+00:00      1.0000           NaN           USDT 2025-04-30 02:55:02.001275+00:00   USDC_USDT 2025-04-30 02:54:25+00:00   0.000
5464  50034.5000            NaN 2025-04-30 02:54:25.413000+00:00  55069.0000        0.0000           USDC 2025-04-30 02:55:02.001275+00:00    BTC_USDC 2025-04-30 02:54:25+00:00   0.004
5465         NaN            NaN 2025-04-30 02:54:25.413000+00:00      1.1000           NaN           USDC 2025-04-30 02:55:02.001275+00:00   USDE_USDC 2025-04-30 02:54:25+00:00   0.000
5466    603.0000            NaN 2025-04-30 02:54:25.413000+00:00    615.3000           NaN           USDC 2025-04-30 02:55:02.001275+00:00    BNB_USDC 2025-04-30 02:54:25+00:00   0.000
5467   1304.3363            NaN 2025-04-30 02:54:25.413000+00:00   1900.0000           NaN           USDC 2025-04-30 02:55:02.001275+00:00  STETH_USDC 2025-04-30 02:54:25+00:00   0.000

      volume_notional  volume_usd  exchange_iv exchange_underlying_symbol  interest_rate option_type  source_ask  source_bid  source_exchange_price  source_high_24  source_last  source_low_24  \
0          653849.695   654101.75          NaN                        NaN            NaN         NaN         NaN         NaN                    NaN             NaN          NaN            NaN
1               0.000        0.00          NaN                        NaN            NaN         NaN         NaN         NaN                    NaN             NaN          NaN            NaN
2               0.000        0.00          NaN                        NaN            NaN         NaN         NaN         NaN                    NaN             NaN          NaN            NaN
3               0.000        0.00          NaN                        NaN            NaN         NaN         NaN         NaN                    NaN             NaN          NaN            NaN
4           46757.000    46773.36          NaN                        NaN            NaN         NaN         NaN         NaN                    NaN             NaN          NaN            NaN
...               ...         ...          ...                        ...            ...         ...         ...         ...                    ...             ...          ...            ...
5463            0.000        0.00          NaN                        NaN            NaN         NaN         NaN         NaN                    NaN             NaN          NaN            NaN
5464          220.276      220.28          NaN                        NaN            NaN         NaN         NaN         NaN                    NaN             NaN          NaN            NaN
5465            0.000        0.00          NaN                        NaN            NaN         NaN         NaN         NaN                    NaN             NaN          NaN            NaN
5466            0.000        0.00          NaN                        NaN            NaN         NaN         NaN         NaN                    NaN             NaN          NaN            NaN
5467            0.000        0.00          NaN                        NaN            NaN         NaN         NaN         NaN                    NaN             NaN          NaN            NaN

      strike underlying_expiration_date  underlying_price
0        NaN                        NaT               NaN
1        NaN                        NaT               NaN
2        NaN                        NaT               NaN
3        NaN                        NaT               NaN
4        NaN                        NaT               NaN
...      ...                        ...               ...
5463     NaN                        NaT               NaN
5464     NaN                        NaT               NaN
5465     NaN                        NaT               NaN
5466     NaN                        NaT               NaN
5467     NaN                        NaT               NaN
        """
        if asset_codes is None:
            asset_codes = self.CURRENCIES
        elif isinstance(asset_codes, str):
            asset_codes = [asset_codes]
        if len(asset_codes) == 1:
            book_summary_df = self.market.get_book_summary_by_currency(currency=asset_codes[0])
        else:
            books = []
            with ThreadPoolExecutor(max_workers=self.TASKS_LIMIT) as executor:
                job_results = {executor.submit(self.market.get_book_summary_by_currency, currency): currency
                               for currency in asset_codes}
                for job_res in concurrent.futures.as_completed(job_results):
                    book_summary_df: pd.DataFrame | Exception = job_res.result()
                    if isinstance(book_summary_df, Exception):
                        currency = job_results[job_res]
                        print(f'[ERROR] for {currency} book summary: {book_summary_df}')
                        raise book_summary_df
                    books.append(book_summary_df)
            book_summary_df = pd.concat(books, ignore_index=True) if len(books) > 1 else books[0]
        return book_summary_df

    def load_option_history(self, symbol: str, params: RequestParameters | None = None,
                            columns: list | None = None) -> pd.DataFrame:
        """load options history."""
        raise NotImplementedError

    def load_future_history(self, symbol: str, params: RequestParameters | None = None,
                            columns: list | None = None) -> pd.DataFrame:
        """load futures history"""
        raise NotImplementedError

    def load_future_book(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                         timeframe: Timeframe = Timeframe.EOD, columns: list | None = None) -> pd.DataFrame:
        raise NotImplementedError

    def load_option_book(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                         timeframe: Timeframe = Timeframe.EOD, columns: list | None = None) -> pd.DataFrame:
        raise NotImplementedError

    def load_option_chain(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                          expiration_date: datetime.datetime | None = None,
                          timeframe: Timeframe = Timeframe.EOD,
                          columns: list | None = None
                          ) -> pd.DataFrame | None:
        """Providing option chain by local file system is not supported return None"""
        raise NotImplementedError
