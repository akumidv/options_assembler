"""
Deribit api provider
"""
import datetime
import re
import pandas as pd
import numpy as np
from option_lib.entities.enum_code import EnumCode
from option_lib.entities import OptionType
from option_lib.entities import Timeframe, AssetKind
from option_lib.entities import (
    OptionColumns as OCl,
    FuturesColumns as FCl,
    SpotColumns as SCl
)
from option_lib.provider._provider_entities import DataEngine, RequestParameters
from option_lib.provider.exchange.exchange_entities import ExchangeCode
from option_lib.provider.exchange._abstract_exchange import AbstractExchange, RequestClass
from option_lib.normalization.datetime_conversion import parse_expiration_date


class DeribitAssetKind(EnumCode):
    """Deribit instrument kinds"""
    FUTURE = AssetKind.FUTURE.value, AssetKind.FUTURE.code
    OPTION = AssetKind.OPTION.value, AssetKind.OPTION.code
    SPOT = AssetKind.SPOT.value, AssetKind.SPOT.code
    FUTURE_COMBO = 'future_combo', 'fc'
    OPTION_COMBO = 'option_combo', 'oc'


DOT_STRIKE_REGEXP = re.compile(r'(\d)d(\d)', flags=re.IGNORECASE)


class DeribitMarket:
    """Deribit Market data api"""

    def __init__(self, client: RequestClass):
        self.client = client

    def get_instruments(self) -> pd.DataFrame:
        """Retrieves available trading instruments.
        https://docs.deribit.com/#public-get_instruments"""
        response = self.client.request_api('/public/get_instruments')
        symbols_df = pd.DataFrame(response['result'])
        return symbols_df

    def get_book_summary_by_currency(self, currency: str, kind: DeribitAssetKind | None = None) -> pd.DataFrame:
        """Retrieves the summary information for all instruments for the currency (optionally filtered by kind).
        https://docs.deribit.com/#public-get_book_summary_by_currency"""
        params = {'currency': currency}
        if kind is not None:
            params['kind'] = kind.value
        request_timestamp = pd.Timestamp.now(tz=datetime.timezone.utc)
        response = self.client.request_api('/public/get_book_summary_by_currency', params=params)
        book_summary_df = pd.DataFrame(response['result'])
        book_summary_df = self._normalize_book(book_summary_df, request_timestamp)
        return book_summary_df

    @staticmethod
    def _kind_enrichment(row: pd.Series) -> pd.Series:
        try:
            exchange_asset_symbol_arr = DOT_STRIKE_REGEXP.sub(r'\1.\2', row[OCl.EXCHANGE_SYMBOL.nm]).split(
                '-')  # for strike DOGE_USDC-7FEB25-0d4064-C  or 3d12
            symbol = exchange_asset_symbol_arr[0]
            row = row.copy(deep=True)
            match len(exchange_asset_symbol_arr):
                case 1:  # SPOT
                    row[SCl.SYMBOL.nm] = symbol
                    row[SCl.KIND.nm] = DeribitAssetKind.SPOT.code
                    return row
                case 2:  # FUT
                    row[FCl.SYMBOL.nm] = symbol
                    expiration_date = parse_expiration_date(exchange_asset_symbol_arr[1])
                    if expiration_date is None and exchange_asset_symbol_arr[1] != 'PERPETUAL':
                        raise SyntaxError(f'Can not parse {exchange_asset_symbol_arr[1]}, '
                                          f'None expiration can be only for PERPETUAL: {row}')
                    row[FCl.EXPIRATION_DATE.nm] = expiration_date
                    row[FCl.KIND.nm] = DeribitAssetKind.FUTURE.code
                    return row
                case 3:  # FUT COMBO
                    # Second value is strategy for combo, for example FS - future spread
                    row[FCl.SYMBOL.nm] = symbol
                    row[FCl.EXPIRATION_DATE.nm] = parse_expiration_date(exchange_asset_symbol_arr[2].split('_')[0])
                    row[FCl.KIND.nm] = DeribitAssetKind.FUTURE_COMBO.code
                    return row
                case 4:  # OPT AND OPT COMBO
                    row[OCl.SYMBOL.nm] = symbol
                    row[OCl.PRICE.nm] = row[FCl.PRICE.nm]
                    row[FCl.PRICE.nm] = None
                    expiration_date = parse_expiration_date(exchange_asset_symbol_arr[1])
                    if expiration_date is None:  # OPT COMBO
                        # Second value is strategy for combo, for example PCOND - put condor, CBUT - call butterfly
                        expiration_date = parse_expiration_date(exchange_asset_symbol_arr[2])
                        kind = DeribitAssetKind.OPTION_COMBO.code
                        option_type = None
                        strike = None
                        futures_expiration_date = None
                    else:  # OPT
                        kind = DeribitAssetKind.OPTION.code
                        option_type = exchange_asset_symbol_arr[3]
                        if option_type not in ['C', 'P']:
                            raise SyntaxError(f'Unknown option type {option_type}')
                        option_type = OptionType.CALL.code if exchange_asset_symbol_arr[3] == 'C' else \
                            OptionType.PUT.code
                        strike = float(exchange_asset_symbol_arr[2])

                        under_arr = row[OCl.EXCHANGE_UNDERLYING_SYMBOL.nm].split('-')
                        if len(under_arr) == 2:
                            futures_expiration_date = parse_expiration_date(under_arr[1])
                        else:
                            if row[OCl.EXCHANGE_UNDERLYING_SYMBOL.nm] in ['SYN.EXPIRY',  # Expired already
                                                                          'index_price']:  # index price
                                futures_expiration_date = None
                            else:
                                print('Syntex error in row:\n', row)
                                raise SyntaxError(f'Can not get expiration from underlying_index '
                                                  f'{row[OCl.EXCHANGE_UNDERLYING_SYMBOL.nm]}')
                    row[OCl.OPTION_TYPE.nm] = option_type
                    row[OCl.STRIKE.nm] = strike
                    row[OCl.EXPIRATION_DATE.nm] = expiration_date
                    row[OCl.KIND.nm] = kind
                    row[OCl.UNDERLYING_EXPIRATION_DATE.nm] = futures_expiration_date
                    return row
                case _:
                    raise SyntaxError(f'Can parse instrument_name {row[OCl.EXCHANGE_SYMBOL.nm]}')
        except SyntaxError as err:
            raise err

    def _normalize_book(self, book_summary_df: pd.DataFrame, request_timestamp: pd.Timestamp) -> pd.DataFrame:  # BookData:
        book_summary_df[OCl.REQUEST_TIMESTAMP.nm] = request_timestamp
        book_summary_df.rename(columns={'creation_timestamp': OCl.TIMESTAMP.nm,
                                        'instrument_name': OCl.EXCHANGE_SYMBOL.nm,
                                        'last': FCl.PRICE.nm,
                                        'underlying_index': OCl.EXCHANGE_UNDERLYING_SYMBOL.nm,
                                        'underlying_price': OCl.UNDERLYING_PRICE.nm}, inplace=True)
        if OCl.TIMESTAMP.nm in book_summary_df.columns:
            book_summary_df[OCl.TIMESTAMP.nm] *= 1000
        book_summary_df.replace({np.nan: None}, inplace=True)
        # book_summary_df[COL_PREPARED_INSTRUMENT_NAME] = book_summary_df[OCl.EXCHANGE_SYMBOL.nm].str.replace(DOT_STRIKE_REGEXP, r'\1.\2', regex=True)\ # May be faster split there

        book_summary_df = book_summary_df.apply(self._kind_enrichment,
                                                axis='columns', result_type='expand')

        return book_summary_df


class DeribitExchange(AbstractExchange):
    """Deribit exchange api"""
    PRODUCT_API_URL: str = 'https://www.deribit.com/api/v2'
    TEST_API_URL: str = 'https://test.deribit.com/api/v2'
    CURRENCIES = ['BTC', 'ETH', 'USDC', 'USDT', 'EURR']

    def __init__(self, engine: DataEngine = DataEngine.PANDAS, api_url: str | None = None):
        """Init"""
        api_url = api_url if api_url else self.PRODUCT_API_URL
        super().__init__(engine, ExchangeCode.DERIBIT.name, api_url=api_url)
        self.market = DeribitMarket(self.client)

    def get_symbols_list(self, asset_kind: AssetKind) -> list[str]:
        symbols_df = self.market.get_instruments()
        return [symbol.upper() for symbol in symbols_df['price_index'].unique()]

    def get_symbols_books_snapshot(self, symbols: list[str] | str | None = None) -> pd.DataFrame:
        """Get all option snapshot"""
        if symbols is None:
            symbols = self.CURRENCIES
        elif isinstance(symbols, str):
            symbols = [symbols]
        books = []
        for currency in symbols:
            book_summary_df = self.market.get_book_summary_by_currency(currency=currency)
            books.append(book_summary_df)
        book_summary_df = pd.concat(books, ignore_index=True) if len(books) > 1 else books[0]
        return book_summary_df

    def load_option_history(self, symbol: str, params: RequestParameters | None = None,
                            columns: list | None = None) -> pd.DataFrame:
        """load options history."""

    def load_future_history(self, symbol: str, params: RequestParameters | None = None,
                            columns: list | None = None) -> pd.DataFrame:
        """load futures history"""

    def load_future_book(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                         timeframe: Timeframe = Timeframe.EOD, columns: list | None = None) -> pd.DataFrame:
        pass

    def load_option_book(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                         timeframe: Timeframe = Timeframe.EOD, columns: list | None = None) -> pd.DataFrame:
        pass

    def load_option_chain(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                          expiration_date: datetime.datetime | None = None,
                          timeframe: Timeframe = Timeframe.EOD,
                          columns: list | None = None
                          ) -> pd.DataFrame | None:
        """Providing option chain by local file system is not supported return None"""
