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
    FuturesColumns as FCl,
    SpotColumns as SCl,
    ALL_COLUMN_NAMES
)
from option_lib.normalization.datetime_conversion import df_columns_to_timestamp
from option_lib.provider import DataEngine, RequestParameters
from exchange.exchange_entities import ExchangeCode
from exchange._abstract_exchange import AbstractExchange, RequestClass
from option_lib.normalization import parse_expiration_date, normalize_timestamp, fill_option_price


class DeribitAssetKind(EnumCode):
    """Deribit instrument kinds"""
    FUTURE = AssetKind.FUTURE.value, AssetKind.FUTURE.code
    OPTION = AssetKind.OPTION.value, AssetKind.OPTION.code
    SPOT = AssetKind.SPOT.value, AssetKind.SPOT.code
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
        request_timestamp = pd.Timestamp.now(tz=datetime.UTC)
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

                        under_arr = row[OCl.EXCHANGE_UNDERLYING_SYMBOL.nm].split('-')
                        if len(under_arr) == 2:
                            future_expiration_date = parse_expiration_date(under_arr[1])
                        else:
                            if row[OCl.EXCHANGE_UNDERLYING_SYMBOL.nm] in ['SYN.EXPIRY',  # Expired already
                                                                          'index_price']:  # index price
                                future_expiration_date = None
                            else:
                                print('Syntax error in row:\n', row)
                                raise SyntaxError(f'Can not get expiration from underlying_index '
                                                  f'{row[OCl.EXCHANGE_UNDERLYING_SYMBOL.nm]}')
                    row[OCl.OPTION_TYPE.nm] = option_type
                    row[OCl.STRIKE.nm] = strike
                    row[OCl.EXPIRATION_DATE.nm] = expiration_date
                    row[OCl.KIND.nm] = kind
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
                    raise SyntaxError(f'Can parse instrument_name {row[OCl.EXCHANGE_SYMBOL.nm]}')
        except SyntaxError as err:
            raise err

    def _normalize_book(self, book_summary_df: pd.DataFrame,
                        request_timestamp: pd.Timestamp) -> pd.DataFrame:
        if book_summary_df.empty:
            return book_summary_df
        book_summary_df[OCl.REQUEST_TIMESTAMP.nm] = request_timestamp
        rename_columns = {'creation_timestamp': OCl.ORIGINAL_TIMESTAMP.nm,
                          'instrument_name': OCl.EXCHANGE_SYMBOL.nm,
                          'underlying_index': OCl.EXCHANGE_UNDERLYING_SYMBOL.nm,
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

    def get_symbols_list(self, asset_kind: AssetKind) -> list[str]:
        symbols_df = self.market.get_instruments()
        return [symbol.upper() for symbol in symbols_df['price_index'].unique()]

    def get_symbols_books_snapshot(self, symbols: list[str] | str | None = None) -> pd.DataFrame:
        """Get all option snapshot"""
        if symbols is None:
            symbols = self.CURRENCIES
        elif isinstance(symbols, str):
            symbols = [symbols]
        if len(symbols) == 1:
            book_summary_df = self.market.get_book_summary_by_currency(currency=symbols[0])
        else:
            books = []
            with ThreadPoolExecutor(max_workers=self.TASKS_LIMIT) as executor:
                job_results = {executor.submit(self.market.get_book_summary_by_currency, currency): currency
                               for currency in symbols}
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
