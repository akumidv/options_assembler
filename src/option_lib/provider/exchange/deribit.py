"""
Deribit api provider
"""
import datetime
import pandas as pd
from option_lib.entities import TimeframeCode, AssetType
from option_lib.provider._provider_entities import DataEngine, RequestParameters
from option_lib.provider.exchange.exchange_entities import ExchangeCode
from option_lib.provider.exchange._abstract_exchange import AbstractExchange, RequestClass


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

    def get_book_summary_by_currency(self, currency: str, kind: str | None = None) -> pd.DataFrame:
        """Retrieves the summary information for all instruments for the currency (optionally filtered by kind).
        https://docs.deribit.com/#public-get_book_summary_by_currency"""
        params = {'currency': currency}
        if isinstance(kind, str):
            params['kind'] = kind
        response = self.client.request_api('/public/get_book_summary_by_currency', params=params)
        book_summary_df = pd.DataFrame(response['result'])
        return book_summary_df


class DeribitExchange(AbstractExchange):
    """Deribit exchange api"""
    PRODUCT_API_URL: str = 'https://www.deribit.com/api/v2'
    TEST_API_URL: str = 'https://test.deribit.com/api/v2'
    CURRENCIES = ['BTC', 'ETH', 'USDC', 'USDT', 'EURR']

    def __init__(self, engine: DataEngine = DataEngine.PANDAS, api_url: str | None = None):
        """Init"""
        api_url = api_url if api_url else self.PRODUCT_API_URL
        super().__init__(engine, ExchangeCode.DERIBIT.value, api_url=api_url)
        self.market = DeribitMarket(self.client)

    def get_symbols_list(self, asset_type: AssetType) -> list[str]:
        symbols_df = self.market.get_instruments()
        return [symbol.upper() for symbol in symbols_df['price_index'].unique()]

    def get_symbols_books_snapshot(self, symbols: list[str] | None = None):
        """Get all option snapshot"""
        if symbols is None:
            symbols = self.CURRENCIES
        books = []
        for currency in symbols:
            book_summary_df = self.market.get_book_summary_by_currency(currency=currency)
            books.append(book_summary_df)
        book_summary_df = pd.concat(books, ignore_index=True)
        return book_summary_df

    def load_option_history(self, symbol: str, params: RequestParameters | None = None,
                            columns: list | None = None) -> pd.DataFrame:
        """load options history."""

    def load_future_history(self, symbol: str, params: RequestParameters | None = None,
                            columns: list | None = None) -> pd.DataFrame:
        """load futures history"""

    def load_future_book(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                         timeframe: TimeframeCode = TimeframeCode.EOD, columns: list | None = None) -> pd.DataFrame:
        pass

    def load_option_book(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                         timeframe: TimeframeCode = TimeframeCode.EOD, columns: list | None = None) -> pd.DataFrame:
        pass

    def load_option_chain(self, settlement_datetime: datetime.datetime | None = None,
                          expiration_date: datetime.datetime | None = None,
                          timeframe: TimeframeCode = TimeframeCode.EOD,
                          columns: list | None = None
                          ) -> pd.DataFrame | None:
        """Providing option chain by local file system is not supported return None"""
