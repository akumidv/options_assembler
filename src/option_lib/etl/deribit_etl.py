import datetime
import os

from option_lib.etl.etl_class import EtlOptions, AssetBookData
from option_lib.entities import TimeframeCode, AssetType
from option_lib.provider.exchange import DeribitExchange


class EtlDeribit(EtlOptions):
    """Deribit ETL process"""
    def __init__(self, exchange: DeribitExchange, asset_names: list[str] | None, timeframe: TimeframeCode, data_path: str):
        super().__init__(exchange, asset_names, timeframe, data_path)

    def get_symbols_books_snapshot(self, timeframe_datetime) -> AssetBookData:
        """Load deribit option and future"""
        print('TIMEFRAME', self._timeframe, datetime.datetime.now())
        book_summary_df = self.exchange.get_symbols_books_snapshot()
        print(book_summary_df)
        # https://docs.deribit.com/#public-get_book_summary_by_currency
        # TODO convert from book_summary_df to list of AssetBookData
        asset_name = 'test'
        return AssetBookData(asset_name=asset_name, request_datetime=timeframe_datetime, option=None, future=None)

    def __save_timeframe_book_update(self,  book_data: AssetBookData):
        """ TODO SHOULD BE IMPLEMENTED FOR DERIBUT DIFFERENT DUE asset is currency adn not symbol name
        so symbol names should be extracted from dataframe (group by?) """
        """Save book data"""
        # TODO get asset names from dataframe and save them by grouping
        opt_path = self.get_timeframe_update_path(book_data.asset_name, AssetType.OPTION, book_data.request_datetime)
        self.add_save_task_to_background(opt_path, book_data.option)
        book_data.option = None
        fut_path = self.get_timeframe_update_path(book_data.asset_name, AssetType.FUTURE, book_data.request_datetime)
        self.add_save_task_to_background(fut_path, book_data.future)
        book_data.future = None


if __name__ == '__main__':
    # TODO инициализация которая вызывае метод получения списка символов для загрузки и
    #  сохраняет их в олкальной переменной этот список обновляется в 0:0UTC
    update_data_path = os.environ.get('DATA_PATH', './data')
    deribit_exchange = DeribitExchange()
    etl_deribit = EtlDeribit(deribit_exchange ,DeribitExchange.CURRENCIES, TimeframeCode.MINUTE_1, update_data_path)
    etl_deribit.print_etl()
    etl_deribit.start()
