"""Deribit ETL module"""
import datetime
import pandas as pd
from option_lib.etl.etl_class import EtlOptions, AssetBookData
from option_lib.entities import (
    Timeframe, AssetKind,
    OptionColumns as OCl,
    OPTION_NON_SPOT_COLUMN_NAMES, OPTION_NON_FUTURES_COLUMN_NAMES
)
from option_lib.provider.exchange import DeribitExchange, DeribitAssetKind


class DeribitAssetBookData(AssetBookData):
    """Asset book data for timeframe"""
    request_datetime: datetime.datetime
    option_combo: pd.DataFrame | None
    future_combo: pd.DataFrame | None


class EtlDeribit(EtlOptions):
    """Deribit ETL process"""

    def __init__(self, exchange: DeribitExchange, asset_names: list[str] | None, timeframe: Timeframe, data_path: str):
        super().__init__(exchange, asset_names, timeframe, data_path)

    def get_symbols_books_snapshot(self, asset_name: str, request_datetime: datetime.datetime) -> AssetBookData:
        """Load deribit option and future"""
        book_summary_df = self.exchange.get_symbols_books_snapshot(asset_name)
        if book_summary_df is None or book_summary_df.empty:
            return AssetBookData(asset_name=asset_name, request_datetime=request_datetime, option=None,
                                 future=None, spot=None)
        options_df = book_summary_df[book_summary_df[OCl.KIND.nm] == DeribitAssetKind.OPTION.code].reset_index(
            drop=True)
        options_combo_df = book_summary_df[
            book_summary_df[OCl.KIND.nm] == DeribitAssetKind.OPTION_COMBO.code].reset_index(drop=True)
        future_columns = [col for col in book_summary_df.columns if col not in OPTION_NON_FUTURES_COLUMN_NAMES]
        future_df = book_summary_df[book_summary_df[OCl.KIND.nm]==DeribitAssetKind.FUTURE.code][future_columns]
        futures_combo_df = book_summary_df[book_summary_df[OCl.KIND.nm]==DeribitAssetKind.FUTURE_COMBO.code
                                           ][future_columns]
        spot_columns = [col for col in book_summary_df.columns if col not in OPTION_NON_SPOT_COLUMN_NAMES]
        spot_df = book_summary_df[book_summary_df[OCl.KIND.nm] == DeribitAssetKind.SPOT.code][spot_columns]

        return DeribitAssetBookData(asset_name=asset_name, request_datetime=request_datetime,
                                    option=options_df if not options_df.empty else None,
                                    futures=future_df if not future_df.empty else None,
                                    spot=spot_df if not spot_df.empty else None,
                                    options_combo=options_combo_df if not options_combo_df.empty else None,
                                    futures_combo=futures_combo_df if not futures_combo_df.empty else None)

    def _save_timeframe_book_update(self, book_data: AssetBookData):
        """ "Save book data
        TODO SHOULD BE IMPLEMENTED FOR DERIBIT DIFFERENT DUE asset is currency adn not symbol name
        so symbol names should be extracted from dataframe (group by?) """

        # TODO get asset names from dataframe and save them by grouping
        raise NotImplementedError
