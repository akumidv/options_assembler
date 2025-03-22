"""Deribit ETL module"""
import datetime
import pandas as pd
from dataclasses import dataclass
from options_etl.etl_class import EtlOptions, AssetBookData, SaveTask
from options_assembler.entities import (
    Timeframe,
    OptionColumns as OCl,
    OPTION_NON_SPOT_COLUMN_NAMES, OPTION_NON_FUTURES_COLUMN_NAMES
)
from exchange import DeribitExchange, DeribitAssetKind, DERIBIT_COLUMNS_TO_CURRENCY
from messanger import AbstractMessanger


@dataclass
class DeribitAssetBookData(AssetBookData):
    """Asset book data for timeframe"""
    future_combo: pd.DataFrame | None
    option_combo: pd.DataFrame | None


class EtlDeribit(EtlOptions):
    """Deribit ETL process
    Updates in store size for timeframes (2025 year, x5 in memory)
    1m timeframe ~750Mb a day, 22.5 Gb a month 275 Gb a year in store
    5m timeframe ~ 150Mb a day, 4.5Gb a month, 60Gb a year
    1h timeframe ~ 11.5Mb a day, 350Mb a month, 4.5Gb a year
    """

    def __init__(self, exchange: DeribitExchange, asset_names: list[str] | str | None, timeframe: Timeframe,
                 update_data_path: str, timeframe_cron: dict | None = None, messanger: AbstractMessanger | None = None,
                 is_detailed: bool = False):
        super().__init__(exchange, asset_names, timeframe, update_data_path, timeframe_cron, messanger, is_detailed)

    @staticmethod
    def _drop_service_or_doublet_columns(df: pd.DataFrame) -> pd.DataFrame:
        drop_columns = []
        for col in df.columns:
            if col.startswith(f'{DeribitExchange.SOURCE_PREFIX}_') and df[col].isnull().all():
                drop_columns.append(col)
        if len(drop_columns) > 0:
            df.drop(columns=drop_columns, inplace=True)
        return df

    def get_symbols_books_snapshot(self, asset_name: list[str] | str | None,
                                   request_timestamp: pd.Timestamp | None = None) -> DeribitAssetBookData:
        """Load deribit option and future"""
        if request_timestamp is None:
            request_timestamp = pd.Timestamp.now(tz=datetime.UTC)
        book_summary_df = self.exchange.get_symbols_books_snapshot(asset_name)
        if book_summary_df is None or book_summary_df.empty:
            return DeribitAssetBookData(asset_name=asset_name, request_timestamp=request_timestamp, option=None,
                                        future=None, spot=None, future_combo=None, option_combo=None)
        book_summary_df[OCl.REQUEST_TIMESTAMP.nm] = request_timestamp
        options_df = book_summary_df[book_summary_df[OCl.KIND.nm] == DeribitAssetKind.OPTION.code].reset_index(
            drop=True)
        options_combo_df = book_summary_df[
            book_summary_df[OCl.KIND.nm] == DeribitAssetKind.OPTION_COMBO.code].reset_index(drop=True)
        future_columns = [col for col in book_summary_df.columns if col not in OPTION_NON_FUTURES_COLUMN_NAMES]
        future_df = book_summary_df[book_summary_df[OCl.KIND.nm] == DeribitAssetKind.FUTURE.code][future_columns]
        future_df = self._drop_service_or_doublet_columns(future_df)
        future_combo_df = book_summary_df[book_summary_df[OCl.KIND.nm] == DeribitAssetKind.FUTURE_COMBO.code
                                          ][future_columns]
        future_combo_df = self._drop_service_or_doublet_columns(future_combo_df)
        spot_columns = [col for col in book_summary_df.columns if col not in OPTION_NON_SPOT_COLUMN_NAMES]
        spot_df = book_summary_df[book_summary_df[OCl.KIND.nm] == DeribitAssetKind.SPOT.code][spot_columns]
        spot_df = self._drop_service_or_doublet_columns(spot_df)
        return DeribitAssetBookData(asset_name=asset_name, request_timestamp=request_timestamp,
                                    option=options_df if not options_df.empty else None,
                                    future=future_df if not future_df.empty else None,
                                    spot=spot_df if not spot_df.empty else None,
                                    future_combo=future_combo_df if not future_combo_df.empty else None,
                                    option_combo=options_combo_df if not options_combo_df.empty else None
                                    )

    def _add_save_task_to_background_to_asset_name(self, df: pd.DataFrame, asset_kind: DeribitAssetKind,
                                                   request_datetime: pd.Timestamp):
        asset_name = df.iloc[0][OCl.SYMBOL.nm]
        save_path = self.get_timeframe_update_path(asset_name, asset_kind, request_datetime)
        self.add_save_task_to_background(SaveTask(save_path, df.copy()))

    def _save_timeframe_book_update(self, book_data: DeribitAssetBookData):
        """ Save book data"""
        fabric = {'option': DeribitAssetKind.OPTION,
                  'future': DeribitAssetKind.FUTURE,
                  'spot': DeribitAssetKind.SPOT,
                  'future_combo': DeribitAssetKind.FUTURE_COMBO,
                  'option_combo': DeribitAssetKind.OPTION_COMBO
                  }
        request_datetime = book_data.request_timestamp
        for asset_kind_attr in fabric:
            df = getattr(book_data, asset_kind_attr)
            if df is not None:
                df.groupby(OCl.SYMBOL.nm, group_keys=False) \
                    .apply(self._add_save_task_to_background_to_asset_name, fabric[asset_kind_attr],
                           request_datetime, include_groups=True)
                setattr(book_data, asset_kind_attr, None)
