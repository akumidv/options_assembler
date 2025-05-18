"""MOEX ETL module"""
import datetime
import pandas as pd
from options_etl.etl_class import EtlOptions, AssetBookData, SaveTask
from option_lib.entities import (
    AssetKind,
    AssetType,
    Timeframe,
    OptionColumns as OCl,
    FutureColumns as FCl,
    SpotColumns as SCl,
)
from exchange import MoexExchange
from messanger import AbstractMessanger


class EtlMoex(EtlOptions):
    """Deribit ETL process
    Updates in store size for timeframes (2025 year, x5 in memory)
    1m timeframe ~750Mb a day, 22.5 Gb a month 275 Gb a year in store
    5m timeframe ~ 150Mb a day, 4.5Gb a month, 60Gb a year
    1h timeframe ~ 11.5Mb a day, 350Mb a month, 4.5Gb a year
    """

    def __init__(self, exchange: MoexExchange, asset_names: list[str] | str | None, timeframe: Timeframe,
                 update_data_path: str, timeframe_cron: dict | str | None = None, messanger: AbstractMessanger | None = None,
                 is_detailed: bool = False):
        super().__init__(exchange, asset_names, timeframe, update_data_path, timeframe_cron, messanger, is_detailed)

    @staticmethod
    def _drop_service_or_doublet_columns(df: pd.DataFrame) -> pd.DataFrame:
        drop_columns = []
        for col in df.columns:
            if col.startswith(f'{MoexExchange.SOURCE_PREFIX}_') and df[col].isnull().all():
                drop_columns.append(col)
        if len(drop_columns) > 0:
            df.drop(columns=drop_columns, inplace=True)
        return df

    def get_symbols_books_snapshot(self, asset_name: list[str] | str | None,
                                   request_timestamp: pd.Timestamp | None = None) -> AssetBookData:
        """Load deribit option and future"""
        if request_timestamp is None:
            request_timestamp = pd.Timestamp.now(tz=datetime.UTC)
        book_summary_df = self.exchange.get_options_assets_books_snapshot(asset_name)
        if book_summary_df is None or book_summary_df.empty:
            return AssetBookData(asset_name=asset_name, request_timestamp=request_timestamp, option=None,
                                 future=None, spot=None, )
        book_summary_df[OCl.REQUEST_TIMESTAMP.nm] = request_timestamp
        options_df = book_summary_df[book_summary_df[OCl.ASSET_TYPE.nm] == AssetKind.OPTION.code].reset_index(
            drop=True)
        future_columns = [OCl.TIMESTAMP.nm, OCl.BASE_CODE.nm, OCl.UNDERLYING_CODE.nm, OCl.UNDERLYING_TYPE.nm,
                          OCl.UNDERLYING_EXPIRATION_DATE.nm, OCl.UNDERLYING_PRICE.nm]
        futures_mask = book_summary_df[OCl.UNDERLYING_TYPE.nm] == AssetType.FUTURE.code
        future_df = book_summary_df[futures_mask][future_columns] \
            .drop_duplicates(subset=[OCl.UNDERLYING_CODE.nm]) \
            .rename(columns={OCl.UNDERLYING_CODE.nm: FCl.ASSET_CODE.nm, OCl.UNDERLYING_TYPE.nm: FCl.ASSET_TYPE.nm,
                             OCl.UNDERLYING_EXPIRATION_DATE.nm: FCl.EXPIRATION_DATE.nm,
                             OCl.UNDERLYING_PRICE.nm: FCl.PRICE.nm})
        future_df = self._drop_service_or_doublet_columns(future_df)
        spot_columns = [OCl.TIMESTAMP.nm, OCl.UNDERLYING_CODE.nm, OCl.UNDERLYING_TYPE.nm, OCl.UNDERLYING_PRICE.nm]
        spot_df = book_summary_df[~futures_mask][spot_columns] \
            .drop_duplicates(subset=[OCl.UNDERLYING_CODE.nm, OCl.UNDERLYING_TYPE.nm]) \
            .rename(columns={OCl.UNDERLYING_CODE.nm: SCl.ASSET_CODE.nm, OCl.UNDERLYING_TYPE.nm: SCl.ASSET_TYPE.nm,
                             OCl.UNDERLYING_PRICE.nm: SCl.PRICE.nm})
        spot_df = self._drop_service_or_doublet_columns(spot_df)

        return AssetBookData(asset_name=asset_name, request_timestamp=request_timestamp,
                             option=options_df if not options_df.empty else None,
                             future=future_df if not future_df.empty else None,
                             spot=spot_df if not spot_df.empty else None,
                             )

    def _add_save_task_to_background_to_asset_name(self, df: pd.DataFrame, asset_kind: AssetKind,
                                                   request_datetime: pd.Timestamp):
        asset_name = df.iloc[0][(OCl.BASE_CODE.nm if asset_kind != AssetKind.SPOT else OCl.ASSET_CODE.nm)]
        save_path = self.get_timeframe_update_path(asset_name, asset_kind, request_datetime)
        self.add_save_task_to_background(SaveTask(save_path, df.copy()))

    def _save_timeframe_book_update(self, book_data: AssetBookData):
        """ Save book data"""

        fabric = {'option': AssetKind.OPTION,
                  'future': AssetKind.FUTURE,
                  'spot': AssetKind.SPOT,
                  }
        request_datetime = book_data.request_timestamp
        for asset_kind_attr in fabric:
            df = getattr(book_data, asset_kind_attr)
            if df is not None:
                df.groupby(OCl.BASE_CODE.nm if fabric[asset_kind_attr] != AssetKind.SPOT else OCl.ASSET_CODE.nm,
                           group_keys=False) \
                    .apply(self._add_save_task_to_background_to_asset_name, asset_kind=fabric[asset_kind_attr],
                           request_datetime=request_datetime, include_groups=True)
                setattr(book_data, asset_kind_attr, None)
