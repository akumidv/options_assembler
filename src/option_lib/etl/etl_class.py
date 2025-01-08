"""ETL class"""
import datetime
import os
from abc import ABC, abstractmethod
import asyncio
from typing import NamedTuple


import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
from option_lib.entities import TimeframeCode, AssetType
from option_lib.provider.exchange import AbstractExchange

class AssetBookData(NamedTuple):
    """Asset book data for timeframe"""
    asset_name: str
    request_datetime: datetime.datetime
    option: pd.DataFrame | None
    future: pd.DataFrame | None


class EtlOptions(ABC):
    """ETL Class"""
    TASKS_LIMIT = 4

    def __init__(self, exchange: AbstractExchange, asset_names: list[str] | None, timeframe: TimeframeCode, data_path: str):
        self.exchange = exchange
        self._data_path = data_path
        self._asset_names = asset_names
        self._timeframe: TimeframeCode = timeframe
        self._create_folders()
        timeframe_cron = self._get_cron_params_from_timeframe(timeframe)
        self._scheduler: BlockingScheduler = BlockingScheduler(timezone=datetime.UTC)
        self._scheduler.add_job(self._timeframe_job, 'cron', **timeframe_cron)


    @staticmethod
    def get_updates_folder(data_path: str, asset_name: str, asset_type: AssetType, timeframe: TimeframeCode) -> str:
        """Return path to folder where all update should be stored"""
        base_path = os.path.abspath(os.path.normpath(os.path.join(data_path, 'updates')))
        return f'{base_path}/{asset_name}/{asset_type.value}/{timeframe.value}'

    def _create_folders(self):
        """Create folders for save books"""
        if self._asset_names:
            for asset_name in self._asset_names:
                os.makedirs(self.get_updates_folder(self._data_path, asset_name, AssetType.FUTURE,
                                                    self._timeframe), exist_ok=True)
                os.makedirs(self.get_updates_folder(self._data_path, asset_name, AssetType.OPTION,
                                                    self._timeframe), exist_ok=True)

    def print_etl(self):
        """Print jobs"""
        print('Assets:', ','.join(self._asset_names))
        print('Timeframe:', self._timeframe.value)
        print('Stored path:', os.path.abspath(os.path.normpath(self._data_path)))
        self._scheduler.print_jobs()

    def start(self):
        """Start scheduled loading"""
        self._scheduler.start()

    @staticmethod
    def _get_cron_params_from_timeframe(timeframe: TimeframeCode):
        match timeframe:
            case TimeframeCode.EOD:
                return {'day': '*', 'hour': 0, 'minute': 0}
            case TimeframeCode.MINUTE_1:
                return {'hour': '*', 'minute': '*'}
            case TimeframeCode.HOUR_1:
                return {'hour': '', 'minute': '0'}
            case _:
                raise ValueError(f'Unknown timeframe  {timeframe.code}')

    async def _load_data(self, asset_name: str, request_datetime: datetime.datetime,
                         semaphore: asyncio.Semaphore) -> AssetBookData:
        async with semaphore:
            return await asyncio.to_thread(self.load_option_and_future_book, asset_name, request_datetime)

    def _timeframe_job(self):
        """Timeframe job"""
        request_datetime = datetime.datetime.now(tz=datetime.timezone.utc)

        semaphore = asyncio.Semaphore(self.TASKS_LIMIT)
        tasks = [self._load_data(asset, request_datetime, semaphore) for asset in self._asset_names]

        results = asyncio.run(asyncio.gather(*tasks, return_exceptions=True))
        for book_data in results:
            self._save_timeframe_book_update(book_data)
        del results

    @abstractmethod
    def get_symbols_books_snapshot(self, asset_name, timeframe_datetime) -> AssetBookData:
        """Loading option book for timeframe
        Should return tuple with future and options dataframe with column name from FutureColumns, OptionColumns"""

    @staticmethod
    def get_request_timeframe_folder(timeframe: TimeframeCode, request_datetime: datetime.datetime) -> str:
        """Get timeframe hierarchy folder structure and name"""
        match timeframe:
            case TimeframeCode.EOD:
                return f'{request_datetime.year}/{request_datetime.strftime("%y-%m-%d")}.parquet'
            case TimeframeCode.MINUTE_1:
                return f'{request_datetime.year}/{request_datetime.month}/{request_datetime.day}/{request_datetime.strftime("%y-%m-%dT%H-%M")}.parquet'
            case TimeframeCode.HOUR_1:
                return f'{request_datetime.year}/{request_datetime.month}/{request_datetime.strftime("%y-%m-%dT%H")}.parquet'
            case _:
                raise ValueError(f'Unknown timeframe {timeframe.code}')

    def get_timeframe_update_path(self, asset_name, asset_type, request_datetime):
        """Get path for request datetime correspondent to timeframe"""
        base_path = self.get_updates_folder(self._data_path, asset_name, asset_type, self._timeframe)
        update_folder = self.get_request_timeframe_folder(self._timeframe, request_datetime)
        return os.path.abspath(os.path.normpath(os.path.join(base_path, update_folder)))

    def add_save_task_to_background(self, store_path, df: pd.DataFrame):
        """
        Saving in background
        """
        os.makedirs(os.path.dirname(store_path), exist_ok=True)
        print( store_path)
        # df.to_parquet(store_path) # TODO to async background task

    def _save_timeframe_book_update(self, book_data: AssetBookData):
        """Save book data"""
        opt_path = self.get_timeframe_update_path(book_data.asset_name, AssetType.OPTION, book_data.request_datetime)
        self.add_save_task_to_background(opt_path, book_data.option)
        book_data.option = None
        fut_path = self.get_timeframe_update_path(book_data.asset_name, AssetType.FUTURE, book_data.request_datetime)
        self.add_save_task_to_background(fut_path, book_data.future)
        book_data.future = None
