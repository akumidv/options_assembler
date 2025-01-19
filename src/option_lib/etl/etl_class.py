"""ETL class"""
import datetime
import os
from abc import ABC, abstractmethod
import asyncio
from typing import NamedTuple
import concurrent
from concurrent.futures import ThreadPoolExecutor
import threading

import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
from option_lib.entities import Timeframe, AssetKind
from option_lib.provider.exchange import AbstractExchange


class AssetBookData(NamedTuple):
    """Asset book data for timeframe"""
    asset_name: str
    request_datetime: datetime.datetime
    option: pd.DataFrame | None
    future: pd.DataFrame | None
    spot: pd.DataFrame | None


class SaveTask(NamedTuple):
    """Save task date"""
    store_path: str
    df: pd.DataFrame


class EtlOptions(ABC):
    """ETL Class"""
    TASKS_LIMIT = 4
    _save_tasks: list[SaveTask] = []
    _task_lock = threading.Lock()

    def __init__(self, exchange: AbstractExchange, asset_names: list[str] | None, timeframe: Timeframe, data_path: str):
        self.exchange = exchange
        self._data_path = data_path
        self._asset_names = asset_names
        self._timeframe: Timeframe = timeframe
        self._create_folders()
        timeframe_cron = self._get_cron_params_from_timeframe(timeframe)
        self._scheduler: BlockingScheduler = BlockingScheduler(timezone=datetime.UTC)
        # self._scheduler: AsyncIOScheduler = AsyncIOScheduler(timezone=datetime.UTC)
        self._scheduler.add_job(self._book_snapshot_timeframe_job, 'cron', **timeframe_cron)

    @staticmethod
    def get_updates_folder(data_path: str, asset_name: str, asset_kind: AssetKind, timeframe: Timeframe) -> str:
        """Return path to folder where all update should be stored"""
        # base_path = os.path.abspath(os.path.normpath(os.path.join(data_path, 'updates')))
        return f'{data_path}/{asset_name}/{asset_kind.value}/{timeframe.value}'

    def _create_folders(self):
        """Create folders for save books"""
        if self._asset_names:
            for asset_name in self._asset_names:
                os.makedirs(self.get_updates_folder(self._data_path, asset_name, AssetKind.FUTURE,
                                                    self._timeframe), exist_ok=True)
                os.makedirs(self.get_updates_folder(self._data_path, asset_name, AssetKind.OPTION,
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
    def _get_cron_params_from_timeframe(timeframe: Timeframe):
        match timeframe:
            case Timeframe.EOD:
                return {'day': '*', 'hour': 0, 'minute': 0}
            case Timeframe.MINUTE_1:
                return {'hour': '*', 'minute': '*'}
            case Timeframe.HOUR_1:
                return {'hour': '', 'minute': '0'}
            case _:
                raise ValueError(f'Unknown timeframe  {timeframe.code}')

    async def _load_data1(self, asset_name: str, request_datetime: datetime.datetime,
                          semaphore: asyncio.Semaphore) -> AssetBookData:
        async with semaphore:
            return await asyncio.to_thread(self.get_symbols_books_snapshot, asset_name, request_datetime)

    def _book_snapshot_timeframe_job(self):
        """Timeframe job"""
        request_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        executor = ThreadPoolExecutor(max_workers=self.TASKS_LIMIT)

        job_results = {executor.submit(self.get_symbols_books_snapshot, asset, request_datetime): asset \
                       for asset in self._asset_names}
        for job_res in concurrent.futures.as_completed(job_results):
            asset = job_results[job_res]
            book_data = job_res.result()
            print('>##', asset, request_datetime, type(book_data))
            if isinstance(book_data, Exception):
                print(f'[ERROR] for {asset} book data: {book_data}')
                continue
            self._save_timeframe_book_update(book_data)
        del job_results

    async def _timeframe_job1(self):
        """Timeframe job"""
        request_datetime = datetime.datetime.now(tz=datetime.timezone.utc)

        semaphore = asyncio.Semaphore(self.TASKS_LIMIT)
        tasks = [self._load_data1(asset, request_datetime, semaphore) for asset in self._asset_names]

        # results = asyncio.run(asyncio.gather(*tasks, return_exceptions=True))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for book_data in results:
            self._save_timeframe_book_update(book_data)
        del results

    @abstractmethod
    def get_symbols_books_snapshot(self, asset_name: str, request_datetime: datetime.datetime) -> AssetBookData:
        """Loading option book for timeframe
        Should return tuple with future and options dataframe with column name from FutureColumns, OptionColumns"""

    @staticmethod
    def get_request_timeframe_folder(timeframe: Timeframe, request_datetime: datetime.datetime) -> str:
        """Get timeframe hierarchy folder structure and name"""
        match timeframe:
            case Timeframe.EOD:
                return f'{request_datetime.year}/{request_datetime.strftime("%y-%m-%d")}.parquet'
            case Timeframe.MINUTE_1:
                return f'{request_datetime.year}/{request_datetime.month}/{request_datetime.day}/' \
                       f'{request_datetime.strftime("%y-%m-%dT%H-%M")}.parquet'
            case Timeframe.HOUR_1:
                return f'{request_datetime.year}/{request_datetime.month}/' \
                       f'{request_datetime.strftime("%y-%m-%dT%H")}.parquet'
            case _:
                raise ValueError(f'Unknown timeframe {timeframe.code}')

    def get_timeframe_update_path(self, asset_name, asset_kind, request_datetime):
        """Get path for request datetime correspondent to timeframe"""
        base_path = self.get_updates_folder(self._data_path, asset_name, asset_kind, self._timeframe)
        update_folder = self.get_request_timeframe_folder(self._timeframe, request_datetime)
        return os.path.abspath(os.path.normpath(os.path.join(base_path, update_folder)))

    def add_save_task_to_background(self, save_task):
        """
        Saving in background
        """
        os.makedirs(os.path.dirname(save_task.store_path), exist_ok=True)
        print(save_task.store_path)
        with self._task_lock:
            # self._task_lock.acquire()
            self._save_tasks.append(save_task)
            # df.to_parquet(store_path) # TODO to async background task
            # self._task_lock.release()

    def _save_timeframe_book_update(self, book_data: AssetBookData):
        """Save book data
        if asset name is None - than parse names from dataframe
        """
        if book_data.option is not None:
            opt_path = self.get_timeframe_update_path(book_data.asset_name, AssetKind.OPTION,
                                                      book_data.request_datetime)
            self.add_save_task_to_background(SaveTask(opt_path, book_data.option))
            book_data.option = None
        if book_data.future is not None:
            fut_path = self.get_timeframe_update_path(book_data.asset_name, AssetKind.FUTURE,
                                                      book_data.request_datetime)
            self.add_save_task_to_background(SaveTask(fut_path, book_data.future))
            book_data.future = None
        if book_data.spot is not None:
            spot_path = self.get_timeframe_update_path(book_data.asset_name, AssetKind.SPOT, book_data.request_datetime)
            self.add_save_task_to_background(SaveTask(spot_path, book_data.spot))
            book_data.spot = None
