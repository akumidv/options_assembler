"""ETL class"""
import datetime
import os
import sys
import time
from abc import ABC, abstractmethod
from typing import NamedTuple
from dataclasses import dataclass
import threading
import concurrent
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from option_lib.entities import Timeframe, AssetKind
from option_lib.provider.exchange import AbstractExchange
from option_lib.etl.messanger import AbstractMessanger, StandardMessanger


@dataclass
class AssetBookData:
    """Asset book data for timeframe"""
    asset_name: str
    request_timestamp: pd.Timestamp
    option: pd.DataFrame | None
    futures: pd.DataFrame | None
    spot: pd.DataFrame | None


class SaveTask(NamedTuple):
    """Save task date"""
    store_path: str
    df: pd.DataFrame | None


class EtlOptions(ABC):
    """ETL Class"""
    TASKS_LIMIT = 4
    SAVE_TASKS_LIMIT = 2
    _save_tasks: list[SaveTask] = []
    _save_tasks_update_lock = threading.Lock()
    _get_data_lock = threading.Lock()
    _save_data_lock = threading.Lock()
    _asset_names: list[str] | str | None
    _heartbeat_message_interval: pd.Timedelta
    _heartbeat_last_message_time: pd.Timestamp
    _number_of_requests: int = 0
    _number_of_jobs: int = 0
    _avg_job_time: float = 0.
    _number_saved_files: int = 0
    _messages: list = []
    _messages_lock = threading.Lock()

    def __init__(self, exchange: AbstractExchange, asset_names: list[str] | str | None, timeframe: Timeframe,
                 data_path: str, timeframe_cron: dict | None = None, messanger: AbstractMessanger | None = None):
        """
        Initialize ETL

        Args:
            timeframe_cron: dict - cron format in dict like {'days': '*', hours: '*', minutes: '*/5'}.

        """
        self.exchange = exchange
        self._data_path = data_path
        self._asset_names = asset_names
        self._timeframe: Timeframe = timeframe
        self._messanger: AbstractMessanger = messanger if messanger is not None else StandardMessanger()

        self._last_request_timestamp = pd.Timestamp.now(tz=datetime.UTC)
        self._heartbeat_last_message_time = self._last_request_timestamp
        self._scheduler_background: BackgroundScheduler = BackgroundScheduler(timezone=datetime.UTC)
        self._heartbeat_message_interval = pd.Timedelta(minutes=timeframe.mult * 2 if timeframe.mult < 30 else 60)
        self._scheduler_background.add_job(self._heartbeat, 'interval',
                                           minutes=1 if self._heartbeat_message_interval < 30 else 10)
        self._scheduler_background.add_job(self._report, 'interval', hours=4 if timeframe.mult >= 30 else 1)
        self._scheduler_background.add_job(self._save_tasks_dataframes_job, 'interval', seconds=60)
        if timeframe_cron is None:
            timeframe_cron = self._get_cron_params_from_timeframe(timeframe)
        self._scheduler_etl: BlockingScheduler = BlockingScheduler(timezone=datetime.UTC)
        self._scheduler_etl.add_job(self._book_snapshot_timeframe_job, 'cron', **timeframe_cron)

    def _heartbeat(self):
        if pd.Timestamp.now(tz=datetime.UTC) - self._heartbeat_last_message_time > pd.Timedelta(minutes=self._heartbeat_message_interval):
            print(f'[HB] [{datetime.datetime.now().isoformat(timespec="seconds")}] '
                  f'loading {"yes" if self._get_data_lock.locked() else "no"}, '
                  f'saving {"yes" if self._save_data_lock.locked() else "no"}, '
                  f'updates to save: {len(self._save_tasks)}, saved {self._number_saved_files}, '
                  f'requests made {self._number_of_requests} '
                  f'in jobs {self._number_of_jobs} by avg {self._avg_job_time: .2f} sec')
        # Checks below
        if len(self._messages) > 0:
            self._report()

    def _report(self):
        report_text = f'`[ETL REPORT]` [{datetime.datetime.now().isoformat(timespec="seconds")}] ' \
                      f'Exchange: **{self.exchange.exchange_code}** for timeframe *{self._timeframe.value}*\n' \
                      f'Saved files {self._number_saved_files} (waiting {len(self._save_tasks)}), ' \
                      f'{self._number_of_requests} requests made ' \
                      f'in {self._number_of_jobs} jobs by avg {self._avg_job_time: .2f} sec'
        messages = self._get_messages()
        if len(messages) > 0:
            report_text += '(!)`Warning messages`:\n'
            report_text += '\n- '.join(messages)
        self._messanger.send_message(report_text)

    def print_etl(self):
        """Print jobs"""
        print('Assets:', ','.join(self._asset_names) if isinstance(self._asset_names, list) else self._asset_names)
        print('Timeframe:', self._timeframe.value)
        print('Stored path:', os.path.abspath(os.path.normpath(self._data_path)))
        print('Stored path:', os.path.abspath(os.path.normpath(self._data_path)))
        self._scheduler_etl.print_jobs()

    def get_updates_folder(self, asset_name: str, asset_kind: AssetKind, timeframe: Timeframe) -> str:
        """Return path to folder where all update should be stored"""
        return f'{self._data_path}/{self.exchange.exchange_code}/{asset_name}/{asset_kind.value}/{timeframe.value}'

    def start(self):
        """Start scheduled loading"""
        self._scheduler_background.start()
        self._scheduler_etl.start()

    @staticmethod
    def _get_cron_params_from_timeframe(timeframe: Timeframe):
        match timeframe:
            case Timeframe.EOD:
                return {'day': '*', 'hour': 23, 'minute': 59}
            case Timeframe.MINUTE_1:
                return {'hour': '*', 'minute': '*'}
            case Timeframe.MINUTE_5:
                return {'hour': '*', 'minute': '4,9,14,19,24,29,34,39,44,49,54,59'}
            case Timeframe.MINUTE_15:
                return {'hour': '*', 'minute': '14,29,44,59'}
            case Timeframe.MINUTE_30:
                return {'hour': '*', 'minute': '29,59'}
            case Timeframe.HOUR_1:
                return {'hour': '*', 'minute': 59}
            case Timeframe.HOUR_4:
                return {'hour': '3,7,11,15,19,23', 'minute': 59}
            case _:
                raise NotImplementedError(f'Unknown timeframe  {timeframe.value}')

    @staticmethod
    def _check_is_request_later_then_timestamp(request_timestamp: pd.Timestamp, last_request_timestamp,
                                               timeframe: Timeframe) -> bool:
        request_diff = request_timestamp - last_request_timestamp
        match timeframe:
            case Timeframe.EOD:
                return (pd.Timedelta(days=1, hours=1) - request_diff).total_seconds() < 0
            case Timeframe.MINUTE_1:
                return (pd.Timedelta(minutes=1, seconds=30) - request_diff).total_seconds() < 0
            case Timeframe.MINUTE_5:
                return (pd.Timedelta(minutes=6, seconds=0) - request_diff).total_seconds() < 0
            case Timeframe.HOUR_1:
                return (pd.Timedelta(hours=1, minutes=15) - request_diff).total_seconds() < 0
            case _:
                return (pd.Timedelta(minutes=timeframe.mult * 1.25) - request_diff).total_seconds() < 0

    def _get_messages(self) -> list[str]:
        messages = []
        with self._messages_lock:
            if len(self._messages) > 0:
                messages = self._messages
                self._messages = []
        return messages

    def _add_message(self, message: str):
        with self._messages_lock:
            self._messages.append(message)

    def _book_snapshot_timeframe_job(self):
        """
        Timeframe job
        """

        if self._save_data_lock.locked():
            # It good idea move to queue of jobs and check len of queue if more than limit - than drop new requests,
            # also can measure avg time of requests and forecast by timeframe what to drop to keep data integrity
            print('[ERROR] Can not be two requests for data at the same time, something wrong, request for time'
                  f'{datetime.datetime.now().isoformat()} delayed', file=sys.stderr)

        with self._get_data_lock:
            start_tm = time.time()
            request_timestamp = pd.Timestamp.now(tz=datetime.UTC)
            if self._check_is_request_later_then_timestamp(request_timestamp, self._last_request_timestamp,
                                                           self._timeframe):
                self._add_message(f'[WARNING] [{datetime.datetime.now().isoformat(timespec="seconds")}] request '
                                  f'distance more than timeframe {self._timeframe.value}: '
                                  f'{request_timestamp - self._last_request_timestamp}')
            self._last_request_timestamp = request_timestamp
            if isinstance(self._asset_names, list):
                executor = ThreadPoolExecutor(
                    max_workers=self.TASKS_LIMIT)  # Multiprocessing pool will be better due dataframe preparation
                job_results = {executor.submit(self.get_symbols_books_snapshot, asset, request_timestamp): asset
                               for asset in self._asset_names}
                for job_res in concurrent.futures.as_completed(job_results):
                    asset = job_results[job_res]
                    book_data = job_res.result()
                    if isinstance(book_data, Exception):
                        print(f'[ERROR] for {asset} book data: {book_data}')
                        continue
                    self._save_timeframe_book_update(book_data)
                self._number_of_requests += len(job_results)
                self._number_of_jobs += 1
                del job_results
            else:
                book_data = self.get_symbols_books_snapshot(self._asset_names, request_timestamp)
                self._save_timeframe_book_update(book_data)  # can be after release self._get_data_lock
                self._number_of_requests += 1
                self._number_of_jobs += 1
            self._avg_job_time = self._avg_job_time - self._avg_job_time / self._number_of_jobs + \
                                 (time.time() - start_tm) / self._number_of_jobs

    @abstractmethod
    def get_symbols_books_snapshot(self, asset_name: str, request_timestamp: pd.Timestamp) -> AssetBookData:
        """Loading option book for timeframe
        Should return object with future and options dataframe with column name from FutureColumns, OptionColumns"""

    @staticmethod
    def get_request_timeframe_folder(timeframe: Timeframe, request_timestamp: pd.Timestamp) -> str:
        """Get timeframe hierarchy folder structure and name"""
        match timeframe:
            case Timeframe.EOD:
                return f'{request_timestamp.year}/{request_timestamp.strftime("%y-%m-%d")}.parquet'
            case Timeframe.MINUTE_1:
                return f'{request_timestamp.year}/{request_timestamp.month}/{request_timestamp.day}/' \
                       f'{request_timestamp.strftime("%y-%m-%dT%H-%M")}.parquet'
            case Timeframe.MINUTE_5:
                return f'{request_timestamp.year}/{request_timestamp.month}/{request_timestamp.day}/' \
                       f'{request_timestamp.strftime("%y-%m-%dT%H-%M")}.parquet'
            case Timeframe.HOUR_1:
                return f'{request_timestamp.year}/{request_timestamp.month}/' \
                       f'{request_timestamp.strftime("%y-%m-%dT%H")}.parquet'
            case _:
                if timeframe.mult < 60:
                    return f'{request_timestamp.year}/{request_timestamp.month}/{request_timestamp.day}/' \
                           f'{request_timestamp.strftime("%y-%m-%dT%H-%M")}.parquet'
                elif timeframe.mult < 60 * 24:
                    return f'{request_timestamp.year}/{request_timestamp.month}/' \
                           f'{request_timestamp.strftime("%y-%m-%dT%H-%M")}.parquet'
                return f'{request_timestamp.year}/{request_timestamp.strftime("%y-%m-%d")}.parquet'

    def get_timeframe_update_path(self, asset_name: str, asset_kind: AssetKind, request_timestamp: pd.Timestamp):
        """Get path for request datetime correspondent to timeframe"""
        base_path = self.get_updates_folder(asset_name, asset_kind, self._timeframe)
        update_folder = self.get_request_timeframe_folder(self._timeframe, request_timestamp)
        return os.path.abspath(os.path.normpath(os.path.join(base_path, update_folder)))

    def add_save_task_to_background(self, save_task):
        """
        Saving in background
        """
        with self._save_tasks_update_lock:
            self._save_tasks.append(save_task)

    def _save_tasks_dataframes_job(self):
        if self._save_data_lock.locked():  # If already processed skip run
            return
        with self._save_data_lock:
            self._get_data_lock.acquire()  # Wait until end of data loading
            self._get_data_lock.release()
            if len(self._save_tasks) > 0:
                tasks = self._save_tasks
                self._save_tasks = []
                with ThreadPoolExecutor(max_workers=self.SAVE_TASKS_LIMIT) as executor:
                    executor.map(self._save_task_dataframe, tasks)
                self._number_saved_files += len(tasks)
                del tasks

    @staticmethod
    def _save_task_dataframe(save_task: SaveTask) -> None:
        if save_task.df is None:
            return
        fn = save_task.store_path
        os.makedirs(os.path.dirname(fn), exist_ok=True)

        df = save_task.df
        if os.path.isfile(fn):
            # For deribit when diff currencies requested they have some intersection symbols in response
            df_cur = pd.read_parquet(fn)
            df = pd.concat([df_cur, df], ignore_index=True, copy=False)
        df.to_parquet(fn)
        save_task.df = None

    def _save_timeframe_book_update(self, book_data: AssetBookData):
        """Save book data
        if asset name is None - than parse names from dataframe
        """
        fabric = {
            'option': AssetKind.OPTION,
            'futures': AssetKind.FUTURE,
            'spot': AssetKind.SPOT
        }
        request_timestamp = book_data.request_timestamp
        asset_name = book_data.asset_name
        for asset_kind_attr in fabric:
            df = getattr(book_data, asset_kind_attr)
            if df is not None:
                opt_path = self.get_timeframe_update_path(asset_name, fabric[asset_kind_attr],
                                                          request_timestamp)
                self.add_save_task_to_background(SaveTask(opt_path, df))
                setattr(book_data, asset_kind_attr, None)
