"""ETL class"""
import datetime
import os
import sys
import time
from abc import ABC, abstractmethod
from typing import NamedTuple
from dataclasses import dataclass
import threading
from concurrent.futures import ThreadPoolExecutor
import functools
import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import STATE_STOPPED as JOBS_STATE_STOPPED
from option_lib.entities import Timeframe, AssetKind
from exchange import AbstractExchange
from messanger import AbstractMessanger, StandardMessanger


@dataclass
class AssetBookData:
    """Asset book data for timeframe"""
    asset_name: str
    request_timestamp: pd.Timestamp
    option: pd.DataFrame | None
    future: pd.DataFrame | None
    spot: pd.DataFrame | None


class SaveTask(NamedTuple):
    """Save task date"""
    store_path: str
    df: pd.DataFrame | None


class EtlOptions(ABC):
    """ETL Class"""
    TASKS_LIMIT = 4
    SAVE_TASKS_LIMIT = 2
    HOST_NAME = os.uname()[1]
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
                 update_data_path: str, timeframe_cron: dict | str | None = None,
                 messanger: AbstractMessanger | None = None,
                 is_detailed: bool = False):
        """
        Initialize ETL

        Args:
            timeframe_cron: dict - cron format in dict like {'days': '*', hours: '*', minutes: '*/5'}.

        """
        self.exchange = exchange
        self._update_data_path = update_data_path
        self._asset_names = asset_names
        self._timeframe: Timeframe = timeframe
        self._messanger: AbstractMessanger = messanger if messanger is not None else StandardMessanger()

        self._last_request_timestamp = pd.Timestamp.now(tz=datetime.UTC)
        self._heartbeat_last_message_time = self._last_request_timestamp
        self._scheduler_background: BackgroundScheduler = BackgroundScheduler(timezone=datetime.UTC) # job_defaults={'max_instances': 1})
        self._heartbeat_message_interval = pd.Timedelta(minutes=timeframe.mult * 2 if timeframe.mult < 30 else 60)
        self._scheduler_background.add_job(self._heartbeat, 'interval',
                                           minutes=(1 if self._heartbeat_message_interval.total_seconds() * 60 < 30
                                                    else 10), max_instances=1)
        self._scheduler_background.add_job(self._report, 'interval', hours=(4 if timeframe.mult >= 30 else 1),
                                           max_instances=1)
        self._scheduler_background.add_job(self._save_tasks_dataframes_job, 'interval', seconds=120, max_instances=1)
        if timeframe_cron is None:
            timeframe_cron = self._get_cron_params_from_timeframe(timeframe, is_detailed)
        elif isinstance(timeframe_cron, str):
            timeframe_cron = self._parse_cron_string(timeframe_cron)
        self._scheduler_etl: BlockingScheduler = BlockingScheduler(timezone=datetime.UTC,
                                                                   job_defaults={'max_instances': 1})
        self._scheduler_etl.add_job(self._book_snapshot_timeframe_job, 'cron', **timeframe_cron, timezone=datetime.UTC,
                                    max_instances=1)

    def _heartbeat(self):
        if pd.Timestamp.now(tz=datetime.UTC) - self._heartbeat_last_message_time > self._heartbeat_message_interval:
            print(f'[HB] [{datetime.datetime.now().isoformat(timespec="seconds")}] '
                  f'loading {"yes" if self._get_data_lock.locked() else "no"}, '
                  f'saving {"yes" if self._save_data_lock.locked() else "no"}, '
                  f'updates to save: {len(self._save_tasks)}, saved {self._number_saved_files}, '
                  f'requests made {self._number_of_requests} '
                  f'in jobs {self._number_of_jobs} by avg {self._avg_job_time: .2f} sec')
        if len(self._messages) > 0:
            self._report()

    def _report(self):
        report_text = f'`[{self.HOST_NAME} ETL REPORT]` {datetime.datetime.now().isoformat(timespec="seconds")}\n' \
                      f'Exchange: **{self.exchange.exchange_code}** for timeframe *{self._timeframe.value}*\n' \
                      f'Saved files {self._number_saved_files} (waiting {len(self._save_tasks)}), ' \
                      f'{self._number_of_requests} requests made ' \
                      f'in {self._number_of_jobs} jobs by avg {self._avg_job_time: .2f} sec'
        messages = self._get_messages()
        if len(messages) > 0:
            report_text += '\n(!)`Warning messages`:\n'
            report_text += '\n- '.join(messages)
        self._messanger.send_message(report_text)

    def print_etl(self):
        def report_jobs(scheduler):
            # pylint: disable=protected-access, W0212
            text = ''
            with scheduler._jobstores_lock:
                if scheduler.state == JOBS_STATE_STOPPED:
                    if scheduler._pending_jobs:
                        text += '\nPending jobs:'
                        for job, _, _ in scheduler._pending_jobs:
                            text += f'\n   - `{job}`'
                    else:
                        text += '\nNo pending jobs'
            return text

        """Print jobs"""
        report_text = 'Assets:' + ','.join(self._asset_names) \
            if isinstance(self._asset_names, list) \
            else (self._asset_names if self._asset_names else 'All assets.')
        report_text += f'\nTimeframe: {self._timeframe.value}'
        report_text += f'\nStored path: {os.path.abspath(os.path.normpath(self._update_data_path))}'
        report_text += report_jobs(self._scheduler_etl)
        report_text += report_jobs(self._scheduler_background)
        print(report_text)
        self._messanger.send_message(report_text)

    def get_updates_folder(self, asset_name: str, asset_kind: AssetKind | str, timeframe: Timeframe) -> str:
        """Return path to folder where all update should be stored"""
        return f'{self._update_data_path}/{self.exchange.exchange_code}/{asset_name}/' \
               f'{asset_kind if isinstance(asset_kind, str) else asset_kind.value}/{timeframe.value}'

    def start(self):
        """Start scheduled loading"""
        self._scheduler_background.start()
        self._scheduler_etl.start()

    @staticmethod
    def _parse_cron_string(timeframe_cron: str):
        if not isinstance(timeframe_cron, str):
            raise TypeError('Parsed cron should be string')
        cron_arr = timeframe_cron.split(' ')
        if len(cron_arr) != 5:
            raise ValueError('Cron structure mismatch')

        return {'day_of_week': cron_arr[4], 'month': cron_arr[3], 'day': cron_arr[2], 'hour': cron_arr[1],
                'minute': cron_arr[0]}

    @staticmethod
    def _get_cron_params_from_timeframe(timeframe: Timeframe, is_detailed: bool = False):
        match timeframe:
            case Timeframe.EOD:
                return {'day': '*', 'hour': '0,12,23', 'minute': 59} if is_detailed else {'day': '*', 'hour': 23,
                                                                                          'minute': 59}
            case Timeframe.MINUTE_1:
                return {'hour': '*', 'minute': '*'}
            case Timeframe.MINUTE_5:
                return {'hour': '*', 'minute': '0,3,4,5,7,9,10,12,14,15,17,19,20,22,24,25,27,29,'
                                               '30,32,34,35,37,39,40,42,44,45,47,49,50,52,54,55,57,59'
                        } if is_detailed else {'hour': '*', 'minute': '4,9,14,19,24,29,34,39,44,49,54,59'}
            case Timeframe.MINUTE_15:
                return {'hour': '*', 'minute': '0,7,14,15,22,29,30,37,44,45,52,59'} if is_detailed else \
                    {'hour': '*', 'minute': '14,29,44,59'}
            case Timeframe.MINUTE_30:
                return {'hour': '*', 'minute': '0,15,29,30,45,59'} if is_detailed else {'hour': '*', 'minute': '29,59'}
            case Timeframe.HOUR_1:
                return {'hour': '*', 'minute': '0,30,59'} if is_detailed else {'hour': '*', 'minute': 59}
            case Timeframe.HOUR_4:
                return {'hour': '0,2,3,4,6,7,8,10,11,12,14,15,16,18,19,20,22,23', 'minute': 59} if is_detailed else \
                    {'hour': '3,7,11,15,19,23', 'minute': 59}
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
        print(message, flush=True)

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
            if isinstance(self._asset_names, list):
                with ThreadPoolExecutor(max_workers=self.TASKS_LIMIT) as executor:
                    function_partial = functools.partial(self.get_symbols_books_snapshot,
                                                         request_timestamp=request_timestamp)

                    for asset_name, book_data in zip(self._asset_names,
                                                     executor.map(function_partial, self._asset_names)):
                        if isinstance(book_data, Exception):
                            print(f'[ERROR] for {asset_name} book data: {book_data}')
                            continue
                        self._save_timeframe_book_update(book_data)
                    self._number_of_requests += len(self._asset_names)
            else:
                book_data = self.get_symbols_books_snapshot(self._asset_names, request_timestamp)
                self._save_timeframe_book_update(book_data)  # can be after release self._get_data_lock
                self._number_of_requests += 1
            self._number_of_jobs += 1
            self._last_request_timestamp = request_timestamp
            self._avg_job_time = self._avg_job_time - self._avg_job_time / self._number_of_jobs + \
                                 (time.time() - start_tm) / self._number_of_jobs

    @abstractmethod
    def get_symbols_books_snapshot(self, asset_name: str, request_timestamp: pd.Timestamp) -> AssetBookData:
        """Loading option book for timeframe
        Should return object with future and options dataframe with column name from FutureColumns, OptionColumns"""

    @staticmethod
    def get_request_timeframe_file(timeframe: Timeframe, request_timestamp: pd.Timestamp) -> str:
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
                       f'{request_timestamp.strftime("%y-%m-%dT%H-%M")}.parquet'
            case _:
                if timeframe.mult <= 120:
                    return f'{request_timestamp.year}/{request_timestamp.month}/{request_timestamp.day}/' \
                           f'{request_timestamp.strftime("%y-%m-%dT%H-%M")}.parquet'
                elif timeframe.mult <= 60 * 24 * 2:
                    return f'{request_timestamp.year}/{request_timestamp.month}/' \
                           f'{request_timestamp.strftime("%y-%m-%dT%H")}.parquet'
                return f'{request_timestamp.year}/{request_timestamp.strftime("%y-%m-%d")}.parquet'

    def get_timeframe_update_path(self, asset_name: str, asset_kind: AssetKind | str, request_timestamp: pd.Timestamp):
        """Get path for request datetime correspondent to timeframe"""
        base_path = self.get_updates_folder(asset_name, asset_kind, self._timeframe)
        update_folder = self.get_request_timeframe_file(self._timeframe, request_timestamp)
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

    # @staticmethod
    def _save_task_dataframe(self, save_task: SaveTask) -> None:
        if save_task.df is None:
            return
        fn = save_task.store_path
        os.makedirs(os.path.dirname(fn), exist_ok=True)

        df = save_task.df
        if os.path.isfile(fn):
            # For deribit when diff currencies requested they have some intersection symbols in response
            try:
                df_cur = pd.read_parquet(fn)
                df = pd.concat([df_cur, df], ignore_index=True, copy=False)
            except Exception as err:
                new_fn = f'{fn}_{datetime.datetime.now(tz=datetime.UTC).isoformat().replace(":", "_")}'
                try:
                    os.rename(fn, new_fn)
                    error_text = f'[ERROR] loading updating file {fn}. File renamed to {new_fn}. Error: {err}'
                except Exception as new_err:
                    error_text = f'[ERROR] loading and renaming updating file {fn}: {err}/{new_err}'
                self._add_message(error_text)
        df.reset_index(drop=True, inplace=True)
        try:
            df.to_parquet(fn)
        except Exception as err:
            error_text = f'[ERROR] saving file {fn}: {err}'
            try:
                df.to_parquet(fn)
            except Exception as new_err:
                new_fn = f'{fn}_{datetime.datetime.now(tz=datetime.UTC).isoformat().replace(":", "_")}'
                try:
                    df.to_parquet(new_fn)
                    error_text += f'. Saved as {new_fn} ({new_err})'
                except Exception as new_save_err:
                    error_text += f'. Can able save as {new_fn} ({new_err}/{new_save_err}). Skipped'
            self._add_message(error_text)
        save_task.df = None

    def _save_timeframe_book_update(self, book_data: AssetBookData):
        """Save book data
        if asset name is None - than parse names from dataframe
        """
        fabric = {
            'option': AssetKind.OPTION,
            'future': AssetKind.FUTURE,
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
