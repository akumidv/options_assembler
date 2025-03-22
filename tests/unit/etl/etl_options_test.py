import datetime
import pytest
import pandas as pd
from options_etl.etl_class import AssetBookData, SaveTask

from options_assembler.entities import Timeframe
import concurrent
from concurrent.futures import ThreadPoolExecutor


def test_add_save_task_to_background_thread_safe(etl_options):
    num_of_task = 100
    etl_options._save_task_cron = {'day': '*', 'hour': '*', 'minute': '*', 'second': '*/5'}
    save_task = SaveTask('./test.parquet', None)
    saved_tasks = len(etl_options._save_tasks)
    etl_options.add_save_task_to_background(save_task)
    assert len(etl_options._save_tasks) == 1 + saved_tasks

    executor = ThreadPoolExecutor(max_workers=10)
    job_results = [executor.submit(etl_options.add_save_task_to_background,
                                   SaveTask(f'./test_{idx}.parquet', None)) for idx in range(num_of_task - 1)]
    job_res = list(concurrent.futures.as_completed(job_results))
    assert len(job_res) == num_of_task - 1
    assert len(etl_options._save_tasks) == num_of_task + saved_tasks
    etl_options._save_tasks = []


def test__save_timeframe_book_update(etl_options):
    options_df = pd.DataFrame()
    future_df = pd.DataFrame()
    spot_df = pd.DataFrame()
    saved_tasks = len(etl_options._save_tasks)
    book_data = AssetBookData(asset_name='BTC',
                              request_timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
                              option=options_df, future=future_df, spot=spot_df)
    etl_options._save_timeframe_book_update(book_data)
    assert len(etl_options._save_tasks) == 3 + saved_tasks
    etl_options._save_tasks = []


def test__check_is_request_later_then_timestamp(etl_options):
    last_request_datetime = datetime.datetime.now(tz=datetime.timezone.utc)
    # 1min
    request_datetime = last_request_datetime + datetime.timedelta(minutes=1)
    assert not etl_options._check_is_request_later_then_timestamp(request_datetime, last_request_datetime, Timeframe.MINUTE_1)
    request_datetime = last_request_datetime + datetime.timedelta(minutes=2)
    assert etl_options._check_is_request_later_then_timestamp(request_datetime, last_request_datetime, Timeframe.MINUTE_1)
    # 1hour
    request_datetime = last_request_datetime + datetime.timedelta(hours=1)
    assert not etl_options._check_is_request_later_then_timestamp(request_datetime, last_request_datetime, Timeframe.HOUR_1)
    request_datetime = last_request_datetime + datetime.timedelta(hours=2)
    assert etl_options._check_is_request_later_then_timestamp(request_datetime, last_request_datetime, Timeframe.HOUR_1)
    # 1day
    request_datetime = last_request_datetime + datetime.timedelta(days=1)
    assert not etl_options._check_is_request_later_then_timestamp(request_datetime, last_request_datetime, Timeframe.EOD)
    request_datetime = last_request_datetime + datetime.timedelta(days=2)
    assert etl_options._check_is_request_later_then_timestamp(request_datetime, last_request_datetime, Timeframe.EOD)
