"""Update history timeframes from timeframe updates"""
import datetime
import random
import os
import re
from typing import Generator
import threading
import concurrent
import itertools
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from option_lib.provider import PandasLocalFileProvider, RequestParameters
from option_lib.entities import Timeframe, AssetKind, OptionColumns as OCl, FuturesColumns as FCl
from option_lib.provider.exchange.exchange_entities import ExchangeCode


# Метод минимального таймфрейма из которого готовятся все остальные более старшие таймфреймы, а лучше данные
# всех нижеследующих таймфреймов

class EtlHistory:
    TASKS_LIMIT: int = 4
    update_fn_pattern: re.Pattern = re.compile(r'^(\d{4}|\d{2})-\d{2}-\d{2}((T\d{2}-\d{2})|(T\d{2}))?\.parquet$')

    def __init__(self, exchange_code: ExchangeCode | str, history_path: str, update_path: str, timeframe: Timeframe,
                 symbols: list[str] | None = None, asset_kinds: list[AssetKind] | None = None,
                 full_history: bool = False):
        self._exchange_code: str = exchange_code if isinstance(exchange_code, str) else exchange_code.name
        self.history_path: str = history_path
        self.update_path: str = update_path
        self._timeframe: Timeframe = timeframe
        self._symbols: list[str] | None = symbols
        self._asset_kinds = asset_kinds if asset_kinds is not None else [ak for ak in AssetKind]
        self._source_timeframes: list[Timeframe] = list(sorted([tm for tm in Timeframe
                                                                if tm.mult <= self._timeframe.mult],
                                                               key=lambda tm: tm.mult))
        self._full_history = full_history  # Is force to process full history or only update from last stored for timeframe date?
        self.provider = PandasLocalFileProvider(self._exchange_code, self.history_path)

    def prepare(self):
        """Load history dataframe and load list of increments and update by them
        - may be additional class or function"""
        start_ts = self.detect_last_update()
        update_files = self.get_symbols_asset_by_timeframes_updates_fn(start_ts)


    def join_symbols_kind_diff_timeframes_update_files(self, timeframes_updates_files: dict[str, list]):
        from pprint import pprint
        update_files_by_period = {}
        for tm in timeframes_updates_files:
            timeframe = Timeframe(tm)
            for fn_path in timeframes_updates_files[tm]:
                fn_ts = self._parse_fn_timestamp(fn_path)
                fn_key = f'{fn_ts.year}-{fn_ts.month}'
                if fn_key not in update_files_by_period:
                    update_files_by_period[fn_key] = {}
                if timeframe.mult not in update_files_by_period[fn_key]:
                    update_files_by_period[fn_key][timeframe.mult] = []
                update_files_by_period[fn_key][timeframe.mult].append(fn_path)

        pprint(update_files_by_period)

        # job_results = [fn for fns in timeframes_updates_files.values() for fn in fns]
        # job_results = [fn for fns in timeframes_updates_files.values() for fn in fns]

        executor = ThreadPoolExecutor(
            max_workers=self.TASKS_LIMIT)  # Multiprocessing pool will be better due dataframe preparation
        # job_results = [executor.submit(pd.read_parquet, fn) for fn in timeframes_updates_files[timeframe] for timeframe in timeframes_updates_files]
        # job_results = [fn for fn in timeframes_updates_files.values()]
        # TODO Sort by years by parsing fn
        # TODO load by years or maybe by month if timeframe less than hour.
        # Fill gaps and for records from start of period - if nothing in prevous period - fill by them - may be
        # something like intermidiate timeframe period and search what near to this values and after convert timefrmae
        # job_results = [fn for fn in fns[0] for fns in timeframes_updates_files.values())]

        # job_results = [fn for fn in itertools.chain(fns for fns in timeframes_updates_files.values())]
        # pprint(job_results)
        # pprint.pprint(job_results[0])
        # for job_res in concurrent.futures.as_completed(job_results):
        #     asset = job_results[job_res]
        #     book_data = job_res.result()
        #     if isinstance(book_data, Exception):
        #         print(f'[ERROR] for {asset} book data: {book_data}')
        #         continue
        #     self._save_timeframe_book_update(book_data)

    def detect_last_update(self) -> pd.Timestamp | None:
        """Detect last history update date from fututres and options"""
        if self._full_history:
            return None
        asset_kinds_start_ts = []
        for asset_kind in self._asset_kinds:
            year_symbols = self._get_asset_history_years(asset_kind)
            asset_start_ts = self._get_start_timestamp(year_symbols, asset_kind)
            if asset_start_ts is not None:
                asset_kinds_start_ts.append(asset_start_ts)
        if not asset_kinds_start_ts:
            return None
        return min(asset_kinds_start_ts)

    def _get_start_timestamp(self, years_symbol: dict[int: list[str]], asset_kind: AssetKind) -> pd.Timestamp | None:
        """Search max timestamp in history files for sample of symbols"""
        max_symbols = 3  # Max symbols to search last date
        if years_symbol is None:
            return None
        years = list(years_symbol.keys())
        if len(years) == 0:
            return None
        max_year = max(years)
        year_symbols = years_symbol[max_year]
        if len(year_symbols) == 0:
            return None
        start_ts = None
        request_parma = RequestParameters(period_to=max_year, timeframe=self._timeframe)
        year_symbols = random.sample(year_symbols, max_symbols) if len(year_symbols) > max_symbols else year_symbols
        for symbol in year_symbols:
            if asset_kind.value == AssetKind.FUTURE.value:  # Use value there because it can be DeribitAssetKind and they will be different but the equal values
                df = self.provider.load_future_history(symbol, request_parma, columns=[FCl.TIMESTAMP.nm])
                start_ts_new = df[FCl.TIMESTAMP.nm].max()
            elif asset_kind.value == AssetKind.OPTION.value:
                df = self.provider.load_option_history(symbol, request_parma, columns=[OCl.TIMESTAMP.nm])
                start_ts_new = df[OCl.TIMESTAMP.nm].max()
            else:
                raise TypeError(f'{asset_kind} Is not supported')
            start_ts = start_ts_new if start_ts is None else max(start_ts, start_ts_new)
        return start_ts

    def _get_asset_history_years(self, asset_kind: AssetKind) -> dict[int: list[str]]:
        """Search for history data year files"""
        symbols = self.provider.get_symbols_list(asset_kind)
        if self._symbols:
            symbols = list(filter(lambda x: x in self._symbols, symbols))
        year_symbols = {}
        for symbol in symbols:
            years = self.provider.get_symbol_history_years(symbol, asset_kind, self._timeframe)
            if len(years) == 0:
                continue
            year = years[-1]
            if year not in year_symbols:
                year_symbols[year] = []
            year_symbols[year].append(symbol)
        return year_symbols

    def get_symbols_asset_by_timeframes_updates_fn(self,
                                                   start_ts: pd.Timestamp |
                                                             None = None) -> dict[str, dict[str, dict[str, list]]]:
        """Prepare list of underlying assets symbols"""

        max_depth = 3
        asset_kind_names = [at.value for at in self._asset_kinds]
        source_timeframe_names = [tm.value for tm in self._source_timeframes]
        updates_files = {}
        exchange_path = os.path.join(self.update_path, self._exchange_code)
        symbols = os.listdir(exchange_path)
        if self._symbols:
            symbols = list(filter(lambda x: x in self._symbols, symbols))
        for symbol in symbols:
            symbols_path = os.path.join(exchange_path, symbol)
            asset_kind_dirs = os.listdir(symbols_path)
            for asset_kind in asset_kind_dirs:
                if asset_kind in asset_kind_names:
                    asset_kind_path = os.path.join(symbols_path, asset_kind)
                    timeframes = os.listdir(asset_kind_path)
                    timeframes = filter(lambda x: x in source_timeframe_names, timeframes)
                    for tm in timeframes:
                        timeframe_path = os.path.join(asset_kind_path, tm)
                        for root_path, dirs, files in os.walk(timeframe_path):
                            if root_path[len(timeframe_path):].count(os.sep) > max_depth:
                                break
                            files = self._filter_files(files, start_ts)
                            if not files:
                                continue
                            updates_files = self._update_symbols_timeframes_fn(updates_files, symbol, asset_kind,
                                                                               tm, root_path, files)
        return updates_files

    def _filter_files(self, files: list[str], start_ts: pd.Timestamp | None) -> list[str]:
        if not files:
            return files
        if start_ts:
            return list(filter(lambda fn: self.update_fn_pattern.match(fn) and self._parse_fn_timestamp(fn) >= start_ts,
                               files))
        return list(filter(lambda fn: self.update_fn_pattern.match(fn), files))

    @staticmethod
    def _parse_fn_timestamp(fn) -> pd.Timestamp:
        fn = os.path.basename(fn)
        if 'T' in fn and fn.count('-') == 3:  # '25-01-22T00-06.parquet' format
            fn = ':'.join(fn.rsplit('-', 1))
        if fn[2] == '-' and not fn.startswith('20'):  # '25-01-30.parquet' format
            fn = f'20{fn}'
        return pd.Timestamp(fn[:-8], tz=datetime.UTC)

    @staticmethod
    def _update_symbols_timeframes_fn(updates_files: dict, symbol: str, asset_kind: str, timeframe: str,
                                      root_path: str, files: list[str] | Generator[str] | filter) -> dict:
        if symbol not in updates_files:
            updates_files[symbol] = {}
        if asset_kind not in updates_files[symbol]:
            updates_files[symbol][asset_kind] = {}
        if timeframe not in updates_files[symbol][asset_kind]:
            updates_files[symbol][asset_kind][timeframe] = [os.path.join(root_path, fn) for fn in sorted(files)]
        else:
            updates_files[symbol][asset_kind][timeframe].extend([os.path.join(root_path, fn) for fn in sorted(files)])
        return updates_files
