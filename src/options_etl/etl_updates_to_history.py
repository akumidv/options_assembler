"""Update history timeframes from timeframe updates"""
import datetime
import random
import os
import re
import time

from typing import Generator
from collections import OrderedDict
# import threading
# import concurrent
# import itertools
# from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from options_lib.entities import Timeframe, AssetKind, OptionsColumns as OCl, FuturesColumns as FCl, SpotColumns as SCl
from options_lib.normalization.timeframe_resample import DEFAULT_RESAMPLE_MODEL, convert_to_timeframe
from provider import PandasLocalFileProvider, RequestParameters
from exchange.exchange_entities import ExchangeCode
from exchange import AbstractExchange


class EtlHistory:
    DEFAULT_PARAMETERS = {
        'low_memory_usage': False,
        'tasks_limit': 4,
        'ochl_model': False,
        'full_history': False,
        'resample_model': DEFAULT_RESAMPLE_MODEL,
        'resample_by_exchange_symbol': True,
        'source_fields': True,
        'update_history': True,
        'parallelize': False,
    }
    update_fn_pattern: re.Pattern = re.compile(r'^(\d{4}|\d{2})-\d{2}-\d{2}((T\d{2}-\d{2})|(T\d{2}))?\.parquet$')

    def __init__(self, exchange_code: ExchangeCode | str, history_path: str, update_path: str, timeframe: Timeframe,
                 symbols: list[str] | None = None, asset_kinds: list[AssetKind] | None = None,
                 params: dict[str] | None = None):
        self._exchange_code: str = exchange_code if isinstance(exchange_code, str) else exchange_code.name
        exchange_data_path = os.path.normpath(os.path.abspath(os.path.join(history_path, self._exchange_code)))
        os.makedirs(exchange_data_path, exist_ok=True)
        self.history_path: str = os.path.normpath(os.path.abspath(history_path))
        self.update_path: str = os.path.normpath(os.path.abspath(update_path))
        self._timeframe: Timeframe = timeframe
        self._symbols: list[str] | None = symbols
        self._asset_kinds = asset_kinds if asset_kinds is not None else [AssetKind.SPOT, AssetKind.FUTURES, AssetKind.OPTIONS]
        self._source_timeframes: list[Timeframe] = list(sorted([tm for tm in Timeframe
                                                                if tm.mult <= self._timeframe.mult],
                                                               key=lambda tm: tm.mult))

        self.provider = PandasLocalFileProvider(self._exchange_code, self.history_path)
        if isinstance(params, dict):
            if isinstance(params.get('resample_model'), dict):
                for key in self.DEFAULT_PARAMETERS['resample_model']:
                    if key not in params['resample_model']:
                        params['resample_model'][key] = self.DEFAULT_PARAMETERS['resample_model']
            for key in self.DEFAULT_PARAMETERS:
                if key not in params or type(params[key]) != type(self.DEFAULT_PARAMETERS[key]):
                    params[key] = self.DEFAULT_PARAMETERS[key]
            self._params = params
        else:
            self._params = self.DEFAULT_PARAMETERS
        self._low_memory_usage = self._params['low_memory_usage']
        self._parallelize = self._params['parallelize']
        self._ochl_model = self._params['ochl_model']
        self._tasks_limit = self._params['tasks_limit']
        self._full_history = self._params['full_history']
        self._resample_model: dict = self._params['resample_model']
        if self._params['source_fields']:
            self._resample_model = self._update_resample_model_for_source(self._resample_model)

        self._resample_by_exchange_symbol = self._params['resample_by_exchange_symbol']
        self._update_history = self._params['update_history']

    def prepare(self):
        """Load history dataframe and load list of increments and update by them
        - may be additional class or function"""
        start_ts = self.detect_last_update()
        if start_ts is not None:
            print('[INFO] Start date', start_ts)
        update_files = self.get_symbols_asset_by_timeframes_updates_fn(start_ts)

        for sym_idx, symbol in enumerate(update_files):
            start_tm = time.time()
            print(f'{sym_idx}/{len(update_files.keys())} {symbol} ')
            for asset_kind in update_files[symbol]:
                timeframes_updates_files = update_files[symbol][asset_kind]
                self.join_symbols_kind_diff_timeframes_update_files(timeframes_updates_files, symbol, asset_kind)
            print(f'  {symbol} {round((time.time() - start_tm), 2)} sec')

    def _add_ochl_columns(self, df):
        if self._ochl_model:
            for col in [OCl.OPEN.nm, OCl.CLOSE.nm, OCl.HIGH.nm, OCl.LOW.nm]:
                if col in self._resample_model and col not in df.columns:
                    df[col] = df[OCl.PRICE.nm]
        return df

    def _get_filepath(self, symbol: str, asset_kind: str | AssetKind, year: int) -> str:
        return self.provider.fn_path_prepare(symbol, asset_kind, self._timeframe, year)

    def _convert_timeframe(self, df: pd.DataFrame) -> pd.DataFrame:
        # TODO self._parallelize split to chanks by month and calc - only for options
        df = convert_to_timeframe(df, timeframe=self._timeframe,
                                  by_exchange_symbol=True, resample_model=self._resample_model)
        return df

    def join_symbols_kind_diff_timeframes_update_files(self, timeframes_updates_files: dict[str, list],
                                                       symbol: str, asset_kind: str | AssetKind):
        """Join different timeframes files and update history files"""
        max_period_records_num_for_optimize = 500_000  # 500_000 - ignore spot, futures anr 1h and more for options
        update_files_by_period = OrderedDict()
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
        periods_by_year = {}
        for year_month in update_files_by_period:
            year = int(year_month.split('-')[0])
            if year not in periods_by_year:
                periods_by_year[year] = []
            periods_by_year[year].append(year_month)
        for year in periods_by_year:
            year_dfs = []
            # TODO self._parallelize -> multiprocessing, due option resampling quite have operation due 3 grouping operations
            # prepare list of files, by chanks periods_fn = [[ts_fn],[ts_fn],...] - move function of loading files and contain and
            for period in sorted(periods_by_year[year]):
                period_dfs = []
                # TODO move to thread pool for loading files not one by one but by threads - faster
                for timeframe in sorted(update_files_by_period[period]):
                    for fn in sorted(update_files_by_period[period][timeframe]):
                        try:
                            df = pd.read_parquet(fn)
                            period_dfs.append(df)
                        except Exception as err:
                            err_text = f'[ERROR] for file {fn}: {err}'
                            print(err_text)
                            raise RuntimeError(err_text)
                period_df = pd.concat(period_dfs, ignore_index=True, copy=False)
                period_df = self._add_ochl_columns(period_df)
                if self._low_memory_usage and len(period_df) > max_period_records_num_for_optimize:  # reduce mem usage
                    period_df = self._convert_timeframe(period_df)

                year_dfs.append(period_df)
            fn = self._get_filepath(symbol, asset_kind, year)
            year_df = pd.concat(year_dfs, ignore_index=True, copy=False)
            early_timestamp: pd.Timestamp = year_df[OCl.TIMESTAMP.nm].min()
            last_timestamp: pd.Timestamp = year_df[OCl.TIMESTAMP.nm].max()
            if self._update_history and os.path.isfile(fn):
                df_prev = pd.read_parquet(fn)
                year_df = pd.concat([df_prev, year_df], ignore_index=True, copy=False)
            year_df = self._convert_timeframe(year_df)
            os.makedirs(os.path.dirname(fn), exist_ok=True)
            year_df.to_parquet(fn)
            print(f'  - updated {symbol}/{asset_kind} for {year} with record: {len(year_df)} and period '
                  f'{early_timestamp.tz_localize(None).isoformat(timespec="minutes")}-'
                  f'{last_timestamp.tz_localize(None).isoformat(timespec="minutes")}: '
                  f'{fn.replace(self.update_path, "")}')

    def detect_last_update(self) -> pd.Timestamp | None:
        """Detect last history update date from fututrers and options"""
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
            if asset_kind.value == AssetKind.FUTURES.value:  # Use value there because it can be DeribitAssetKind and they will be different but the equal values
                df = self.provider.load_future_history(symbol, request_parma, columns=[FCl.TIMESTAMP.nm])
                start_ts_new = df[FCl.TIMESTAMP.nm].max()
            elif asset_kind.value == AssetKind.OPTIONS.value:
                df = self.provider.load_option_history(symbol, request_parma, columns=[OCl.TIMESTAMP.nm])
                start_ts_new = df[OCl.TIMESTAMP.nm].max()
            else:
                raise TypeError(f'{asset_kind} Is not supported')
            start_ts = start_ts_new if start_ts is None else max(start_ts, start_ts_new)
        return start_ts

    def _get_asset_history_years(self, asset_kind: AssetKind) -> dict[int: list[str]]:
        """Search for history data year files"""
        symbols = self.provider.get_assets_list(asset_kind)
        if self._symbols:
            symbols = list(filter(lambda x: x in self._symbols, symbols))
        year_symbols = {}
        for symbol in symbols:
            years = self.provider.get_asset_history_years(symbol, asset_kind, self._timeframe)
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
            if os.path.isfile(symbols_path):
                continue
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
                                print(f'[WARNING] path more than deep {max_depth}', timeframe_path)
                                break
                            files = self._filter_files(files, start_ts, root_path)
                            if not files:
                                continue
                            updates_files = self._update_symbols_timeframes_fn(updates_files, symbol, asset_kind,
                                                                               tm, root_path, files)
        return updates_files

    def _filter_files(self, files: list[str], start_ts: pd.Timestamp | None = None,
                      root_path: str | None = None) -> list[str]:
        if not files:
            return files
        correct_files = list(filter(lambda fn: self.update_fn_pattern.match(fn), files))
        if len(correct_files) != len(files):
            print(f'[WARING] files have incorrect names {root_path}:', [fn for fn in files if fn not in correct_files])
        if start_ts:
            return list(filter(lambda fn:  self._parse_fn_timestamp(fn) >= start_ts,
                               correct_files))
        return correct_files

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

    @staticmethod
    def _update_resample_model_for_source(resample_model: dict) -> dict:
        prefix = AbstractExchange.SOURCE_PREFIX
        list_of_source_columns = [OCl.PRICE.nm, OCl.LAST.nm, OCl.ASK.nm, OCl.BID.nm, OCl.EXCHANGE_PRICE.nm]
        for col in list_of_source_columns:
            source_col = f'{prefix}_{col}'
            if col in resample_model and source_col not in resample_model:
                resample_model[source_col] = resample_model[col]
        return resample_model
