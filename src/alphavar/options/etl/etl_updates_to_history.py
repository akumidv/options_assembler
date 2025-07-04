"""Update history timeframes from timeframe updates"""

import datetime
import logging
import os
import random
import re
import time
from collections import OrderedDict
from collections.abc import Generator

# import threading
# import concurrent
# import itertools
# from concurrent.futures import ThreadPoolExecutor
import pandas as pd

from alphavar.core.dictionary import InstrumentKind
from alphavar.io.exchange import AbstractExchange, get_exchange_class
from alphavar.io.exchange.exchange_entities import ExchangeCode
from alphavar.io.provider import PandasLocalFileProvider, RequestParameters, read_reference, write_reference
from alphavar.options.dictionary import ContractKind, OptionsTerm, Timeframe
from alphavar.options.lib.normalization import validate_path_segment
from alphavar.options.lib.normalization.timeframe_resample import DEFAULT_RESAMPLE_MODEL, convert_to_timeframe
from alphavar.options.lib.reference import (
    CONTRACT_KEY_COLUMNS,
    CONTRACT_REF_COLUMNS,
    append_on_change,
    split_reference,
)

logger = logging.getLogger(__name__)


class EtlHistory:
    DEFAULT_PARAMETERS = {
        "low_memory_usage": False,
        "tasks_limit": 4,
        "ochl_model": False,
        "full_history": False,
        "resample_model": DEFAULT_RESAMPLE_MODEL,
        "resample_by_exch_symbol": True,
        "source_fields": True,
        "update_history": True,
        "update_reference": True,
        "slim_series": False,
        "parallelize": False,
    }
    update_fn_pattern: re.Pattern = re.compile(r"^(\d{4}|\d{2})-\d{2}-\d{2}((T\d{2}-\d{2})|(T\d{2}))?\.parquet$")

    def __init__(
        self,
        exchange_code: ExchangeCode | str,
        history_path: str,
        update_path: str,
        timeframe: Timeframe,
        asset_codes: list[str] | None = None,
        asset_kinds: list[InstrumentKind] | None = None,
        params: dict[str] | None = None,
    ):
        self._exchange_code: str = exchange_code if isinstance(exchange_code, str) else exchange_code.name
        validate_path_segment(self._exchange_code, field="exchange_code")
        # Exchange class for venue-native -> canonical kind resolution (ADR 0001 / R2.2).
        self._exchange_cls: type[AbstractExchange] = get_exchange_class(exchange_code)
        exchange_data_path = os.path.normpath(os.path.abspath(os.path.join(history_path, self._exchange_code)))
        os.makedirs(exchange_data_path, exist_ok=True)
        self.history_path: str = os.path.normpath(os.path.abspath(history_path))
        self.update_path: str = os.path.normpath(os.path.abspath(update_path))
        self._timeframe: Timeframe = timeframe
        self._asset_codes: list[str] | None = asset_codes
        # Canonical instrument kinds to migrate (singular).
        self._instrument_kinds: list[InstrumentKind] = (
            asset_kinds
            if asset_kinds is not None
            else [InstrumentKind.SPOT, InstrumentKind.FUTURE, InstrumentKind.OPTION]
        )
        self._source_timeframes: list[Timeframe] = list(
            sorted([tm for tm in Timeframe if tm.mult <= self._timeframe.mult], key=lambda tm: tm.mult)
        )

        self.provider = PandasLocalFileProvider(self._exchange_code, self.history_path)
        if isinstance(params, dict):
            if isinstance(params.get("resample_model"), dict):
                for key in self.DEFAULT_PARAMETERS["resample_model"]:
                    if key not in params["resample_model"]:
                        params["resample_model"][key] = self.DEFAULT_PARAMETERS["resample_model"]
            for key in self.DEFAULT_PARAMETERS:
                if key not in params or type(params[key]) is not type(self.DEFAULT_PARAMETERS[key]):
                    params[key] = self.DEFAULT_PARAMETERS[key]
            self._params = params
        else:
            self._params = self.DEFAULT_PARAMETERS
        self._low_memory_usage = self._params["low_memory_usage"]
        self._parallelize = self._params["parallelize"]
        self._ochl_model = self._params["ochl_model"]
        self._tasks_limit = self._params["tasks_limit"]
        self._full_history = self._params["full_history"]
        self._resample_model: dict = self._params["resample_model"]
        if self._params["source_fields"]:
            self._resample_model = self._update_resample_model_for_source(self._resample_model)

        self._resample_by_exch_symbol = self._params["resample_by_exch_symbol"]
        self._update_history = self._params["update_history"]
        self._update_reference = self._params["update_reference"]
        # Slimming drops the now-extracted reference columns from the stored series; it is only
        # safe when the reference is being written too (else the dropped columns are unrecoverable).
        self._slim_series = self._params["slim_series"] and self._update_reference

    def prepare(self):
        """Load history dataframe and load list of increments and update by them
        - may be additional class or function"""
        start_ts = self.detect_last_update()
        if start_ts is not None:
            logger.info("Start date %s", start_ts)
        update_files = self.get_asset_codes_by_timeframes_updates_fn(start_ts)

        for sym_idx, asset_code in enumerate(update_files):
            start_tm = time.time()
            logger.info("%d/%d %s", sym_idx, len(update_files.keys()), asset_code)
            for asset_kind in update_files[asset_code]:
                timeframes_updates_files = update_files[asset_code][asset_kind]
                self.join_symbols_kind_diff_timeframes_update_files(timeframes_updates_files, asset_code, asset_kind)
            logger.info("  %s %.2f sec", asset_code, time.time() - start_tm)

    def _add_ochl_columns(self, df):
        if self._ochl_model:
            for col in [OptionsTerm.OPEN, OptionsTerm.CLOSE, OptionsTerm.HIGH, OptionsTerm.LOW]:
                if col in self._resample_model and col not in df.columns:
                    df[col] = df[OptionsTerm.PRICE]
        return df

    def _get_filepath(self, asset_code: str, asset_kind: str | InstrumentKind, year: int) -> str:
        return self.provider.fn_path_prepare(asset_code, asset_kind, self._timeframe, year)

    def _convert_timeframe(self, df: pd.DataFrame) -> pd.DataFrame:
        # TODO self._parallelize split to chanks by month and calc - only for options
        df = convert_to_timeframe(
            df, timeframe=self._timeframe, by_exch_symbol=True, resample_model=self._resample_model
        )
        return df

    def join_symbols_kind_diff_timeframes_update_files(
        self, timeframes_updates_files: dict[str, list], asset_code: str, asset_kind: str | InstrumentKind
    ):
        """Join different timeframes files and update history files"""
        max_period_records_num_for_optimize = 500_000  # 500_000 - ignore spot, futures anr 1h and more for options
        update_files_by_period = OrderedDict()
        for tm in timeframes_updates_files:
            timeframe = Timeframe(tm)
            for fn_path in timeframes_updates_files[tm]:
                fn_ts = self._parse_fn_timestamp(fn_path)
                fn_key = f"{fn_ts.year}-{fn_ts.month}"
                if fn_key not in update_files_by_period:
                    update_files_by_period[fn_key] = {}
                if timeframe.mult not in update_files_by_period[fn_key]:
                    update_files_by_period[fn_key][timeframe.mult] = []
                update_files_by_period[fn_key][timeframe.mult].append(fn_path)
        periods_by_year = {}
        for year_month in update_files_by_period:
            year = int(year_month.split("-")[0])
            if year not in periods_by_year:
                periods_by_year[year] = []
            periods_by_year[year].append(year_month)
        for year in periods_by_year:
            year_dfs = []
            # TODO self._parallelize -> multiprocessing (option resampling does 3 grouping ops).
            # Prepare a list of files by chunks: periods_fn = [[ts_fn], [ts_fn], ...] — move
            # the file-loading function out.
            for period in sorted(periods_by_year[year]):
                period_dfs = []
                # TODO move to thread pool for loading files not one by one but by threads - faster
                for timeframe in sorted(update_files_by_period[period]):
                    for fn in sorted(update_files_by_period[period][timeframe]):
                        try:
                            df = pd.read_parquet(fn)
                            period_dfs.append(df)
                        except Exception as err:
                            err_text = f"for file {fn}: {err}"
                            logger.error("%s", err_text)
                            raise RuntimeError(err_text) from err
                period_df = pd.concat(period_dfs, ignore_index=True, copy=False)
                period_df = self._add_ochl_columns(period_df)
                if self._low_memory_usage and len(period_df) > max_period_records_num_for_optimize:  # reduce mem usage
                    period_df = self._convert_timeframe(period_df)

                year_dfs.append(period_df)
            fn = self._get_filepath(asset_code, asset_kind, year)
            year_df = pd.concat(year_dfs, ignore_index=True, copy=False)
            early_timestamp: pd.Timestamp = year_df[OptionsTerm.TIMESTAMP].min()
            last_timestamp: pd.Timestamp = year_df[OptionsTerm.TIMESTAMP].max()
            if self._update_history and os.path.isfile(fn):
                df_prev = pd.read_parquet(fn)
                year_df = pd.concat([df_prev, year_df], ignore_index=True, copy=False)
            year_df = self._convert_timeframe(year_df)
            os.makedirs(os.path.dirname(fn), exist_ok=True)
            if self._update_reference:
                self._fold_reference(year_df, asset_code)  # sidecar first, so slim is reversible
            self._to_stored_series(year_df).to_parquet(fn)
            logger.info(
                "  - updated %s/%s for %s with record: %d and period %s-%s: %s",
                asset_code,
                asset_kind,
                year,
                len(year_df),
                early_timestamp.tz_localize(None).isoformat(timespec="minutes"),
                last_timestamp.tz_localize(None).isoformat(timespec="minutes"),
                fn.replace(self.update_path, ""),
            )

    def _to_stored_series(self, df: pd.DataFrame) -> pd.DataFrame:
        """Slim the series for storage when enabled: drop the reference columns now held in the
        sidecar (`split_reference().quotes`), leaving the per-row time series + the contract key
        (the load path reattaches them, T25). Options-only; a no-op (returns ``df`` unchanged)
        when slimming is off or the frame carries no extractable reference. Guarded — a failure
        keeps the wide frame rather than risking a lossy write."""
        if not self._slim_series or OptionsTerm.ASSET_CODE not in df.columns or OptionsTerm.STRIKE not in df.columns:
            return df
        try:
            return split_reference(df).quotes
        except Exception as err:  # never write a lossy series; fall back to the wide frame
            logger.error("slim_series failed: %s", err)
            return df

    def _fold_reference(self, df: pd.DataFrame, asset_code: str) -> None:
        """Fold the options reference from a freshly-written history batch into the asset's
        SCD-2 sidecar (`_meta.parquet` + `_asset.json`) via `append_on_change` (R4.6, T25 inc.4B).

        Options-only (needs the contract key columns) and **additive** — only the sidecar is
        written, never the series. Guarded: a reference failure is logged, never aborts the
        history write. `when` is the batch's latest observation (the moment we have evidence for).
        """
        if OptionsTerm.ASSET_CODE not in df.columns or OptionsTerm.STRIKE not in df.columns:
            return  # not an options frame -> no contract-level reference to fold
        try:
            split = split_reference(df)
            if split.contracts.empty:
                return
            asset_dir = os.path.join(self.provider.exchange_data_path, asset_code)
            _prev_asset, history = read_reference(asset_dir)
            when = df[OptionsTerm.TIMESTAMP].max()
            key_cols = [c for c in CONTRACT_KEY_COLUMNS if c in split.contracts.columns]
            attr_cols = [c for c in CONTRACT_REF_COLUMNS if c in split.contracts.columns]
            history = append_on_change(history, split.contracts, when, key_cols, attr_cols)
            write_reference(split.asset, history, asset_dir)
        except Exception as err:  # reference is best-effort; never block the history write
            logger.error("reference fold failed for %s: %s", asset_code, err)

    def detect_last_update(self) -> pd.Timestamp | None:
        """Detect last history update date from fututrers and options"""
        if self._full_history:
            return None
        asset_kinds_start_ts = []
        for asset_kind in self._instrument_kinds:
            year_asset_codes = self._get_asset_history_years(asset_kind)
            asset_start_ts = self._get_start_timestamp(year_asset_codes, asset_kind)
            if asset_start_ts is not None:
                asset_kinds_start_ts.append(asset_start_ts)
        if not asset_kinds_start_ts:
            return None
        return min(asset_kinds_start_ts)

    def _get_start_timestamp(
        self, years_symbol: dict[int : list[str]], asset_kind: InstrumentKind
    ) -> pd.Timestamp | None:
        """Search max timestamp in history files for sample of asset_codes"""
        max_asset_codes = 3  # Max asset_codes to search last date
        if years_symbol is None:
            return None
        years = list(years_symbol.keys())
        if len(years) == 0:
            return None
        max_year = max(years)
        year_asset_codes = years_symbol[max_year]
        if len(year_asset_codes) == 0:
            return None
        start_ts = None
        request_parma = RequestParameters(period_to=max_year, timeframe=self._timeframe)
        if len(year_asset_codes) > max_asset_codes:
            year_asset_codes = random.sample(year_asset_codes, max_asset_codes)
        for asset_code in year_asset_codes:
            if asset_kind == InstrumentKind.FUTURE:
                df = self.provider.load_futures_history(asset_code, request_parma, columns=[OptionsTerm.TIMESTAMP])
                start_ts_new = df[OptionsTerm.TIMESTAMP].max()
            elif asset_kind == InstrumentKind.OPTION:
                df = self.provider.load_options_history(asset_code, request_parma, columns=[OptionsTerm.TIMESTAMP])
                start_ts_new = df[OptionsTerm.TIMESTAMP].max()
            else:
                raise TypeError(f"{asset_kind} Is not supported")
            start_ts = start_ts_new if start_ts is None else max(start_ts, start_ts_new)
        return start_ts

    def _get_asset_history_years(self, asset_kind: InstrumentKind) -> dict[int : list[str]]:
        """Search for history data year files"""
        asset_codes = self.provider.get_assets_list(asset_kind)
        if self._asset_codes:
            asset_codes = list(filter(lambda x: x in self._asset_codes, asset_codes))
        year_asset_codes = {}
        for asset_code in asset_codes:
            years = self.provider.get_asset_history_years(asset_code, asset_kind, self._timeframe)
            if len(years) == 0:
                continue
            year = years[-1]
            if year not in year_asset_codes:
                year_asset_codes[year] = []
            year_asset_codes[year].append(asset_code)
        return year_asset_codes

    def get_asset_codes_by_timeframes_updates_fn(
        self, start_ts: pd.Timestamp | None = None
    ) -> dict[str, dict[str, dict[str, list]]]:
        """Prepare list of underlying assets asset_codes"""

        max_depth = 3
        source_timeframe_names = [tm.value for tm in self._source_timeframes]
        updates_files = {}
        exchange_path = os.path.join(self.update_path, self._exchange_code)
        asset_codes = os.listdir(exchange_path)
        if self._asset_codes:
            asset_codes = list(filter(lambda x: x in self._asset_codes, asset_codes))
        for asset_code in asset_codes:
            symbols_path = os.path.join(exchange_path, asset_code)
            if os.path.isfile(symbols_path):
                continue
            # Update dirs are venue-native kind tokens (ADR 0001). Keep only those that map
            # to a requested vanilla instrument kind; combos (contract_kind != vanilla) are
            # not migrated into the kind-partitioned history.
            asset_kind_dirs = os.listdir(symbols_path)
            for asset_kind in asset_kind_dirs:
                resolved = self._exchange_cls.resolve_instrument_kind(asset_kind)
                if resolved is None:
                    continue
                instrument_kind, contract_kind = resolved
                if contract_kind == ContractKind.VANILLA and instrument_kind in self._instrument_kinds:
                    asset_kind_path = os.path.join(symbols_path, asset_kind)
                    timeframes = os.listdir(asset_kind_path)
                    timeframes = filter(lambda x: x in source_timeframe_names, timeframes)
                    for tm in timeframes:
                        timeframe_path = os.path.join(asset_kind_path, tm)
                        for root_path, _dirs, files in os.walk(timeframe_path):
                            if root_path[len(timeframe_path) :].count(os.sep) > max_depth:
                                logger.warning("path more than deep %s %s", max_depth, timeframe_path)
                                break
                            files = self._filter_files(files, start_ts, root_path)
                            if not files:
                                continue
                            updates_files = self._update_symbols_timeframes_fn(
                                updates_files, asset_code, asset_kind, tm, root_path, files
                            )
        return updates_files

    def _filter_files(
        self, files: list[str], start_ts: pd.Timestamp | None = None, root_path: str | None = None
    ) -> list[str]:
        if not files:
            return files
        correct_files = list(filter(lambda fn: self.update_fn_pattern.match(fn), files))
        if len(correct_files) != len(files):
            logger.warning(
                "files have incorrect names %s: %s", root_path, [fn for fn in files if fn not in correct_files]
            )
        if start_ts:
            return list(filter(lambda fn: self._parse_fn_timestamp(fn) >= start_ts, correct_files))
        return correct_files

    @staticmethod
    def _parse_fn_timestamp(fn) -> pd.Timestamp:
        fn = os.path.basename(fn)
        if "T" in fn and fn.count("-") == 3:  # '25-01-22T00-06.parquet' format
            fn = ":".join(fn.rsplit("-", 1))
        if fn[2] == "-" and not fn.startswith("20"):  # '25-01-30.parquet' format
            fn = f"20{fn}"
        return pd.Timestamp(fn[:-8], tz=datetime.UTC)

    @staticmethod
    def _update_symbols_timeframes_fn(
        updates_files: dict,
        asset_code: str,
        asset_kind: str,
        timeframe: str,
        root_path: str,
        files: list[str] | Generator[str] | filter,
    ) -> dict:
        if asset_code not in updates_files:
            updates_files[asset_code] = {}
        if asset_kind not in updates_files[asset_code]:
            updates_files[asset_code][asset_kind] = {}
        period_files = [os.path.join(root_path, fn) for fn in sorted(files)]
        if timeframe not in updates_files[asset_code][asset_kind]:
            updates_files[asset_code][asset_kind][timeframe] = period_files
        else:
            updates_files[asset_code][asset_kind][timeframe].extend(period_files)
        return updates_files

    @staticmethod
    def _update_resample_model_for_source(resample_model: dict) -> dict:
        suffix = AbstractExchange.RAW_SUFFIX
        list_of_raw_columns = [
            OptionsTerm.PRICE,
            OptionsTerm.LAST,
            OptionsTerm.ASK,
            OptionsTerm.BID,
            OptionsTerm.EXCH_MARK_PRICE,
        ]
        for col in list_of_raw_columns:
            raw_col = f"{col}{suffix}"
            if col in resample_model and raw_col not in resample_model:
                resample_model[raw_col] = resample_model[col]
        return resample_model
