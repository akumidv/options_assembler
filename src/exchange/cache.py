from functools import wraps
import threading
import time
import sys
import hashlib
import psutil
from copy import deepcopy
import numpy as np
import pandas as pd


class Cache:
    EXPIRATION_DELTA_MINUTES = 30  # Cache storage timeout
    INVALIDATION_TIMES = 4  # Check INVALIDATION_TIMES per EXPIRATION_DELTA_MINUTES
    LOCK_TIMEOUT_SEC = 1
    MAX_MEM_DF_RATIO = 25  # max files in dedicated max_cache_memory

    validate_df: pd.DataFrame
    _lock_obj: threading.Lock
    _lock_expiry: float
    cached_data: dict
    ignore_keys: dict  # Potentially there can be a lot of records - it will use memory and will slow cache
    expiry: float
    expiry_timeout_sec: float
    invalidate_timeout_sec: float
    max_cache_memory: int
    max_cached_df_size: int

    def __init__(self, mem_size_limit_mb: int = 1_024, mem_ratio_percent_limit: int = 10,
                 is_new_day_ttl_reset: bool = False):
        self.validate_df: pd.DataFrame = pd.DataFrame(
            {'create': np.ndarray((0,), dtype=float),
             'update': np.ndarray((0,), dtype=float),
             'expiry': np.ndarray((0,), dtype=float),
             'data_type': np.ndarray((0,), dtype=str),
             'key': np.ndarray((0,), dtype=int),
             'size': np.ndarray((0,), dtype=int),
             'requests': np.ndarray((0,), dtype=int),
             }
        )
        self._is_new_day_ttl_reset = is_new_day_ttl_reset  # TODO not implemented
        self._lock_obj: threading.Lock = threading.Lock()
        self.cached_data: dict = dict()
        self.ignore_keys: dict = dict()
        self.expiry_timeout_sec: float = self.EXPIRATION_DELTA_MINUTES * 60
        self.invalidate_timeout_sec: float = self.expiry_timeout_sec / self.INVALIDATION_TIMES
        self._set_expiry()
        self._lock_expiry = time.time()
        self.max_cache_memory: int = int(
            min(mem_size_limit_mb * 1048576, self.get_total_memory() * mem_ratio_percent_limit / 100))
        self.max_cached_df_size: int = int(self.max_cache_memory / self.MAX_MEM_DF_RATIO)

    def it(self, func):
        @wraps(func)
        # @_develop_time_rec
        def wrapper(*args, **kwargs):
            def _norm_args(args_list: tuple, func_name: str):
                """
                args[0] is self for method of class and contain function as method, otherwise it is just function
                """
                if len(args_list) and hasattr(args_list[0], func_name):
                    return args_list[1:]
                return args_list

            data_type = func.__name__
            key = self._get_key(data_type, *(_norm_args(args, data_type)), **kwargs)
            if key in self.ignore_keys:
                df = func(*args, **kwargs)
            else:
                df = self._get_cache(key)
                if df is None:
                    df = func(*args, **kwargs)
                    if df is not None:
                        self._set_cache(df, key, data_type)
            if not isinstance(df, pd.DataFrame):
                return deepcopy(df)
            return df.copy(deep=True)

        return wrapper

    def _get_cache(self, key: int):
        self.invalidate_cache_by_expiry()
        if self._is_missed(key):
            return None
        df = self._get_cached_data(key)
        return df

    def _add_cache_ignore(self, key: int):
        self.ignore_keys[key] = True

    def _set_cache(self, df: pd.DataFrame, key: int, data_type: str) -> bool:
        res_value = False
        if key in self.ignore_keys:
            return res_value
        if not isinstance(df, pd.DataFrame) or not isinstance(self.validate_df, pd.DataFrame) or \
            not isinstance(self.cached_data, dict):
            return res_value
        cur_time = time.time()
        try:
            self._lock()
            size = self.get_df_memory_usage(df)
            if size > self.max_cached_df_size:
                self._add_cache_ignore(key)
            else:
                ttl = cur_time + self.expiry_timeout_sec
                # if self._is_new_day_ttl_reset:
                #     ttl = ttl # round to days
                self.validate_df.loc[key, self.validate_df.columns] = [cur_time, cur_time, ttl,
                                                                       data_type, key, size, 1]
                self._inefficiency_invalidation()
                if key in self.validate_df.index:
                    self.cached_data[key] = df.copy(deep=True)
                    res_value = True
        except Exception as err:
            print('[ERROR] set cache:', err, file=sys.stderr)
        self._unlock()
        return res_value

    @staticmethod
    def _get_key(data_type, *args, **kwargs) -> int:
        if kwargs:
            key_args = list(args) + [kwargs[key] for key in kwargs]
        else:
            key_args = list(args)
        key_str = data_type + '_' + (','.join([str(val) for val in key_args]) if len(key_args) != 0 else '')
        key = int(hashlib.sha256(key_str.encode('utf-8')).hexdigest(), 16) % (10 ** 16)
        return key

    def _get_cached_data(self, key: int) -> pd.DataFrame | None:
        df = self.cached_data.get(key)
        if df is not None:
            try:
                self.validate_df.loc[key, 'requests'] += 1
            except:
                pass
        return df

    def _set_expiry(self) -> None:
        self.expiry = time.time() + self.invalidate_timeout_sec

    def _lock(self):
        self._lock_obj.acquire(blocking=True, timeout=self.LOCK_TIMEOUT_SEC)

    def _unlock(self):
        try:
            if self._lock_obj.locked():
                self._lock_obj.release()  # Possible situation, when block was auto-release and blocked again - it will release not self block
        except:
            pass

    def invalidate_cache_by_expiry(self):
        cur_time = time.time()
        if self.expiry > cur_time:
            return True
        try:
            self._lock()
            self._set_expiry()
            self._invalidation()
        except:
            pass
        self._unlock()

    def _invalidation(self):
        cur_time = time.time()
        expired_df = self.validate_df[self.validate_df['expiry'] < cur_time]
        if not expired_df.empty:
            idxs = set(expired_df.index)
            del expired_df
            self._del_cache_keys(idxs)

    def _inefficiency_invalidation(self):
        score_df = self.validate_df.assign(_score=lambda x: x['requests'] / x['size']) \
            .sort_values(by=['_score'], ascending=False) \
            .assign(_cumsum=lambda x: x['size'].cumsum())
        inefficiency_df = score_df[score_df['_cumsum'] > self.max_cache_memory]
        if not inefficiency_df.empty:
            idxs = set(inefficiency_df.index)
            del inefficiency_df
            self._del_cache_keys(idxs)

    def _del_cache_keys(self, keys):
        self.validate_df.drop(keys, axis='index', inplace=True)
        for key in keys:
            if key in self.cached_data:
                del self.cached_data[key]

    def _is_missed(self, key: int) -> bool:
        if key in self.validate_df.index and key in self.cached_data:
            return False
        return True

    @staticmethod
    def get_df_memory_usage(df):
        mem_usage = sys.getsizeof(df)
        return mem_usage

    @staticmethod
    def get_total_memory():
        mem = psutil.virtual_memory()
        return mem.total
