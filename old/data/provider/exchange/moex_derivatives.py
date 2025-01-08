import asyncio
import os
import datetime
import aiohttp

import pandas as pd
from typing import Literal
from provider.connector import Connector
from provider.data_entities import AssetType, OptionType
from functools import wraps
import logging
from itertools import chain

IS_DEVELOP = os.environ.get('PYTHON_MODE') == 'development' or os.environ.get('PYTEST_VERSION') is not None

DEV_CACHE_PATH = os.path.normpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../.cache_data/MOEX'))

if IS_DEVELOP:
    os.makedirs(DEV_CACHE_PATH, exist_ok=True)

TYPE_TO_MOEX_ASSET_TYPE = {
    AssetType.FUTURE.code: 'futures',
    AssetType.SHARE.code: "share",
    AssetType.COMMODITIES.code: "commodity",
    AssetType.CURRENCY.code: 'currency',
    AssetType.INDEX.code: 'index',
}



def day_cache_async(func):
    @wraps(func)
    async def wrapped(*args, **kwargs):
        def _get_key(data_type) -> tuple[int, str]:  # , *_args, **_kwargs
            key_args = list(args[1:]) + [kwargs[key] for key in kwargs] if kwargs else list(args[1:])
            if any([isinstance(val, pd.DataFrame) for val in key_args]):
                return None
            params_str = (','.join([str(val) for val in key_args]) if len(key_args) != 0 else '')
            key_str = data_type if not params_str else data_type + '_' + params_str
            # get_sha256 = lambda text_str: int(hashlib.sha256(text_str.encode('utf-8')).hexdigest(), 16) % (10 ** 16)
            # key = get_sha256(key_str)
            return key_str  # key
        key_name = _get_key(func.__name__)
        fn_path = None
        if key_name:
            fn_path = os.path.join(DEV_CACHE_PATH, f'{key_name}.parquet')
            if os.path.exists(fn_path) and \
                    datetime.date.today() != datetime.date.fromtimestamp(os.path.getmtime(fn_path)) + \
                    datetime.timedelta(days=1):
                print(f'[INFO] From cache {func.__name__}:', fn_path)
                return pd.read_parquet(fn_path)
        res = await func(*args, **kwargs)
        if isinstance(res, pd.DataFrame) and fn_path is not None:
            res.to_parquet(fn_path)
        return res

    return wrapped

def connect_check111(func):
    @wraps(func)
    async def wrapped(self, *args, **kwargs):
        print('self', self)
        print('args', *args)
        if not self.is_connected:
            self._client = aiohttp.ClientSession()
        elif self._client.closed:
            self._client = aiohttp.ClientSession()
            logging.warning('REOPERN MOEX')
        else:
            logging.warning('MOEX already connected')
        return await func(self, *args, **kwargs)
    return wrapped

class MoexOptions(Connector):
    """
    API Swagger Doc https://iss.moex.com/iss/apps/option-calc/v1/docs
    MOEX Base API https://iss.moex.com/iss/reference/

    """
    _client: aiohttp.ClientSession | None = None
    log: logging.Logger
    BASE_URL = 'https://iss.moex.com/iss/apps/option-calc/v1'

    # async def __aenter__(self):
    #     self._client = aiohttp.ClientSession()
    #     print('CONNECTED')
    #     return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not self.is_connected:
            await self._client.close()
            self._client = None

    def __init__(self, credential: dict | None = None, log: logging.Logger = None):
        super().__init__('MOEX Derivatives', credential, log)

    @property
    def is_connected(self):
        return self._client is not None and not self._client.closed

    def connect(self):
        if not self.is_connected:
            self._client = aiohttp.ClientSession()
            print('CONNECT')
        elif self._client.closed:
            self._client = aiohttp.ClientSession()
            print('REOPERN MOEX')
            logging.warning('REOPERN MOEX')
        else:
            logging.warning('MOEX already connected')

    @staticmethod
    def _connect_check(func):
        @wraps(func)
        async def wrapped(self, *args, **kwargs):
            self.connect()
            return await func(self, *args, **kwargs)
        return wrapped

    async def disconnect(self):
        if self._client is not None:
            await self._client.close()
            self._client = None
        else:
            logging.warning('MOEX already closed')

    async def get_working_time(self):
        """
        https://iss.moex.com/iss/engines/futures.xml
        TODO get calendar
        check is in working period - than return minute
        if not return last close time
        """

    @day_cache_async
    @_connect_check
    async def get_calendar(self):
        """
        https://www.moex.com/n64121 роспись
        проводиться в официальные выходные дни 3–5 и 8 января, 29 и 30 апреля, 10 мая и 30 декабря 2024 года.
        В дни официальных государственных праздников 1–2 января, 7 января, 23 февраля, 8 марта, 1 мая, 9 мая, 12 июня, 4 ноября 2024 года торги на рынках Московской биржи не проводятся.
        Обращаем ваше внимание, что 27 апреля, 2 ноября и 28 декабря 2024 года ­– рабочие субботы, торги проводятся в обычном режиме.
        https://iss.moex.com/iss/engines/futures.xml

        https://xmlcalendar.ru/ - календарь https://xmlcalendar.ru/data/ru/2024/calendar.json
        * предпразничный день с сокр. на 1 час рабочим днем. + рабочи ставшие выходными, transitions - выходные, которые стали рабочими.
        """
        import io
        resp = await self._client.get('https://iss.moex.com/iss/engines/futures.xml')
        resp_text = await resp.text()
        df_current_week = pd.read_xml(io.StringIO(resp_text), xpath="data[@id='timetable']/rows/row")
        print(df_current_week)
        df_working_days_and_holidays = pd.read_xml(io.StringIO(resp_text), xpath="data[@id='dailytable']/rows/row") # Не надежный - последняя дата 06.12
        print(df_working_days_and_holidays)
        # verify by https://www.moex.com/n64121
        # df = pd.read_xml('https://iss.moex.com/iss/engines/futures.xml', xpath="data[@id='timetable']/rows/row", )
        # TODO GENERATE HOLIDAYS FROM exchange_calendars
        # COMBINE THEM WITH MOEX response and return calendar
        return df_working_days_and_holidays


    @_connect_check
    async def _get(self, path: str, params: dict | None = None, base_url: str | None = None) -> list | dict:
        url = f'{self.BASE_URL}/{path}' if base_url is None else f'{base_url}/{path}'
        resp = await self._client.get(url, params=params)
        response = await resp.json()
        if resp.status == 422:
            raise ValueError(f'[ERROR] in request format: {resp.status} {url} {params} {response.get("detail")}')
        elif resp.status != 200:
            err_text = response.get("detail") if 'detail' in response else response
            raise RuntimeError(f'Request error: {resp.status} {url} {params} {err_text}')
        return response

    def _data_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
        df['request_time'] = datetime.datetime.now(tz=datetime.timezone.utc)
        df = self._rename_columns(df)
        df = self._convert_values(df)
        df = self._convert_date(df)
        if 'exchange_symbol' in df.columns and 'underlying_type' in df.columns:
            df['underlying_id'] = df['exchange_symbol'] + '.' + df['underlying_type'].str.upper() + '/MOEX'
        return df

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        columns_rename = self._filter_dict_by_column(df.columns,
                                                     {'asset_code': 'exchange_symbol', 'asset_type': 'underlying_type',
                                                      'asset_subtype': 'underlying_subtype', 'title': 'name',
                                                      'secid': 'exchange_option_id', # is not uniq due it do not contain full year
                                                      'futures_code': 'exchange_future_id', # is not uniq due it do not contain full year
                                                      'optionseries_code': 'exchange_option_series_id',
                                                      'theorprice': 'calculated_option_price',
                                                      'lastprice': 'last_price',
                                                      'settleprice': 'settlement_price'

                                                      })
        if columns_rename:
            df.rename(columns=columns_rename, inplace=True)
        return df

    def _convert_values(self, df: pd.DataFrame, columns: list | None = None):
        type_replace_dict = {'futures': AssetType.FUTURE.code,
                             'index': AssetType.INDEX.code,
                             'share': AssetType.SHARE.code,
                             'currency': AssetType.CURRENCY.code,
                             'commodity': AssetType.COMMODITIES.code}
        columns_retype = self._filter_list_by_column(df.columns, columns if columns else ['underlying_type',
                                                                                          'underlying_subtype'])
        if type_replace_dict:
            df[columns_retype] = df[columns_retype].replace(type_replace_dict)
        if 'option_type' in df.columns:
            df['option_type'] = df['option_type'].replace({OptionType.CALL.value: OptionType.CALL.code,
                                                           OptionType.PUT.value: OptionType.PUT.code})
        return df

    @staticmethod
    def _filter_list_by_column(columns: pd.Index, rename_list: list):
        return [col for col in rename_list if col in columns]

    @staticmethod
    def _filter_dict_by_column(columns: pd.Index, rename_dict: dict):
        return {col: rename_dict[col] for col in rename_dict if col in columns}

    def _convert_date(self, df: pd.DataFrame, columns: list | None = None):
        date_columns = self._filter_list_by_column(df.columns, columns if columns is not None else [
            'expiration_date', 'updatetime', 'datetime'
        ])
        df[date_columns] = df[date_columns].apply(pd.to_datetime, axis='columns', format='mixed')
        return df

    @day_cache_async
    async def get_symbol_list(self, underlying_type: Literal['futures', 'currency', 'share', 'index',
                                                             'commodity'] | None = None) -> pd.DataFrame:
        """
             exchange_symbol name               underlying_type underlying_subtype  underlying_id
        0    ABIO   ABIO (iАРТГЕН ао)   s    None     ABIO.S/MOEX
        3    AFLT   AFLT (Аэрофлот)    s    None     AFLT.S/MOEX
        4    AFLT   AFLT (фьючерс)     f    s        AFLT.F/MOEX
        89   WHEAT  WHEAT (фьючерс)    f    g        WHEAT.F/MOEX
        90   YDEX   YDEX (ЯНДЕКС)      s    None     YDEX.S/MOEX
        """
        if underlying_type is not None and underlying_type not in ['futures', 'currency', 'share', 'index', 'commodity']:
            raise ValueError(f'{underlying_type} is unknown type')
        params = {'asset_type': underlying_type} if underlying_type else None
        underlyings = await self._get('assets', params=params)
        und_df = pd.DataFrame.from_records(underlyings)
        und_df = self._data_normalization(und_df)
        return und_df

    @day_cache_async
    async def get_futures_list(self, futures: list | None = None, expiration_dates: list | None = None) -> pd.DataFrame:
        """
      local_futures_id exchange_symbol type expiration_date underlying_id
0          AKZ4   AFKS    f      2024-12-20   AFKS.F/MOEX
1          AKH5   AFKS    f      2025-03-21   AFKS.F/MOEX
6          BRX4     BR    f      2024-11-01     BR.F/MOEX
7          BRZ4     BR    f      2024-12-02     BR.F/MOEX
        """
        if futures is None:
            und_df = await self.get_symbol_list(underlying_type='futures')
            futures = list(und_df[und_df['underlying_type'] == AssetType.FUTURE.code]['exchange_symbol'].unique())
            expiration_dates = [None] * len(futures)
        elif expiration_dates is None or len(futures) != len(expiration_dates):
            expiration_dates = [None] * len(futures)
        tasks = []
        for future_code, expiration_date in zip(futures, expiration_dates):
            params = {'expiration_date': expiration_date} if expiration_date is not None else None
            tasks.append(self._get(f'assets/{future_code}/futures', params))
        futures = await self._gather_with_concurrency(*tasks)
        futures = list(chain.from_iterable(futures))
        futures_df = pd.DataFrame.from_records(futures)
        futures_df = self._data_normalization(futures_df)
        return futures_df

    @day_cache_async
    async def get_options_list(self, symbols_df: pd.DataFrame | None = None) -> pd.DataFrame:
        """
        Time 17 sec (6 thread)
         exchange_option_id exchange_symbol underlying_type local_futures_id expiration_date series_type  strike option_type underlying_id
0       IS76CJ4D   ABIO    s         None      2024-10-23           W    76.0           c   ABIO.S/MOEX  currency # ABIO/S@MOEX
1       IS76CV4D   ABIO    s         None      2024-10-23           W    76.0           p   ABIO.S/MOEX  RUB
104     AK5500BL4   AFKS    f         AKZ4      2024-12-18           Q   5500.0           c   AFKS.F/MOEX RUB
105     AK5500BX4   AFKS    f         AKZ4      2024-12-18           Q   5500.0           p   AFKS.F/MOEX RUB
1550    BR44BJ4D     BR    f         BRX4      2024-10-24           W    44.0           c     BR.F/MOEX RUB
1551    BR44BV4D     BR    f         BRX4      2024-10-24           W    44.0           p     BR.F/MOEX RUB
737     BR106BR5     BR    f         BRN5      2025-06-25           M   106.0           p     BR.F/MOEX RUB
3738    BR107BF5     BR    f         BRN5      2025-06-25           M   107.0           c     BR.F/MOEX RUB
5056    CR12.6CJ4D  CNYRUB_TOM  m     None      2024-10-24           W    12.6           c    CNYRUB_TOM.M/MOEX RUB
5057    CR12.6CV4D  CNYRUB_TOM  m     None      2024-10-24           W    12.6           p    CNYRUB_TOM.M/MOEX RUB
10856  IM2400CJ4D  IMOEX    i         None      2024-10-23           W  2400.0           c  IMOEX.I/MOEX RUB
10857  IM2400CV4D  IMOEX    i         None      2024-10-23           W  2400.0           p  IMOEX.I/MOEX RUB
8650  GL7800CJ4D  GLDRUB_TOM    g     None      2024-10-24           W  7800.0           c  GLDRUB_TOM.G/MOEX RUB
8651  GL7800CV4D  GLDRUB_TOM    g     None      2024-10-24           W  7800.0           p  GLDRUB_TOM.G/MOEX RUB
        """
        if symbols_df is None:
            all_symbols_df = await self.get_symbol_list()
            symbols_df = all_symbols_df[['exchange_symbol', 'underlying_type']]
        tasks = []
        for idx, opt_row in symbols_df.iterrows():
            params = {'asset_type': TYPE_TO_MOEX_ASSET_TYPE.get(opt_row['underlying_type'])}
            if 'expiration_date' in symbols_df:
                params.update = {'expiration_date': opt_row['expiration_date']}
            tasks.append(self._get(f'assets/{opt_row["exchange_symbol"]}/options', params))
        results = await self._gather_with_concurrency(*tasks)
        res = list(chain.from_iterable(results))
        options_list_df = pd.DataFrame.from_records(res)
        options_list_df = self._data_normalization(options_list_df)
        options_list_df['currency'] = 'RUB'
        return options_list_df

    @day_cache_async
    async def get_option_series(self, symbols_df: pd.DataFrame | None = None, timeframe: Literal['D', 'h'] = 'D') -> pd.DataFrame:
        """daily updates or hour?
        Time of execution: 14 sec. (4 thread), 9 sec (6 threads)
        exchange_option_series_id exchange_symbol type local_futures_id series_type expiration_date  central_strike          updatetime currency            datetime  \
0          ISKJP231024XE   ABIO    s         None           W      2024-10-23            96.0 2024-10-18 23:45:04      RUB 2024-10-18 23:45:04
1          ISKJP301024XE   ABIO    s         None           W      2024-10-30            96.0 2024-10-18 01:14:56      RUB 2024-10-18 01:14:56

 call_volume  put_volume  call_volume_contracts  put_volume_contracts  call_openposition  put_openposition  call_oichange  put_oichange  \
0            0.0         0.0                      0                     0              42274             43920              0             0
1            0.0         0.0                      0                     0                  0                 0              0             0

underlying_id
0     ABIO.S/MOEX
1     ABIO.S/MOEX
TODO Timeframe convert for D as last datetime after trades stop and before start,
                                                   # confert for 1h to nearest hour.
                                                   # - or before start trades and after close - the previous close time
        """
        if symbols_df is None:
            all_symbols_df = await self.get_symbol_list()
            symbols_df = all_symbols_df[['exchange_symbol', 'underlying_type']]
        tasks = []
        for idx, opt_row in symbols_df.iterrows():
            asset_type = TYPE_TO_MOEX_ASSET_TYPE.get(opt_row['underlying_type'])
            params = {'asset_type': asset_type} if asset_type is not None else {}
            tasks.append(self._get(f'assets/{opt_row["exchange_symbol"]}/optionseries', params))
        results = await self._gather_with_concurrency(*tasks)
        res = list(chain.from_iterable(results))
        res_options_series = []
        for row in res:
            series = {code: row[code] for code in row.keys() if code not in ['call', 'put']}
            series['currency'] = 'RUB'
            series['datetime'] = row['updatetime']
            series['call_volume'] = row['call']['volume_rub']
            series['put_volume'] = row['put']['volume_rub']
            series['call_volume_contracts'] = row['call']['volume_contracts']
            series['put_volume_contracts'] = row['put']['volume_contracts']
            series['call_openposition'] = row['call']['openposition']
            series['put_openposition'] = row['put']['openposition']
            series['call_oichange'] = row['call']['oichange']
            series['put_oichange'] = row['put']['oichange']
            res_options_series.append(series)
        options_series_df = pd.DataFrame.from_records(res_options_series)
        options_series_df = self._data_normalization(options_series_df)
        return options_series_df


    @day_cache_async
    async def get_option_for_id(self, options_list_df: pd.DataFrame | None = None) -> pd.DataFrame:
        """
        #TODO add datetime after previous day and before current - end of day. Otherwise - near minute
        Time of execution 22m 9s (4 thread) 19m 13s (6 threads), 1m28sec for BR only

        Not effective, more effective optionboard (get_options)
        {
          +"delta": 0.99333,
          +"gamma": 0.002127,
          +"vega": 0.001315,
          +"theta": -0.040989,
          +"rho": 0.004129,
          +"secid": "IS76CJ4D",
          -"days_until_expiring": 2,
          -"underlying_price": 95.1,
          +"volatility": 124.72161,
          -"underlying_asset": "ABIO",
          -"underlying_type": "share",
          +"theorprice": 19.119578,
          -"fee": 0.03,
          -"expiring_date": "2024-10-23",
          -"lastprice": 95.1,
          -"settleprice": 95.1
        }
        """
        if options_list_df is None:
            options_list_df = await self.get_options_list()
        # options_list_df = options_list_df[['exchange_symbol', 'exchange_option_id', 'underlying_type', 'underlying_id']]
        tasks = []
        for idx, opt_row in options_list_df.iterrows():
            asset_type = TYPE_TO_MOEX_ASSET_TYPE.get(opt_row['underlying_type'])
            params = {'asset_type': asset_type} if asset_type is not None else {}
            tasks.append(self._get(f'assets/{opt_row["exchange_symbol"]}/options/{opt_row["exchange_option_id"]}', params))
        results = await self._gather_with_concurrency(*tasks)
        options_df = pd.DataFrame.from_records(results)
        options_df = self._data_normalization(options_df)
        options_df.drop(columns=['underlying_type', 'underlying_asset'], inplace=True)

        options_df = options_df.merge(
            options_list_df[['exchange_future_id', 'expiration_date',  # 'exchange_symbol', 'type', 'series_type',
                             'option_type', 'underlying_id', 'exchange_option_id', 'currency']],  # request_time, strike
            on=['exchange_option_id'], how='left')

        # options_df = options_df.merge(options_list_df[['exchange_option_id', 'underlying_id']], on='exchange_option_id')
        # options_df['option_type'] = options_df['exchange_option_id'].apply(self._parse_type_code_from_option_id)

        # options_df['currency'] = 'RUB'
        return options_df


    @day_cache_async
    async def get_options(self, options_series_df: pd.DataFrame | None = None) -> pd.DataFrame:
        """
        # TODO add datetime after previous day and before current - end of day. Otherwise - near minute
        # "days_until_expiring": 2,
        # DATETUME - conversion
        # Торговля разбита на основную сессию и две дополнительных — утреннюю и вечернюю: 2
        # Утренняя сессия: 8:50–9:59. 2
        # Основная сессия: 10:00–18:50. 2
        # Вечерняя сессия: 19:05–23:50.
        #  Фьючесры (8:50 – 9:59), основная (10:00 – 18:50), вечерняя (19:05 – 23:50).
        # валютной секции торги идут с 6:50 утра до конца дня, 23:50
        Time of execution 32/33/40 sec (6 thread)
        {
          +"delta": 0.993319,
          +"gamma": 0.002129,
          +"vega": 0.001316,
          +"theta": -0.041057,
          +"rho": 0.004128,
          +"secid": "IS76CJ4D",
          +"theorprice": 19.119618,
          +"theorprice_rub": 19.12,
          -"last": null,
          -"offer": null,
          -"bid": null,
          -"numtrades": 0,
          -"strike": 76,
          +"volatility": 124.75,
          -"intrinsic_value": 19.1,
          -"timed_value": 0.02
        },
        """
        if options_series_df is None:
            options_series_df = await self.get_option_series()
        options_series_df = options_series_df[['exchange_symbol', 'exchange_option_series_id', 'underlying_type']]

        tasks = []
        for idx, opt_row in options_series_df.iterrows():
            asset_type = TYPE_TO_MOEX_ASSET_TYPE.get(opt_row['underlying_type'])
            params = {'asset_type': asset_type} if asset_type is not None else {}
            tasks.append(self._get(f'assets/{opt_row["exchange_symbol"]}/optionseries/{opt_row["exchange_option_series_id"]}/optionboard',
                                   params))
        response = await self._gather_with_concurrency(*tasks)
        res_options = []
        for option_board in response:
            for option_type in option_board:
                for option in option_board[option_type]:
                    res_options.append(option)

        options_df = pd.DataFrame.from_records(res_options)
        options_df = self._data_normalization(options_df)
        options_list_df = await self.get_options_list()
        options_df = options_df.merge(
            options_list_df[['exchange_future_id', 'expiration_date',  # 'exchange_symbol', 'type', 'series_type',
                             'option_type', 'underlying_id', 'exchange_option_id', 'currency']],  # request_time, strike
            on=['exchange_option_id'], how='left')

        option_base_df = await self.get_option_for_id(
            options_list_df.drop_duplicates(subset=['underlying_id'], keep='first'))

        del options_list_df

        options_df = options_df.merge(option_base_df[['underlying_price', 'fee', 'lastprice', 'settleprice',
                                                      'underlying_id']], on='underlying_id', how='left')
        del option_base_df
        return options_df

