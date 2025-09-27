"""
Deribit api provider
"""
import datetime
import re
import pandas as pd
import concurrent
from typing import Any
from pydantic import validate_call
from concurrent.futures import ThreadPoolExecutor
from options_lib.dictionary.enum_code import EnumCode
from options_lib.dictionary import (
    Timeframe, AssetKind, OptionsType, AssetType, OptionsStyle,
    OptionsColumns as OCl,
    FuturesColumns as FCl,
    SpotColumns as SCl,
    ALL_COLUMN_NAMES, Currency
)
from options_lib.normalization.datetime_conversion import df_columns_to_timestamp
from options_lib.normalization import parse_expiration_date, normalize_timestamp, fill_option_price

from provider import DataEngine, RequestParameters
from exchange.cache import Cache
from exchange.exchange_entities import ExchangeCode
from exchange._abstract_exchange import AbstractExchange, RequestClass, APIException


ttl_cache = Cache(128, is_new_day_ttl_reset=True)


class MoexAssetType(EnumCode):
    """https://iss.moex.com/iss/docs/option-calc/v1/glossary/
    sset_type = currency, share, futures, commodity, index
    asset_subtype – for futures: currency, share, commodity, index)"""
    CURRENCY = AssetType.CURRENCY.value, AssetType.CURRENCY.code
    SHARE = AssetType.SHARE.value, AssetType.SHARE.code
    COMMODITY = AssetType.COMMODITY.value, AssetType.COMMODITY.code
    INDEX = AssetType.INDEX.value, AssetType.INDEX.code
    FUTURES = 'futures', AssetKind.FUTURES.code
    # OPTION = AssetType.OPTION.value, AssetType.OPTION.code


# option_type – (call, put)
# series_type = W – week, M – month, Q – quarter
DOT_STRIKE_REGEXP = re.compile(r'(\d)d(\d)', flags=re.IGNORECASE)
COLUMNS_TO_CURRENCY = [OCl.ASK.nm, OCl.BID.nm, OCl.LAST.nm, OCl.HIGH_24.nm, OCl.LOW_24.nm, OCl.EXCHANGE_MARK_PRICE.nm]


class MoexOptions:
    """Moex options calculator api
    https://iss.moex.com/iss/docs/option-calc/v1/about/
    """

    def __init__(self, client: RequestClass):
        self.client = client

    def get_assets(self, asset_type: MoexAssetType | str | None = None) -> pd.DataFrame:
        """Assets list
        https://iss.moex.com/iss/docs/option-calc/v1/requests_option/#_2
        Response example:
         asset_code           title asset_type underlying_type
3           AFLT          AFLT (Аэрофлот)      s          None
4           AFLT           AFLT (фьючерс)      f           s
8             BR             BR (фьючерс)      f           m
26          HANG           HANG (фьючерс)      f           i
28         IMOEX  IMOEX (Индекс МосБиржи)      i          None
67            SI             Si (фьючерс)     f           c
86  USD000UTSTOM               USDRUB_TOM     c          None
        """
        params = self._get_asset_type_params(None, asset_type)
        response = self.client.request_api('/assets', params=params)
        # TODO replace asset type and asset_subtype to AssetType code
        symbols_df = pd.DataFrame(response) \
            .rename(columns={'asset_subtype': OCl.UNDERLYING_TYPE.nm}) \
            .replace({OCl.ASSET_TYPE.nm: {at.value: at.code for at in MoexAssetType},
                      OCl.UNDERLYING_TYPE.nm: {at.value: at.code for at in MoexAssetType}})
        return symbols_df

    @staticmethod
    def _get_asset_type_params(params: dict | None = None, asset_type: MoexAssetType | str | None = None):
        if not asset_type:
            return params
        asset_type = asset_type if isinstance(asset_type, MoexAssetType) else MoexAssetType(asset_type)
        asset_type_query = {'asset_type': asset_type.value}
        if params is None:
            params = {}
        params.update(asset_type_query)
        return params

    @validate_call
    def get_asset_info(self, asset_code: str, asset_type: MoexAssetType | str | None = None) -> pd.Series:
        """Asset info
        https://iss.moex.com/iss/docs/option-calc/v1/requests_option/#_3

asset_code                       SI
title              Si (фьючерс)
asset_type                    f
underlying_type               c
        """
        params = self._get_asset_type_params(None, asset_type)
        response = self.client.request_api(f'/assets/{asset_code}', params=params)
        data = {}
        for key, value in response.items():
            if key == OCl.ASSET_TYPE.nm:
                value = MoexAssetType(value).code
            elif key == 'asset_subtype':
                key = OCl.UNDERLYING_TYPE.nm
                value = MoexAssetType(value).code
            data[key] = value
        asset_data = pd.Series(data)
        return asset_data

    @validate_call
    def get_asset_futures(self, asset_code: str) -> pd.DataFrame:
        """
        https://iss.moex.com/iss/docs/option-calc/v1/requests_option/#_4

        :param asset_code:
        :return:
   asset_code base_asset_code asset_type expiration_date
0         AFM5       AFLT    f      2025-06-20
1         AFU5       AFLT    f      2025-09-19
        """
        response = self.client.request_api(f'/assets/{asset_code}/futures')
        fut_df = pd.DataFrame(response) \
            .rename(columns={'asset_code': FCl.BASE_CODE.nm, 'futures_code': FCl.ASSET_CODE.nm}) \
            .replace({FCl.ASSET_TYPE.nm: {at.value: at.code for at in MoexAssetType}})
        fut_df = df_columns_to_timestamp(fut_df, columns=[FCl.EXPIRATION_DATE.nm])
        return fut_df

    @validate_call
    def get_asset_options(self, asset_code: str) -> pd.DataFrame | None:
        """
        https://iss.moex.com/iss/docs/option-calc/v1/requests_option/#_5

        :param asset_code:
        :return:
          asset_code base_asset_code underlying_asset_type underlying_asset_code expiration_date series_type    strike option_type
0     SI69000BE5B              SI                     f                  SIM5      2025-05-08           W   69000.0           c
1     SI69000BQ5B              SI                     f                  SIM5      2025-05-08           W   69000.0           p
2     SI69500BE5B              SI                     f                  SIM5      2025-05-08           W   69500.0           c
3     SI69500BQ5B              SI                     f                  SIM5      2025-05-08           W   69500.0           p
4     SI70000BE5B              SI                     f                  SIM5      2025-05-08           W   70000.0           c
        """
        try:
            response = self.client.request_api(f'/assets/{asset_code}/options')
            opt_df = pd.DataFrame(response) \
                .rename(columns={'asset_code': OCl.BASE_CODE.nm, 'futures_code': OCl.UNDERLYING_CODE.nm,
                                 'asset_type': OCl.UNDERLYING_TYPE.nm, 'secid': OCl.ASSET_CODE.nm}) \
                .replace({OCl.UNDERLYING_TYPE.nm: {at.value: at.code for at in MoexAssetType},
                          OCl.OPTION_TYPE.nm: {at.value: at.code for at in OptionsType}})
            opt_df[OCl.ASSET_TYPE.nm] = AssetType.OPTIONS.code
            opt_df = df_columns_to_timestamp(opt_df, columns=[OCl.EXPIRATION_DATE.nm])
            return opt_df
        except APIException as err:
            if err.status_code == 422:
                return None
            raise err from err

    @validate_call
    def calc_options(self, asset_code: str, option_series: str, asset_type: MoexAssetType | str | None = None,
                     ) -> pd.DataFrame | None:
        """
        https://iss.moex.com/iss/docs/option-calc/v1/requests_option/#_6
        :param asset_code:
        :param option_series:
        :param asset_type:
        :return:
        """
        raise NotImplementedError

    @validate_call
    def get_option_series(self, asset_code: str,
                          asset_type: MoexAssetType | str | None = None) -> pd.DataFrame | None:
        """
        https://iss.moex.com/iss/docs/option-calc/v1/requests_optionboard/#_2
        :param asset_code:
        :param asset_type:
        :return:

      series_code      base_asset_code underlying_asset_type underlying_asset_code series_type         expiration_date   central_strike   original_timestamp    option_type  volume_premium  volume   open_interest  oichange
0    SI-6.25M080525XA              SI                     f                  SIM5           W 2025-05-08 00:00:00+00:00         84500.0  2025-05-02T23:51:39           c    1.157186e+09   13650    44728     18598
1    SI-6.25M080525XA              SI                     f                  SIM5           W 2025-05-08 00:00:00+00:00         84500.0  2025-05-02T23:51:39           p    4.260180e+08    5165    17008      5168
2    SI-6.25M150525XA              SI                     f                  SIM5           M 2025-05-15 00:00:00+00:00         84500.0  2025-05-02T23:50:30           c    5.992550e+07     687    48778       860
3    SI-6.25M150525XA              SI                     f                  SIM5           M 2025-05-15 00:00:00+00:00         84500.0  2025-05-02T23:50:30           p    4.586150e+07     547    14004       742
4    SI-6.25M220525XA              SI                     f                  SIM5           W 2025-05-22 00:00:00+00:00         84500.0  2025-05-02T23:49:00           c    0.000000e+00       0     8         0
5    SI-6.25M220525XA              SI                     f                  SIM5           W 2025-05-22 00:00:00+00:00         84500.0  2025-05-02T23:49:00           p    0.000000e+00       0     0         0
        """
        try:
            params = self._get_asset_type_params(None, asset_type)
            response = self.client.request_api(f'/assets/{asset_code}/optionseries', params=params)
            options = []
            for series_call in response:
                call = series_call['call']
                put = series_call['put']
                del series_call['call']
                del series_call['put']
                series_call['updatetime'] = series_call['updatetime'] + '+03:00'
                series_put = series_call.copy()
                series_call[OCl.OPTION_TYPE.nm] = OptionsType.CALL.code
                series_put[OCl.OPTION_TYPE.nm] = OptionsType.PUT.code
                series_call.update(call)
                series_put.update(put)
                options.extend([series_call, series_put])
            opt_df = pd.DataFrame(options) \
                .rename(columns={'asset_code': OCl.BASE_CODE.nm, 'asset_type': OCl.UNDERLYING_TYPE.nm,
                                 'futures_code': OCl.UNDERLYING_CODE.nm, 'optionseries_code': OCl.SERIES_CODE.nm,
                                 'updatetime': OCl.ORIGINAL_TIMESTAMP.nm, 'volume_rub': OCl.VOLUME_PREMIUM.nm,
                                 'volume_contracts': OCl.VOLUME.nm, 'openposition': OCl.OPEN_INTEREST.nm}) \
                .replace({OCl.UNDERLYING_TYPE.nm: {at.value: at.code for at in MoexAssetType}})
            opt_df = df_columns_to_timestamp(opt_df, columns=[OCl.EXPIRATION_DATE.nm, OCl.ORIGINAL_TIMESTAMP.nm])
            opt_df[OCl.ASSET_TYPE.nm] = AssetType.OPTIONS.code
            return opt_df
        except APIException as err:
            if err.status_code == 422:
                return None
            raise err from err

    def get_option_series_info(self, asset_code: str, series_code: str,
                               asset_type: MoexAssetType | str | None = None) -> pd.DataFrame | None:
        """
        https://iss.moex.com/iss/docs/option-calc/v1/requests_optionboard/#_3
        """
        raise NotImplementedError

    @validate_call
    def get_option_series_list(self, asset_code: str, series_code: str,
                               asset_type: MoexAssetType | str | None = None,
                               strike: int | None = None,
                               option_type: OptionsType | None = None) -> pd.DataFrame | None:
        """
        https://iss.moex.com/iss/docs/option-calc/v1/requests_optionboard/#_4
        :param asset_code:
        :param series_code:
        :param asset_type:
        :param strike:
        :param option_type:
        :return:
       asset_code base_asset_code underlying_asset_type underlying_asset_code expiration_date series_type   strike option_type asset_type       series_code
0    SI69000BE5B              SI                     f                  SIM5      2025-05-08           W  69000.0           c          o  SI-6.25M080525XA
1    SI69000BQ5B              SI                     f                  SIM5      2025-05-08           W  69000.0           p          o  SI-6.25M080525XA
2    SI69500BE5B              SI                     f                  SIM5      2025-05-08           W  69500.0           c          o  SI-6.25M080525XA
3    SI69500BQ5B              SI                     f                  SIM5      2025-05-08           W  69500.0           p          o  SI-6.25M080525XA
4    SI70000BE5B              SI                     f                  SIM5      2025-05-08           W  70000.0           c          o  SI-6.25M080525XA
        """
        try:
            params = self._get_asset_type_params(None, asset_type)
            response = self.client.request_api(f'/assets/{asset_code}/optionseries/{series_code}/options',
                                               params=params)
            opt_df = pd.DataFrame(response) \
                .rename(columns={'asset_code': OCl.BASE_CODE.nm, 'futures_code': OCl.UNDERLYING_CODE.nm,
                                 'asset_type': OCl.UNDERLYING_TYPE.nm, 'secid': OCl.ASSET_CODE.nm}) \
                .replace({OCl.UNDERLYING_TYPE.nm: {at.value: at.code for at in MoexAssetType},
                          OCl.OPTION_TYPE.nm: {at.value: at.code for at in OptionsType}})
            opt_df = df_columns_to_timestamp(opt_df, columns=[OCl.EXPIRATION_DATE.nm])
            opt_df[OCl.ASSET_TYPE.nm] = AssetType.OPTIONS.code
            opt_df[OCl.SERIES_CODE.nm] = series_code
            return opt_df

        except APIException as err:
            if err.status_code == 422:
                return None
            raise err from err

    @validate_call
    def get_option_series_desk(self, asset_code: str, series_code: str,
                               asset_type: MoexAssetType | str | None = None,
                               rows: int | None = None) -> pd.DataFrame | None:
        """
        https://iss.moex.com/iss/docs/option-calc/v1/requests_optionboard/#_5
        :param asset_code:
        :param series_code:
        :param asset_type:
        :return:
         delta     gamma        vega     theta          rho   asset_code  exchange_price  theorprice_rub  last       ask    bid  numtrades    strike  exchange_iv  intrinsic_value  timed_value  \
17734  0.993510  0.000003    2.222820 -7.649889    14.674173  SI67500BE5D    14893.480709        14893.48   0.0  299320.0  748.0          0   67500.0        55.06          14879.0        14.48
17735 -0.006490  0.000003    2.222820 -7.649889    -0.120348  SI67500BQ5D       14.480709           14.48   0.0       0.0    0.0          0   67500.0        55.06              0.0        14.48
17736  0.992849  0.000003    2.421940 -8.139000    14.771538  SI68000BE5D    14394.727225        14394.73   0.0  289440.0  723.0          0   68000.0        53.77          14379.0        15.73
17737 -0.007151  0.000003    2.421940 -8.139000    -0.132572  SI68000BQ5D       15.727225           15.73   0.0       0.0    0.0          0   68000.0        53.77              0.0        15.73
17738  0.992156  0.000003    2.627418 -8.608309    14.868349  SI68500BE5D    13895.969174        13895.97   0.0  279560.0  698.0          0   68500.0        52.42          13879.0        16.97

      option_type base_asset_code asset_type       series_code option_style           expiration_date                request_timestamp                 timestamp currency     iv  price  underlying_price
17734           c              SI          o  SI-6.25M220525XA            a 2025-05-22 00:00:00+00:00 2025-05-14 01:01:48.702548+00:00 2025-05-14 01:01:49+00:00      rub  55.06    0.0   82379.0
17735           p              SI          o  SI-6.25M220525XA            a 2025-05-22 00:00:00+00:00 2025-05-14 01:01:48.702548+00:00 2025-05-14 01:01:49+00:00      rub  55.06    0.0   82379.0
17736           c              SI          o  SI-6.25M220525XA            a 2025-05-22 00:00:00+00:00 2025-05-14 01:01:48.702548+00:00 2025-05-14 01:01:49+00:00      rub  53.77    0.0   82379.0
17737           p              SI          o  SI-6.25M220525XA            a 2025-05-22 00:00:00+00:00 2025-05-14 01:01:48.702548+00:00 2025-05-14 01:01:49+00:00      rub  53.77    0.0   82379.0
17738           c              SI          o  SI-6.25M220525XA            a 2025-05-22 00:00:00+00:00 2025-05-14 01:01:48.702548+00:00 2025-05-14 01:01:49+00:00      rub  52.42    0.0   82379.0
        """
        try:
            request_timestamp = pd.Timestamp.now(tz=datetime.UTC)
            params = self._get_asset_type_params(None, asset_type)
            response = self.client.request_api(f'/assets/{asset_code}/optionseries/{series_code}/optionboard',
                                               params=params)
            opt_df = self._normalize_option_desk(response, asset_code, series_code, asset_type, request_timestamp)
            return opt_df
        except APIException as err:
            if err.status_code == 422:
                return None
            raise err from err

    def _normalize_option_desk(self, response: dict, asset_code: str, series_code: str,
                               asset_type: MoexAssetType | str | None = None,
                               request_timestamp: pd.Timestamp | None = None) -> pd.DataFrame:
        option = []
        for call in response['call']:
            call[OCl.OPTION_TYPE.nm] = OptionsType.CALL.code
            option.append(call)
        for put in response['put']:
            put[OCl.OPTION_TYPE.nm] = OptionsType.PUT.code
            option.append(put)

        opt_df = pd.DataFrame(option) \
            .rename(columns={'secid': OCl.ASSET_CODE.nm, 'offer': OCl.ASK.nm,
                             'theorprice': OCl.EXCHANGE_PRICE.nm, 'volatility': OCl.EXCHANGE_MARK_IV.nm}) \
            .replace({OCl.UNDERLYING_TYPE.nm: {at.value: at.code for at in MoexAssetType},
                      OCl.OPTION_TYPE.nm: {at.value: at.code for at in OptionsType}}) \
            .sort_values(by=[OCl.STRIKE.nm, OCl.OPTION_TYPE.nm]).reset_index(drop=True)
        if asset_type is not None:
            opt_df[OCl.UNDERLYING_TYPE.nm] = asset_type.code if isinstance(asset_type,
                                                                           MoexAssetType) else MoexAssetType(
                asset_type).code
        opt_df = df_columns_to_timestamp(opt_df, columns=[OCl.EXPIRATION_DATE.nm])
        opt_df[OCl.BASE_CODE.nm] = asset_code
        opt_df[OCl.ASSET_TYPE.nm] = AssetType.OPTIONS.code
        opt_df[OCl.SERIES_CODE.nm] = series_code
        opt_df[OCl.OPTION_STYLE.nm] = OptionsStyle.AMERICAN.code if series_code.lower().endswith(
            'a') else OptionsStyle.EUROPEAN.code
        opt_df[OCl.EXPIRATION_DATE.nm] = parse_expiration_date(series_code[-8:-2])
        opt_df[OCl.REQUEST_TIMESTAMP.nm] = pd.Timestamp.now(
            tz=datetime.UTC) if request_timestamp is None else request_timestamp
        opt_df[OCl.TIMESTAMP.nm] = opt_df[OCl.REQUEST_TIMESTAMP.nm].copy()
        opt_df = normalize_timestamp(opt_df, columns=[OCl.TIMESTAMP.nm], freq='1s')
        opt_df[OCl.CURRENCY.nm] = Currency.RUB.code
        opt_df[OCl.SETTLEMENT_IV.nm] = opt_df[OCl.EXCHANGE_MARK_IV.nm]
        # https://www.moex.com/s205

        opt_df = fill_option_price(opt_df)
        opt_df_call_otm = opt_df[
            (opt_df[OCl.OPTION_TYPE.nm] == OptionsType.CALL.code) & (opt_df[OCl.INTRINSIC_VALUE.nm] > 0)]
        underlying_price = opt_df_call_otm.iloc[0][OCl.STRIKE.nm] + opt_df_call_otm.iloc[0][OCl.INTRINSIC_VALUE.nm]
        opt_df[OCl.UNDERLYING_PRICE.nm] = underlying_price
        return opt_df


class MoexExchange(AbstractExchange):
    """Deribit exchange api"""
    PRODUCT_API_URL: str = 'https://iss.moex.com/iss/apps/option-calc/v1'
    TEST_API_URL: str = 'https://iss.moex.com/iss/apps/option-calc/v1'
    CURRENCIES = [Currency.RUB.value]
    TASKS_LIMIT: int = 2

    def __init__(self, engine: DataEngine = DataEngine.PANDAS, api_url: str | None = None):
        """Init"""
        api_url = api_url if api_url else self.PRODUCT_API_URL
        super().__init__(engine, ExchangeCode.MOEX.name, api_url=api_url, http_params={'timeout': 30})
        self.options = MoexOptions(self.client)

    @ttl_cache.it
    def _request_asset_options(self, asset_code: str) -> pd.DataFrame | None:
        try:
            return self.options.get_asset_options(asset_code)
        except Exception as err:
            print(f'[ERROR] asset options request for {asset_code}: {err}')
            return None

    def get_assets_list(self, asset_kind: AssetKind | str | None = None) -> list[str]:
        """       """
        asset_codes = self._get_asset_list_wo_options(asset_kind)
        if asset_kind not in [AssetKind.OPTIONS, AssetKind.OPTIONS.value]:
            return asset_codes
        options_asset_codes = []
        with ThreadPoolExecutor(max_workers=self.TASKS_LIMIT) as executor:
            job_results = {executor.submit(self._request_asset_options, asset_code): asset_code for asset_code in
                           asset_codes}
            for job_res in concurrent.futures.as_completed(job_results):
                opt_df: pd.DataFrame | Exception | None = job_res.result()
                if isinstance(opt_df, pd.DataFrame):
                    asset_code = job_results[job_res]
                    options_asset_codes.append(asset_code)
        return options_asset_codes

    @ttl_cache.it
    def _get_asset_list_wo_options(self, asset_kind: AssetKind | str | None = None):
        if asset_kind in [AssetType.OPTIONS, AssetType.OPTIONS.value]:
            asset_kind = None
        elif asset_kind in [AssetKind.FUTURES, AssetKind.FUTURES.value, MoexAssetType.FUTURES.value]:
            asset_kind = MoexAssetType.FUTURES
        elif isinstance(asset_kind, AssetType):
            asset_kind = MoexAssetType(asset_kind.value)  # TODO REFACTOR THIS, due changes in asset type to assend kind
        assets_code_df = self.options.get_assets(asset_kind)
        asset_codes = [asset_code.upper() for asset_code in assets_code_df[OCl.ASSET_CODE.nm].unique()]
        return asset_codes

    def _request_series(self, asset_code: str):
        try:
            return self.options.get_option_series(asset_code)
        except Exception as err:
            print(f'[ERROR] option series request for {asset_code}: {err}')
            return None

    @ttl_cache.it
    def _get_options_series(self, asset_codes: list[str]) -> pd.DataFrame:
        asset_series = []
        with ThreadPoolExecutor(max_workers=self.TASKS_LIMIT) as executor:
            job_results = {executor.submit(self._request_series, asset_code): asset_code for asset_code
                           in asset_codes}
            for job_res in concurrent.futures.as_completed(job_results):
                series_df: pd.DataFrame | Exception | None = job_res.result()
                if isinstance(series_df, pd.DataFrame):
                    asset_series.append(series_df)
                # else:
                #     raise FileNotFoundError(f'Asset code {asset_code} is not options') # asset_code = job_results[job_res]
        asset_series_df = pd.concat(asset_series, ignore_index=True) if len(asset_series) > 1 else asset_series[0]
        return asset_series_df

    def _request_underlying(self, asset_code: str):
        try:
            return self.options.get_asset_futures(asset_code)
        except Exception as err:
            print(f'[ERROR] option underlying request for {asset_code}: {err}')
            return None

    @ttl_cache.it
    def _get_underlyings(self, asset_codes: list[str]) -> pd.DataFrame:
        asset_underlying = []
        with ThreadPoolExecutor(max_workers=self.TASKS_LIMIT) as executor:
            job_results = {executor.submit(self._request_underlying, asset_code): asset_code for asset_code
                           in asset_codes}
            for job_res in concurrent.futures.as_completed(job_results):
                underlying_df: pd.DataFrame | Exception | None = job_res.result()
                if isinstance(underlying_df, pd.DataFrame):
                    asset_underlying.append(underlying_df)
        asset_underlying_df = pd.concat(asset_underlying, ignore_index=True) if len(asset_underlying) > 1 else \
            asset_underlying[0]
        return asset_underlying_df

    def _request_desk(self, asset_code: str, series_code: str):
        try:
            opt_df = self.options.get_option_series_desk(asset_code=asset_code, series_code=series_code)
            return opt_df
        except Exception as err:
            print(f'[ERROR] get desk for {asset_code} {series_code}: {err}')
            return None

    def get_options_assets_books_snapshot(self, asset_codes: list[
                                                                 str] | str | None = None) -> pd.DataFrame:  # TODO rename symbols to assets and in deribit too
        """Get all option snapshot
        Request time: ~2min
        """
        # print('\n[WARNING] for get_options_assets_books_snapshot used STATIC FILE')
        # return pd.read_parquet('./book_summary_df.parquet')
        if asset_codes is None:
            asset_codes = self._get_asset_list_wo_options(AssetType.OPTIONS)
        elif isinstance(asset_codes, str):
            asset_codes = [asset_codes]
        asset_series_df = self._get_options_series(asset_codes)[[OCl.SERIES_CODE.nm, OCl.BASE_CODE.nm,
                                                                 OCl.UNDERLYING_CODE.nm, OCl.UNDERLYING_TYPE.nm,
                                                                 OCl.ORIGINAL_TIMESTAMP.nm]]
        futures_asset_codes = list(asset_series_df[asset_series_df[OCl.UNDERLYING_TYPE.nm] == AssetType.FUTURES.code][
                                       OCl.BASE_CODE.nm].unique())
        futures_asset_underlying_df = self._get_underlyings(futures_asset_codes)[
            [FCl.ASSET_CODE.nm, FCl.ASSET_TYPE.nm, FCl.EXPIRATION_DATE.nm]] \
            .rename(columns={FCl.ASSET_CODE.nm: OCl.UNDERLYING_CODE.nm, FCl.ASSET_TYPE.nm: OCl.UNDERLYING_TYPE.nm,
                             FCl.EXPIRATION_DATE.nm: OCl.UNDERLYING_EXPIRATION_DATE.nm})
        asset_series_df = asset_series_df.merge(futures_asset_underlying_df,
                                                left_on=[OCl.UNDERLYING_CODE.nm, OCl.UNDERLYING_TYPE.nm],
                                                right_on=[FCl.UNDERLYING_CODE.nm, FCl.UNDERLYING_TYPE.nm], how='left')
        tasks = [[asset_code, series_code] for asset_code in asset_series_df[OCl.BASE_CODE.nm].unique() \
                 for series_code in list(asset_series_df[asset_series_df[OCl.BASE_CODE.nm] == asset_code][
                                             OCl.SERIES_CODE.nm].unique())]
        books = []
        with ThreadPoolExecutor(max_workers=self.TASKS_LIMIT) as executor:
            job_results = {executor.submit(self._request_desk, asset_code, series_code): [asset_code, series_code]
                           for asset_code, series_code in tasks}
            for job_res in concurrent.futures.as_completed(job_results):
                book_summary_df: pd.DataFrame | Exception = job_res.result()
                if isinstance(book_summary_df, pd.DataFrame):
                    books.append(book_summary_df)
                else:
                    asset_code, series_code = job_results[job_res]
                    print(f'[ERROR] for {asset_code} {series_code} book summary: {book_summary_df}')  # raise ?
        book_summary_df = pd.concat(books, ignore_index=True) if len(books) > 1 else books[0]
        book_summary_df = book_summary_df.merge(asset_series_df[[OCl.SERIES_CODE.nm, OCl.UNDERLYING_CODE.nm,
                                                                 OCl.UNDERLYING_TYPE.nm,
                                                                 OCl.UNDERLYING_EXPIRATION_DATE.nm,
                                                                 OCl.ORIGINAL_TIMESTAMP.nm]],
                                                on=OCl.SERIES_CODE.nm, how='left')
        book_summary_df[OCl.UNDERLYING_CODE.nm] = book_summary_df[OCl.UNDERLYING_CODE.nm] \
            .infer_objects(copy=False) \
            .fillna(book_summary_df[OCl.BASE_CODE.nm])
        return book_summary_df

    def load_option_history(self, symbol: str, params: RequestParameters | None = None,
                            columns: list | None = None) -> pd.DataFrame:
        """load options history."""
        raise NotImplementedError

    def load_future_history(self, symbol: str, params: RequestParameters | None = None,
                            columns: list | None = None) -> pd.DataFrame:
        """load futures history"""
        raise NotImplementedError

    def load_future_book(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                         timeframe: Timeframe = Timeframe.EOD, columns: list | None = None) -> pd.DataFrame:
        raise NotImplementedError

    def load_option_book(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                         timeframe: Timeframe = Timeframe.EOD, columns: list | None = None) -> pd.DataFrame:
        raise NotImplementedError

    def load_option_chain(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                          expiration_date: datetime.datetime | None = None,
                          timeframe: Timeframe = Timeframe.EOD,
                          columns: list | None = None
                          ) -> pd.DataFrame | None:
        """Providing option chain by local file system is not supported return None"""
        raise NotImplementedError
