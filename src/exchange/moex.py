"""
Deribit api provider
"""
import datetime
import re
import pandas as pd
import concurrent
from pydantic import validate_call
from concurrent.futures import ThreadPoolExecutor
from option_lib.entities.enum_code import EnumCode
from option_lib.entities import (
    Timeframe, AssetKind, OptionType, AssetType, OptionKind,
    OptionColumns as OCl,
    FuturesColumns as FCl,
    SpotColumns as SCl,
    ALL_COLUMN_NAMES
)
from option_lib.normalization.datetime_conversion import df_columns_to_timestamp
from option_lib.normalization import parse_expiration_date, normalize_timestamp, fill_option_price

from options_assembler.provider import DataEngine, RequestParameters
from exchange.exchange_entities import ExchangeCode
from exchange._abstract_exchange import AbstractExchange, RequestClass, APIException



class MoexAssetType(EnumCode):
    """https://iss.moex.com/iss/docs/option-calc/v1/glossary/
    sset_type = currency, share, futures, commodity, index
    asset_subtype – for futures: currency, share, commodity, index)"""
    CURRENCY = AssetType.CURRENCY.value, AssetType.CURRENCY.code
    SHARE = AssetType.SHARE.value, AssetType.SHARE.code
    COMMODITY = AssetType.COMMODITY.value, AssetType.COMMODITY.code
    INDEX = AssetType.INDEX.value, AssetType.INDEX.code
    FUTURES = 'futures', AssetType.FUTURES.code
    # OPTION = AssetType.OPTION.value, AssetType.OPTION.code


# option_type – (call, put)
# series_type = W – week, M – month, Q – quarter
DOT_STRIKE_REGEXP = re.compile(r'(\d)d(\d)', flags=re.IGNORECASE)
COLUMNS_TO_CURRENCY = [OCl.ASK.nm, OCl.BID.nm, OCl.LAST.nm, OCl.HIGH_24.nm, OCl.LOW_24.nm, OCl.EXCHANGE_PRICE.nm]


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
        response = self.client.request_api('/assets', params = params)
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
            .rename(columns={'asset_code': OCl.BASE_CODE.nm, 'futures_code': OCl.ASSET_CODE.nm}) \
            .replace({OCl.ASSET_TYPE.nm: {at.value: at.code for at in MoexAssetType}})
        fut_df = df_columns_to_timestamp(fut_df, columns=[OCl.EXPIRATION_DATE.nm])
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
                          OCl.OPTION_TYPE.nm: {at.value: at.code for at in OptionType}})
            opt_df[OCl.ASSET_TYPE.nm] = MoexAssetType.OPTION.code
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
                series_put = series_call.copy()
                series_call[OCl.OPTION_TYPE.nm] = OptionType.CALL.code
                series_put[OCl.OPTION_TYPE.nm] = OptionType.PUT.code
                series_call.update(call)
                series_put.update(put)
                options.extend([series_call, series_put])
            opt_df = pd.DataFrame(options) \
                .rename(columns={'asset_code': OCl.BASE_CODE.nm, 'asset_type': OCl.UNDERLYING_TYPE.nm,
                                 'futures_code': OCl.UNDERLYING_CODE.nm, 'optionseries_code': OCl.SERIES_CODE.nm,
                                 'updatetime': OCl.ORIGINAL_TIMESTAMP.nm, 'volume_rub': OCl.VOLUME_PREMIUM.nm,
                                 'volume_contracts': OCl.VOLUME.nm, 'openposition': OCl.OPEN_INTEREST.nm}) \
                .replace({OCl.UNDERLYING_TYPE.nm: {at.value: at.code for at in MoexAssetType}})
            opt_df = df_columns_to_timestamp(opt_df, columns=[OCl.EXPIRATION_DATE.nm])
            opt_df[OCl.ASSET_TYPE.nm] = MoexAssetType.OPTION.code
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
                               option_type: OptionType | None = None) -> pd.DataFrame | None:
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
                          OCl.OPTION_TYPE.nm: {at.value: at.code for at in OptionType}})
            opt_df = df_columns_to_timestamp(opt_df, columns=[OCl.EXPIRATION_DATE.nm])
            opt_df[OCl.ASSET_TYPE.nm] = MoexAssetType.OPTION.code
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
                 delta     gamma       vega      theta        rho   asset_code  exchange_price  theorprice_rub    last      ask     bid  numtrades   strike  exchange_iv  intrinsic_value  timed_value  \
0    0.999762  0.000000   0.096503  -0.367881  11.339113  SI69000BE5B    15549.300858        15549.30     0.0      0.0     0.0          0  69000.0        45.75          15549.0         0.30
1   -0.000238  0.000000   0.096503  -0.367881  -0.003352  SI69000BQ5B        0.300858            0.30     0.0      0.0     0.0          0  69000.0        45.75              0.0         0.30
2    0.999762  0.000000   0.096573  -0.354982  11.421304  SI69500BE5B    15049.290191        15049.29     0.0      0.0     0.0          0  69500.0        44.11          15049.0         0.29
3   -0.000238  0.000000   0.096573  -0.354982  -0.003353  SI69500BQ5B        0.290191            0.29     0.0      0.0     0.0          0  69500.0        44.11              0.0         0.29
4    0.999759  0.000000   0.097719  -0.346293  11.503455  SI70000BE5B    14549.283408        14549.28     0.0      0.0     0.0          0  70000.0        42.53          14549.0         0.28

    option_type     iv base_asset_code asset_type       series_code option_kind           expiration_date   price  underlying_price
0             c  45.75              SI          o  SI-6.25M080525XA           a 2025-05-08 00:00:00+00:00     0.0           84549.0
1             p  45.75              SI          o  SI-6.25M080525XA           a 2025-05-08 00:00:00+00:00     0.0           84549.0
2             c  44.11              SI          o  SI-6.25M080525XA           a 2025-05-08 00:00:00+00:00     0.0           84549.0
3             p  44.11              SI          o  SI-6.25M080525XA           a 2025-05-08 00:00:00+00:00     0.0           84549.0
4             c  42.53              SI          o  SI-6.25M080525XA           a 2025-05-08 00:00:00+00:00     0.0           84549.0
        """
        try:
            params = self._get_asset_type_params(None, asset_type)
            response = self.client.request_api(f'/assets/{asset_code}/optionseries/{series_code}/optionboard',
                                               params=params)
            option = []
            for call in response['call']:
                call[OCl.OPTION_TYPE.nm] = OptionType.CALL.code
                option.append(call)
            for put in response['put']:
                put[OCl.OPTION_TYPE.nm] = OptionType.PUT.code
                option.append(put)
            opt_df = pd.DataFrame(option) \
                .rename(columns={'secid': OCl.ASSET_CODE.nm, 'offer': OCl.ASK.nm,
                                 'theorprice': OCl.EXCHANGE_PRICE.nm, 'volatility': OCl.EXCHANGE_IV.nm}) \
                .replace({OCl.UNDERLYING_TYPE.nm: {at.value: at.code for at in MoexAssetType},
                          OCl.OPTION_TYPE.nm: {at.value: at.code for at in OptionType}}) \
                .sort_values(by=[OCl.STRIKE.nm, OCl.OPTION_TYPE.nm]).reset_index(drop=True)
            if asset_type is not None:
                opt_df[OCl.UNDERLYING_TYPE.nm] = asset_type.value if isinstance(asset_type, MoexAssetType) else MoexAssetType(asset_type).value
            opt_df = df_columns_to_timestamp(opt_df, columns=[OCl.EXPIRATION_DATE.nm])
            opt_df[OCl.IV.nm] = opt_df[OCl.EXCHANGE_IV.nm]
            opt_df[OCl.BASE_CODE.nm] = asset_code
            opt_df[OCl.ASSET_TYPE.nm] = MoexAssetType.OPTION.code
            opt_df[OCl.SERIES_CODE.nm] = series_code
            # https://www.moex.com/s205
            opt_df[OCl.OPTION_KIND.nm] = OptionKind.AMERICAN.code if series_code.lower().endswith('a') else OptionKind.EUROPEAN.code
            opt_df[OCl.EXPIRATION_DATE.nm] = parse_expiration_date(series_code[-8:-2])
            opt_df = fill_option_price(opt_df)
            opt_df_call_otm = opt_df[(opt_df[OCl.OPTION_TYPE.nm] == OptionType.CALL.code)&(opt_df[OCl.INTRINSIC_VALUE.nm] > 0)]
            underlying_price = opt_df_call_otm.iloc[0][OCl.STRIKE.nm] + opt_df_call_otm.iloc[0][OCl.INTRINSIC_VALUE.nm]
            opt_df[OCl.UNDERLYING_PRICE.nm] = underlying_price
            return opt_df

        except APIException as err:
            if err.status_code == 422:
                return None
            raise err from err

class MoexExchange(AbstractExchange):
    """Deribit exchange api"""
    PRODUCT_API_URL: str = 'https://iss.moex.com/iss/apps/option-calc/v1'
    TEST_API_URL: str = 'https://iss.moex.com/iss/apps/option-calc/v1'
    CURRENCIES = ['RUB']
    TASKS_LIMIT: int = 2

    def __init__(self, engine: DataEngine = DataEngine.PANDAS, api_url: str | None = None):
        """Init"""
        api_url = api_url if api_url else self.PRODUCT_API_URL
        super().__init__(engine, ExchangeCode.DERIBIT.name, api_url=api_url)
        self.options = MoexOptions(self.client)

    def get_assets_list(self, asset_type: AssetType | MoexAssetType | str | None = None) -> list[str]:
        """        TODO request all futures and options for asset list"""
        is_options = False
        if asset_type == AssetType.OPTION or asset_type == AssetType.OPTION.value:
            asset_type = None
            is_options = True
        elif isinstance(asset_type, AssetType):
            asset_type = MoexAssetType(asset_type.value)
        assets_code_df = self.options.get_assets(asset_type)
        asset_codes = [asset_code.upper() for asset_code in assets_code_df[OCl.ASSET_CODE.nm].unique()]
        if not is_options:
            return asset_codes
        options_asset_codes = []
        with ThreadPoolExecutor(max_workers=self.TASKS_LIMIT) as executor:
            job_results = {executor.submit(self.options.get_asset_options, asset_code): asset_code for asset_code in asset_codes}
            for job_res in concurrent.futures.as_completed(job_results):
                opt_df: pd.DataFrame | Exception | None = job_res.result()
                if isinstance(opt_df, pd.DataFrame):
                    asset_code = job_results[job_res]
                    options_asset_codes.append(asset_code)
        return options_asset_codes


    def get_symbols_books_snapshot(self, symbols: list[str] | str | None = None) -> pd.DataFrame: # TODO rename symbols to assets and in deribit too
        """Get all option snapshot"""
        # get list of assets
        # get list of futures
        # get list of options to check is option for asset_code or just next get_option_series
        # get list of series get_option_series
        # get desk for series
        # join series with futures
        # join desk with info series

        raise NotImplementedError
        if symbols is None:
            symbols = self.CURRENCIES
        elif isinstance(symbols, str):
            symbols = [symbols]
        if len(symbols) == 1:
            book_summary_df = self.options.get_book_summary_by_currency(currency=symbols[0])
        else:
            books = []
            with ThreadPoolExecutor(max_workers=self.TASKS_LIMIT) as executor:
                job_results = {executor.submit(self.options.get_book_summary_by_currency, currency): currency
                               for currency in symbols}
                for job_res in concurrent.futures.as_completed(job_results):
                    book_summary_df: pd.DataFrame | Exception = job_res.result()
                    if isinstance(book_summary_df, Exception):
                        currency = job_results[job_res]
                        print(f'[ERROR] for {currency} book summary: {book_summary_df}')
                        raise book_summary_df
                    books.append(book_summary_df)
            book_summary_df = pd.concat(books, ignore_index=True) if len(books) > 1 else books[0]
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
