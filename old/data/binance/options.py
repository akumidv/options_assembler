import os
import time
import requests
from pprint import pprint
import pandas as pd



CACHE_PATH = '../.data_example/binance/'
BINANCE_API_KEY = os.environ['BINANCE_API_KEY']
BINANCE_SECRET = os.environ['BINANCE_SECRET']

pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 180)
pd.set_option('display.max_rows', 200)


API_URL = 'https://eapi.binance.com'


def fetch_server_time():
    resp = base_request('eapi/v1/time')
    return resp['serverTime']


def base_request(method, params: dict = None):
    if params is None:
        params = {}
    resp = requests.get(f"{API_URL}/{method}", data=params)
    resp.raise_for_status()
    return resp.json()


# https://binance-docs.github.io/apidocs/voptions/en/#exchange-information
def fetch_exchange_options():
    is_cache_exist = os.path.exists(f'{CACHE_PATH}/option_assets.parquet') and os.path.exists(f'{CACHE_PATH}/option_contracts.parquet') and \
                     os.path.exists(f'{CACHE_PATH}/option_symbols.parquet')
    if not is_cache_exist:
        exchange_info = base_request('eapi/v1/exchangeInfo')
        option_assets_df = pd.DataFrame.from_records(exchange_info['optionAssets'])
        option_contracts_df = pd.DataFrame.from_records(exchange_info['optionContracts'])
        option_symbols_df = pd.DataFrame.from_records(exchange_info['optionSymbols'])
        option_symbols_df['expiryDate'] = pd.to_datetime(option_symbols_df['expiryDate'], unit="ms")
        option_assets_df.to_parquet(f'{CACHE_PATH}/option_assets.parquet')
        option_contracts_df.to_parquet(f'{CACHE_PATH}/option_contracts.parquet')
        option_symbols_df.to_parquet(f'{CACHE_PATH}/option_symbols.parquet')
    else:
        option_assets_df = pd.read_parquet(f'{CACHE_PATH}/option_assets.parquet')
        option_contracts_df = pd.read_parquet(f'{CACHE_PATH}/option_contracts.parquet')
        option_symbols_df = pd.read_parquet(f'{CACHE_PATH}/option_symbols.parquet')
    return option_assets_df, option_contracts_df, option_symbols_df


# https://binance-docs.github.io/apidocs/voptions/en/#order-book
def fetch_order_book(symbol: str):
    order_book = base_request('eapi/v1/depth', dict(symbol=symbol))
    return order_book

# https://binance-docs.github.io/apidocs/voptions/en/#kline-candlestick-data


if __name__ == '__main__':
    server_time = fetch_server_time()
    timestamp_offset = server_time - int(time.time() * 1000)

    option_assets_df, option_contracts_df, option_symbols_df = fetch_exchange_options()
    expiration_date = sorted(option_symbols_df['expiryDate'].unique())
    # print(option_symbols_df[(option_symbols_df['expiryDate'] == expiration_date[0])&(option_symbols_df['underlying'] == 'ETHUSDT')])
    order_book = fetch_order_book(expiration_date[0])
    pprint(order_book)




