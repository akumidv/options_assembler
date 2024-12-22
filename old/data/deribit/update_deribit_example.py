"""
https://docs.deribit.com/#market-data
"""

# built ins
import asyncio
import datetime
import logging
import os.path
from typing import Dict
import numpy as np

import asyncio
import websockets
import json
import pandas as pd
import pprint
import nest_asyncio

# installed
import aiohttp

import random

nest_asyncio.apply()

# API_URL = 'wss://test.deribit.com/ws/api/v2'
API_URL = 'wss://www.deribit.com/ws/api/v2'

EXAMPLE_DIR = '../.data_example/deribit'
os.makedirs(EXAMPLE_DIR, exist_ok=True)

pd.set_option('display.max_columns', 75)
pd.set_option('display.width', 200)
pd.set_option('display.max_rows', 150)

WEBSOCKET = None

OPTIONS_COLUMNS_TO_FILL = ['symbol', 'underlying_price', 'interest_rate', 'index_price', 'level',
                           'estimated_delivery_price']
OPT_COL_SHOW = ['settlement_price_c', 'mark_iv_c', 'settlement_price_usd_c',
                'symbol', 'expiration', 'strike',  # 'underlying_index'
                'settlement_price_p', 'mark_iv_p', 'settlement_price_usd_p']

METHOD_CURRENCIES = "public/get_currencies"  # params = {}
METHOD_GET_INSTRUMENTS = "public/get_instruments"  # "params": "params": {"currency": "BTC" }
METHOD_GET_INSTRUMENT_DETAILS = "public/get_instrument"  # "params": {"currency": "BTC" }
METHOD_TICKER = "public/ticker"  # "params": {"instrument_name": "BTC-PERPETUAL" }
METHOD_GET_INDEX = "public/get_index"  # "params": { "currency": "btc"}
METHOD_GET_INDEX_NAMES = "public/get_index_price_names"  # "params": { }
# METHOD_GET_INDEX_PRICE = "public/get_index_price"  #   "params": { "index_name": "btc_usd"} # TODO
METHOD_GET_HISTORICAL_VOLATILITY = "public/get_historical_volatility"  # "params": { "currency" : "BTC"}
METHOD_GET_FUNDING_RATE_HISTORY = "public/get_funding_rate_history"  # "params": { "instrument_name" : "BTC-PERPETUAL",     "start_timestamp" : 1569888000000, "end_timestamp" : 1569902400000}
METHOD_GET_FUNDING_RATE_VALUE = "public/get_funding_rate_value"  # "params": { "instrument_name" : "BTC-PERPETUAL",     "start_timestamp" : 1569888000000, "end_timestamp" : 1569902400000}

# https://docs.deribit.com/#public-get_order_book
METHOD_GET_ORDER_BOOK = "public/get_order_book"  # "params": {"instrument_name" : "BTC-PERPETUAL",    "depth" : 25}

METHOD_GET_LAST_TRADES = "public/get_last_trades_by_currency"  # "params": { "currency" : "BTC",     "count" : 2}
METHOD_GET_LAST_SETTLEMENTS = "public/get_last_settlements_by_currency"  # "params": {  "currency" : "BTC",     "type" : "delivery",     "count" : 2}
METHOD_GET_TRADINGVIEW = "public/get_tradingview_chart_data"  # "params": {  "instrument_name" : "BTC-PERPETUAL",  "resolution": "1D",    "start_timestamp" : 1569888000000, "end_timestamp" : 1569902400000}

BASE_CURRENCIES = ('BTC', 'ETH', 'USDC')


async def call_ws_api(method: str, params: dict = None):
    if params is None:
        params = dict()
    request_id = random.randint(0, 10000)
    # async with websockets.connect('wss://test.deribit.com/ws/api/v2') as websocket:
    await WEBSOCKET.send(json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}))
    while WEBSOCKET.open:
        resp = await WEBSOCKET.recv()
        json_par = json.loads(resp)
        return json_par


async def call_ws_api_list(method: str, list_params: list):
    results = []
    params_ids = {}
    PAGE_SIZE = 25
    idx = 0
    for params in list_params:
        if not WEBSOCKET.open:
            raise ConnectionError
        request_id = random.randint(0, len(list_params) * 100_000)
        await WEBSOCKET.send(json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}))
        resp = await WEBSOCKET.recv()
        json_par = json.loads(resp)
        if 'error' in json_par:
            print(idx, 'Error request', params, json_par['error'])
        else:
            params_ids[request_id] = params
            results.append(json_par)
        idx += 1
        if idx % PAGE_SIZE == 0:
            print(f'Request {method} {idx}/{len(list_params)}: {datetime.datetime.now().isoformat(timespec="seconds")}')
            print('[DEV] REMOVE BREAK!!!!!!!!!!!!!')
            break
    return results, params_ids


async def _get_currencies() -> pd.DataFrame:
    if os.path.isfile(f'{EXAMPLE_DIR}/currencies.parquet'):
        currencies = pd.read_parquet(f'{EXAMPLE_DIR}/currencies.parquet')
    else:
        response = await call_ws_api(METHOD_CURRENCIES, {})
        currencies = pd.DataFrame.from_records(response['result'])
        currencies.to_parquet(f'{EXAMPLE_DIR}/currencies.parquet')
    # print('# currencies\n', currencies, sep='')
    return currencies


async def _get_instruments(currencies: pd.DataFrame, should_force: bool = False) -> pd.DataFrame:
    instrument_kind = ['future', 'option', 'spot', 'future_combo', 'option_combo']
    if not should_force and os.path.isfile(f'{EXAMPLE_DIR}/instruments.parquet'):
        instruments = pd.read_parquet(f'{EXAMPLE_DIR}/instruments.parquet')
    else:
        list_of_params = [{'currency': ticker, 'kind': kind, 'expired': expired} for ticker in \
                          currencies['currency'].to_list() for kind in instrument_kind for expired in [True, False]]
        instrument_records = []
        responses, params_ids = await call_ws_api_list(METHOD_GET_INSTRUMENTS, list_of_params)
        for response in responses:
            instrument_records.extend(response['result'])
        instruments = pd.DataFrame.from_records(instrument_records)
        for column in ['creation_timestamp']:
            instruments[column] = instruments[column].apply(lambda dt: datetime.datetime.fromtimestamp(dt / 1000))
        for column in ['expiration_timestamp']:
            instruments[f'{column}_iso'] = instruments[column].apply(
                lambda dt: datetime.datetime.fromtimestamp(dt / 1000).isoformat())
        if not should_force:
            instruments.to_parquet(f'{EXAMPLE_DIR}/instruments.parquet')
    # print('# instruments\n', instruments)
    return instruments


async def _get_historical_volatility(currencies: pd.DataFrame) -> pd.DataFrame:
    if os.path.isfile(f'{EXAMPLE_DIR}/historical_volatility.parquet'):
        historical_volatility = pd.read_parquet(f'{EXAMPLE_DIR}/historical_volatility.parquet')
    else:
        list_of_params = [{'currency': ticker} for ticker in currencies['currency'].to_list()]
        instrument_records = []
        responses, params_ids = await call_ws_api_list(METHOD_GET_HISTORICAL_VOLATILITY, list_of_params)
        for response in responses:
            params = params_ids[response['id']]
            for record in response['result']:
                instrument_records.append(
                    {'timestamp': record[0], 'volatility': record[1], 'currency': params['currency']})
        historical_volatility = pd.DataFrame.from_records(instrument_records)
        historical_volatility['timestamp'] = historical_volatility['timestamp'].apply(
            lambda dt: datetime.datetime.fromtimestamp(dt / 1000, tz=datetime.timezone.utc))
        # historical_volatility['timestamp'] = pd.to_datetime(historical_volatility['timestamp'], unit='ms')
        historical_volatility.to_parquet(f'{EXAMPLE_DIR}/historical_volatility.parquet')
    print('# historical_volatility\n', historical_volatility)
    return historical_volatility


async def _get_indexes() -> pd.DataFrame:
    if os.path.isfile(f'{EXAMPLE_DIR}/indexes.parquet'):
        indexes = pd.read_parquet(f'{EXAMPLE_DIR}/indexes.parquet')
    else:
        list_of_params = [{'currency': ticker} for ticker in BASE_CURRENCIES if
                          ticker != 'USDC']  # currencies['currency'].to_list()]
        instrument_records = []
        responses, params_ids = await call_ws_api_list(METHOD_GET_INDEX, list_of_params)
        for response in responses:
            if 'result' in response:
                instrument_records.append({'name': 'BTC' if response['result'].get('BTC') else 'ETH',
                                           'value': response['result'].get('BTC', response['result'].get('ETH')),
                                           'edp': response['result']['edp']})
            else:
                print('Error', response)
        indexes = pd.DataFrame.from_records(instrument_records)
        indexes.to_parquet(f'{EXAMPLE_DIR}/indexes.parquet')
    print('# indexes\n', indexes)
    return indexes


async def _get_index_names() -> pd.DataFrame:
    if os.path.isfile(f'{EXAMPLE_DIR}/index_names.parquet'):
        index_names = pd.read_parquet(f'{EXAMPLE_DIR}/index_names.parquet')
    else:
        response = await call_ws_api(METHOD_GET_INDEX_NAMES, {})
        index_names = pd.Series(response['result'])
        index_names.name = 'index_names'
        index_names = pd.DataFrame(index_names)
        index_names.to_parquet(f'{EXAMPLE_DIR}/index_names.parquet')
    print('# index_names\n', index_names)
    return index_names


async def _get_instruments_details(instruments: pd.DataFrame) -> pd.DataFrame:
    if os.path.isfile(f'{EXAMPLE_DIR}/instruments_details.parquet'):
        instruments_details = pd.read_parquet(f'{EXAMPLE_DIR}/instruments_details.parquet')
    else:
        list_of_params = [{"instrument_name": instrument_name} for instrument_name in
                          instruments['instrument_name'].to_list()]
        instrument_records = []
        responses, params_ids = await call_ws_api_list(METHOD_GET_INSTRUMENT_DETAILS, list_of_params)
        for response in responses:
            instrument_records.append(response['result'])
        instruments_details = pd.DataFrame.from_records(instrument_records)
        for column in ['creation_timestamp']:
            instruments_details[column] = instruments_details[column].apply(
                lambda dt: datetime.datetime.fromtimestamp(dt / 1000, tz=datetime.timezone.utc))
        for column in ['expiration_timestamp']:
            instruments_details[f'{column}_iso'] = instruments_details[column].apply(
                lambda dt: datetime.datetime.fromtimestamp(dt / 1000).isoformat())
        instruments_details.to_parquet(f'{EXAMPLE_DIR}/instruments_details.parquet')
    print('# instruments_details\n', instruments_details)
    return instruments_details


async def _get_tickers(instruments: pd.DataFrame) -> pd.DataFrame:
    if os.path.isfile(f'{EXAMPLE_DIR}/tickers.parquet'):
        tickers = pd.read_parquet(f'{EXAMPLE_DIR}/tickers.parquet')
    else:
        list_of_params = [{"instrument_name": instrument_name} for instrument_name in
                          instruments['instrument_name'].to_list()]
        tickers_records = []
        responses, params_ids = await call_ws_api_list(METHOD_TICKER, list_of_params)
        for response in responses:
            item = response['result']
            for key in item['stats'].keys():
                item[f'stats_{key}'] = item['stats'][key]
            del item['stats']
            if item.get('greeks'):
                for key in item['greeks'].keys():
                    item[key] = item['greeks'][key]
                del item['greeks']
            tickers_records.append(item)
        tickers = pd.DataFrame.from_records(tickers_records)
        tickers['timestamp'] = tickers['timestamp'].apply(
            lambda dt: datetime.datetime.fromtimestamp(dt / 1000, tz=datetime.timezone.utc))
        # tickers['timestamp'] = pd.to_datetime(tickers['timestamp'], unit='ms')
        print(list(tickers.columns))  # ("','"))
        tickers = tickers[['delivery_price', 'funding_8h',
                           # 'current_funding',
                           'estimated_delivery_price',  # expired - convert to -1
                           # 'best_bid_amount', 'best_ask_amount',  'best_bid_price',
                           # 'best_ask_price',   'mark_price',   'open_interest',      'max_price', 'min_price',
                           #
                           #                    'last_price', 'interest_value', 'instrument_name', 'index_price', 'state', 'timestamp',
                           #                    'stats_volume_notional', 'stats_volume_usd', 'stats_volume', 'stats_price_change',
                           #                    'stats_low', 'stats_high', 'settlement_price'

                           ]]

        print(tickers)
        tickers.to_parquet(f'{EXAMPLE_DIR}/tickers1.parquet')
    print('# tickers\n', tickers)
    return tickers


async def _get_funding_rate_history(instruments: pd.DataFrame, start_timestamp, end_timestamp) -> pd.DataFrame:
    if os.path.isfile(f'{EXAMPLE_DIR}/funding_rate_history.parquet'):
        funding_rate_history = pd.read_parquet(f'{EXAMPLE_DIR}/funding_rate_history.parquet')
    else:

        list_of_params = [{"instrument_name": instrument_name, 'start_timestamp': start_timestamp,
                           'end_timestamp': end_timestamp} for instrument_name in
                          instruments['instrument_name'].to_list() if 'PERPETUAL' in instrument_name]
        dfs = []
        print('###list_of_params', list_of_params)
        responses, params_ids = await call_ws_api_list(METHOD_GET_FUNDING_RATE_HISTORY, list_of_params)
        for response in responses:
            params = params_ids[response['id']]
            df = pd.DataFrame.from_records(response['result'])
            df['instrument_name'] = params['instrument_name']
            df['start_timestamp'] = datetime.datetime.fromtimestamp(params['start_timestamp'] / 1000,
                                                                    tz=datetime.timezone.utc)
            df['end_timestamp'] = datetime.datetime.fromtimestamp(params['end_timestamp'] / 1000,
                                                                  tz=datetime.timezone.utc)
            dfs.append(df)  # TODO check result is list without ticker? then convert to json
        funding_rate_history = pd.concat(dfs, ignore_index=True)
        funding_rate_history['timestamp'] = funding_rate_history['timestamp'].apply(
            lambda dt: datetime.datetime.fromtimestamp(dt / 1000, tz=datetime.timezone.utc))
        funding_rate_history.to_parquet(f'{EXAMPLE_DIR}/funding_rate_history.parquet')
    print('# funding_rate_history\n', funding_rate_history)
    return funding_rate_history


async def _get_funding_rate_values(instruments: pd.DataFrame, start_timestamp_mont_ago, end_timestamp):
    if os.path.isfile(f'{EXAMPLE_DIR}/funding_rate_values.parquet'):
        funding_rate_values = pd.read_parquet(f'{EXAMPLE_DIR}/funding_rate_values.parquet')
    else:
        list_of_params = [{"instrument_name": instrument_name, 'start_timestamp': start_timestamp_mont_ago,
                           'end_timestamp': end_timestamp} for instrument_name in
                          instruments['instrument_name'].to_list() if 'PERPETUAL' in instrument_name]
        dfs = []
        responses, params_ids = await call_ws_api_list(METHOD_GET_FUNDING_RATE_VALUE, list_of_params)
        for response in responses:
            params = params_ids[response['id']]
            if 'result' in response:
                dfs.append({"instrument_name": params['instrument_name'], 'rate': response['result'],
                            'start_timestamp': datetime.datetime.fromtimestamp(params['start_timestamp'] / 1000,
                                                                               tz=datetime.timezone.utc),
                            'end_timestamp': datetime.datetime.fromtimestamp(params['end_timestamp'] / 1000,
                                                                             tz=datetime.timezone.utc)})
            else:
                print(f"Error for {params['instrument_name']}", response)
        funding_rate_values = pd.DataFrame.from_records(dfs)
        funding_rate_values.to_parquet(f'{EXAMPLE_DIR}/funding_rate_values.parquet')
    print('funding_rate_values\n', funding_rate_values)
    return funding_rate_values


async def _get_tradingview_chart(instruments: pd.DataFrame, start_timestamp_mont_ago, end_timestamp):
    if os.path.isfile(f'{EXAMPLE_DIR}/tradingview_chart.parquet'):
        tradingview_chart = pd.read_parquet(f'{EXAMPLE_DIR}/tradingview_chart.parquet')
    else:
        list_of_params = [{"instrument_name": instrument_name, "resolution": "1D",
                           'start_timestamp': start_timestamp_mont_ago, 'end_timestamp': end_timestamp} for
                          instrument_name in
                          instruments['instrument_name'].to_list() if 'PERPETUAL' in instrument_name]
        dfs = []
        responses, params_ids = await call_ws_api_list(METHOD_GET_TRADINGVIEW, list_of_params)
        for response in responses:
            params = params_ids[response['id']]
            if 'result' in response:
                print(response['result'])
                status = response['result']['status']
                del response['result']['status']
                df = pd.DataFrame(response['result'])
                df['status'] = status
                df['instrument_name'] = params['instrument_name']
                df['resolution'] = params['resolution']
                df['start_timestamp'] = datetime.datetime.fromtimestamp(params['start_timestamp'] / 1000,
                                                                        tz=datetime.timezone.utc)
                df['end_timestamp'] = datetime.datetime.fromtimestamp(params['end_timestamp'] / 1000,
                                                                      tz=datetime.timezone.utc)
                df['ticks'] = df['ticks'].apply(
                    lambda dt: datetime.datetime.fromtimestamp(dt / 1000, tz=datetime.timezone.utc))
                dfs.append(df)
            else:
                print(f"Error for {params['instrument_name']}", response)
        tradingview_chart = pd.concat(dfs, ignore_index=True)
        tradingview_chart.to_parquet(f'{EXAMPLE_DIR}/tradingview_chart.parquet')
    print('tradingview_chart\n', tradingview_chart)
    return tradingview_chart


async def _get_order_book(instruments: pd.DataFrame, should_force: bool = False):
    if not should_force and os.path.isfile(f'{EXAMPLE_DIR}/order_book.parquet'):
        order_book = pd.read_parquet(f'{EXAMPLE_DIR}/order_book.parquet')
    else:
        list_of_params = [{"instrument_name": instrument_name, 'depth': 5} for instrument_name in
                          instruments['instrument_name'].to_list()]
        dfs = []
        responses, params_ids = await call_ws_api_list(METHOD_GET_ORDER_BOOK, list_of_params)
        for response in responses:
            bids = pd.DataFrame(response['result']['bids'])
            asks = pd.DataFrame(response['result']['asks'])
            book = pd.merge(bids, asks, left_index=True, right_index=True)
            book = book.rename({"0_x": "bid_price", "1_x": "bid_amount",
                                "0_y": "ask_price", "1_y": "ask_amount"}, axis='columns')
            if book.empty:
                book = pd.DataFrame.from_records(
                    [{'bid_price': None, 'bid_amount': None, 'ask_price': None, 'ask_amount': None, 'level': None}])
            else:
                book['level'] = np.arange(0, len(book), dtype=int)
            for key in response['result']:
                if key in ['bids', 'asks']:
                    continue
                if key == 'stats':
                    for key_st in response['result']['stats'].keys():
                        book[f'stats_{key_st}'] = response['result']['stats'][key_st]
                elif key == 'timestamp':
                    book[key] = datetime.datetime.fromtimestamp(response['result']['timestamp'] / 1000,
                                                                tz=datetime.timezone.utc)
                else:
                    book[key] = response['result'][key]
            dfs.append(book)
            print(dfs)
            print(response['result'])
            print(response['result']['stats'])
            break
        order_book = pd.concat(dfs, ignore_index=True)
        order_book[['symbol', 'expiration', 'strike', 'type']] = pd.DataFrame.from_records(
            order_book['instrument_name'].apply(
                # lambda x: [x.split('-')[0], x.split('-')[1], x.split('-')[2], x.split('-')[3]] if len(
                lambda x: x.split('-') if len(x.split('-')) == 4 else [None, None, None, None]))
        order_book['strike'] = order_book['strike'].astype('Int64')
        # if not should_force:
        order_book.to_parquet(f'{EXAMPLE_DIR}/order_book.parquet')
    print('order_book\n', order_book)
    return order_book


async def _parse_options_table_from_order_book(order_book: pd.DataFrame, should_force: bool = False):
    if not should_force and os.path.isfile(f'{EXAMPLE_DIR}/options_table.parquet'):
        options_table = pd.read_parquet(f'{EXAMPLE_DIR}/options_table.parquet')
    else:
        options_df = order_book[(order_book['level'] == 0) & (order_book['strike'].notnull())] \
            .drop(
            columns=['change_id', 'instrument_name', 'state', 'stats_volume_notional', 'interest_value', 'funding_8h',
                     'current_funding', 'implied_bid', 'implied_ask', 'combo_state'])
        options_df_c = options_df[options_df['type'] == 'C'].set_index(['underlying_index', 'expiration', 'strike'])
        options_df_p = options_df[options_df['type'] == 'P'].set_index(['underlying_index', 'expiration', 'strike'])
        options_table = options_df_c.join(options_df_p, how="outer", lsuffix='_c', rsuffix='_p').reset_index(drop=False)
        for col in OPTIONS_COLUMNS_TO_FILL:
            options_table[col] = options_table[f'{col}_c'].fillna(value=options_table[f'{col}_p'])
        # The underlying future SYN.BTC-15SEP23 mark price 26068.32 is used for mark price, greeks and iv/usd order calculation with interest rate 0.00%
        options_table['settlement_price_usd_c'] = options_table['underlying_price_c'] * options_table[
            'settlement_price_c']
        options_table['settlement_price_usd_p'] = options_table['underlying_price_p'] * options_table[
            'settlement_price_p']
        if not should_force:
            options_table.to_parquet(f'{EXAMPLE_DIR}/options_table.parquet')
    # print('options_table\n', options_table)

    return options_table


async def update_data():
    global WEBSOCKET
    WEBSOCKET = await websockets.connect(API_URL, max_size=10_000_000)

    cur_dt = datetime.datetime.now(tz=datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end_timestamp = int(cur_dt.timestamp() * 1000)
    start_timestamp = int((cur_dt - datetime.timedelta(days=365)).timestamp() * 1000)
    start_timestamp_mont_ago = int((cur_dt - datetime.timedelta(days=30)).timestamp() * 1000)

    currencies = await _get_currencies()
    instruments = await _get_instruments(currencies)
    # historical_volatility = await _get_historical_volatility(currencies)
    # indexes = await _get_indexes()
    # index_names = await _get_index_names()
    # instruments_details = await _get_instruments_details(instruments)
    tickers = await _get_tickers(instruments)
    return
    funding_rate_history = await _get_funding_rate_history(instruments, start_timestamp, end_timestamp)
    funding_rate_values = await _get_funding_rate_history(instruments, start_timestamp_mont_ago, end_timestamp)
    tradingview_chart = await _get_tradingview_chart(instruments, start_timestamp_mont_ago, end_timestamp)
    order_book = await _get_order_book(instruments)
    options_table = await _parse_options_table_from_order_book(order_book)

    btc_expiration = options_table[options_table['symbol'] == 'BTC']['expiration'].unique()
    btc_options = options_table[(options_table['symbol'] == 'BTC') & (options_table['expiration'] == btc_expiration[0])]
    print(btc_options[OPT_COL_SHOW])
    # print(btc_expiration)

    # print(instruments[(instruments['option_type'].notna())&(instruments['settlement_currency']=='BTC')]) # &(instruments['expiration_timestamp']=='169320960000')
    # print(options_table[(options_table['underlying_index']=='BTC-1SEP23')&(options_table['expiration']=='1SEP23')][OPT_COL_SHOW])

    # btc_currencies = currencies[currencies['currency']=='BTC']
    # btc_instruments = await _get_instruments(btc_currencies, should_force=True)
    # print(btc_instruments)

    #    btc_instruments =


if __name__ == "__main__":
    # https://docs.deribit.com/#json-rpc


    # Мск 50 тыс. выпускников 11 классов, 2100 - 100 бальинков = 4.2%
    # Спб 36,7 тыс. 396 стобальников (2022 -500) = 1.01%
    # Новосиб 8,97 тыс - 96 стобальников = 1.07%
    # Томская - 5.3 тыс - 36 стобальников = 0.68%
    # Татарстан 17 тыс. - 200 стобальников = 1.18%
    # Ярославская 5 тыс - 51 стобальник = 1.02%
    # ХК 5 тыс. выпускников 11 классов 44 стобальника = 1.1%
    # Амур.обл. 3,5 т.выпускников 30 стобальников (24 на 15 июня) = 0.9%

    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    loop.run_until_complete(update_data())
    exit()

    response = asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msg_transaction)))
    transaction = pd.DataFrame.from_records(response['result'])
    print('transaction\n', transaction)
    transaction.to_parquet(f'{EXAMPLE_DIR}/transaction.parquet')

    response = asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msg_settlements)))
    settlements = pd.DataFrame.from_records(response['result'])
    print('settlements\n', transaction)
    settlements.to_parquet(f'{EXAMPLE_DIR}/settlements.parquet')
