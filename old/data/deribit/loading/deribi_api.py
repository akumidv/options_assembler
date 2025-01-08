import time

import websocket
import json
import pandas as pd
import nest_asyncio
import random
import datetime
import numpy as np

pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 150)
pd.set_option('display.max_rows', 200)


class Deribit:
    ws = None
    API_URL = 'wss://www.deribit.com/ws/api/v2'
    REQUEST_LIMIT_PER_SEC = 5
    request_count = 0
    request_second = 0

    def __init__(self):
        self.ws = websocket.create_connection(self.API_URL) #, max_size=10_000_000)

    def __del__(self):
        if self.ws is not None:
            self.close_ws()

    def close_ws(self):
        self.ws.close()
        self.ws = None

    def _wait_for_api_limit(self):
        # https://www.deribit.com/kb/deribit-rate-limits
        cur_time = int(time.time())
        if self.request_second != cur_time:
            self.request_second = cur_time
        else:
            if self.request_count >= self.REQUEST_LIMIT_PER_SEC:
                time.sleep(cur_time + 1 - time.time())
                self.request_count = 0
        self.request_count += 1

    def _call_ws_api(self, method: str, params: dict = {}):
        self._wait_for_api_limit()
        request_id = random.randint(0, 10000)
        self.ws.send(json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}))
        resp = self.ws.recv()
        json_par = json.loads(resp)
        return json_par


    def _call_ws_api_list(self, method: str, list_params: list):
        results = []
        params_ids = {}
        PAGE_SIZE = 25
        request_id_base = random.randint(0, len(list_params) * 100_000)
        for idx, params in enumerate(list_params):
            self._wait_for_api_limit()
            request_id = request_id_base + idx
            params_ids[request_id] = params
            self.ws.send(json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}))
            resp = self.ws.recv()
            json_par = json.loads(resp)
            results.append(json_par)
            if idx % PAGE_SIZE == 0:
                print(f'Request {method} {idx}/{len(list_params)}: {datetime.datetime.now().isoformat(timespec="seconds")}')
        return results, params_ids

    def get_currencies(self) -> pd.DataFrame:
        METHOD_CURRENCIES = "public/get_currencies"  # params = {}
        response = self._call_ws_api(METHOD_CURRENCIES, {})
        currencies = pd.DataFrame.from_records(response['result'])
        print('# currencies\n', currencies, sep='')
        return currencies

    def get_instruments(self, currencies: list) -> pd.DataFrame:
        METHOD_GET_INSTRUMENTS = "public/get_instruments"  # "params": "params": {"currency": "BTC" }
        list_of_params = [{'currency': ticker} for ticker in currencies]
        instrument_records = []
        responses, params_ids = self._call_ws_api_list(METHOD_GET_INSTRUMENTS, list_of_params)
        for response in responses:
            instrument_records.extend(response['result'])
        instruments = pd.DataFrame.from_records(instrument_records)
        instruments = instruments.assign(creation_timestamp=pd.to_datetime(instruments['creation_timestamp'], unit='ms', utc=True),
                                         expiration_timestamp=pd.to_datetime(instruments['creation_timestamp'], unit='ms'), utc=True)

        # print('# instruments\n', instruments)
        return instruments


    def get_order_book(self, instruments: list):
        METHOD_GET_ORDER_BOOK = "public/get_order_book"  # "params": {"instrument_name" : "BTC-PERPETUAL",    "depth" : 25}
        list_of_params = [{"instrument_name": instrument_name, 'depth': 5} for instrument_name in instruments]
        dfs = []
        responses, params_ids = self._call_ws_api_list(METHOD_GET_ORDER_BOOK, list_of_params)
        for response in responses:
            bids = pd.DataFrame(response['result']['bids'])
            asks = pd.DataFrame(response['result']['asks'])
            book = pd.merge(bids, asks, left_index=True, right_index=True)
            book = book.rename({"0_x": "bid_price", "1_x": "bid_amount",
                                "0_y": "ask_price", "1_y": "ask_amount"}, axis='columns')
            if book.empty:
                book = pd.DataFrame.from_records([{'bid_price': None, 'bid_amount': None, 'ask_price': None, 'ask_amount': None, 'level': None}])
            else:
                book['level'] = np.arange(0, len(book), dtype=int)
            for key in response['result']:
                if key in ['bids', 'asks']:
                    continue
                if key == 'stats':
                    for key_st in response['result']['stats'].keys():
                        book[f'stats_{key_st}'] = response['result']['stats'][key_st]
                elif key == 'timestamp':
                    book['datetime'] = pd.to_datetime(response['result']['timestamp'], unit='ms', utc=True)
                    #book[key] = datetime.datetime.fromtimestamp(response['result']['timestamp'] / 1000, tz=datetime.timezone.utc)
                else:
                    book[key] = response['result'][key]
            dfs.append(book)
        order_book = pd.concat(dfs, ignore_index=True)

        order_book = order_book.join(order_book['instrument_name'].str.split('-', expand=True).drop(columns=[0]) \
                                    .rename(columns={1: 'expiration_date', 2: 'strike', 3: 'call_put'}).astype({'strike': int}))
        order_book['expiration_date'] = pd.to_datetime(order_book['expiration_date'], format='%d%b%y')
        return order_book


def get_order_book_snapshot(currencies=None):
    deribit_api = Deribit()
    if currencies is None:
        currencies_df = deribit_api.get_currencies()
        currencies = currencies_df['currency'].to_list()
    elif isinstance(currencies, str):
        currencies = [currencies]
    instruments_df = deribit_api.get_instruments(currencies)
    instruments_df = instruments_df[instruments_df['kind']=='option']
    # print(instruments_df)
    instruments = [instrument_name for instrument_name in instruments_df['instrument_name'].to_list()]
    order_book_df = deribit_api.get_order_book(instruments)
    # order_book_df.to_parquet('./order_book_df.parquet')
    # order_book_df = pd.read_parquet('./order_book_df.parquet')
    order_book_df = order_book_df[~(order_book_df['level'] > 0)]
    return order_book_df


if __name__ == "__main__":
    order_book = get_order_book_snapshot('BTC')
    print(order_book)
