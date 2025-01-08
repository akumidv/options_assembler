import asyncio
from binance.client import AsyncClient, BinanceAPIException, Client

# from binance.client import AsyncClient, BinanceAPIException
from binance.streams import BinanceSocketManager

from provider.connector import Connector
import logging


class BinanceStock(Connector):
    def __init__(self, credential: dict, log: logging.Logger):
        super(BinanceStock, self).__init__('binance', credential, log)
        # self.api_key = credential['api_key']
        # self.api_secret = credential['api_secret']
        # self.client = None

    async def connect(self):
        self.client = await AsyncClient.create(self.api_key, self.api_secret)

    async def disconnect(self):
        await self.client.close_connection()

    async def get_symbol_list(self):
        self.log.info('[DEV] TEST')
        exch_info = await self.client.get_exchange_info()
        # print(exch_info)
        # /api/v3/exchangeInfo
        return exch_info['symbols']

