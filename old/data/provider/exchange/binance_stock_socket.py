import asyncioao
from binance.client import AsyncClient, BinanceAPIException, Client

# from binance.client import AsyncClient, BinanceAPIException
from binance.streams import BinanceSocketManager

from provider.connector import ConnectorSocket
import logging


class BinanceStockSocket(ConnectorSocket):
    def __init__(self, credential: dict, log: logging):
        super(BinanceStockSocket, self).__init__('binance')
        self.socket = None
        self.client = None
        self.api_key = credential['api_key']
        self.api_secret = credential['api_secret']

    async def connect(self):
        self.client = await AsyncClient.create(self.api_key, self.api_secret)
        self.socket = BinanceSocketManager(self.client, user_timeout=30)

    async def disconnect(self):
        await self.client.close_connection()

    async def get_symbol_list(self):
        raise NotImplementedError
