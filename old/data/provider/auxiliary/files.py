"""
Module that can load price data from file and iterate from it.

"""

from catcher_bot.model.module.connector import Connector
import logging


class FileConnector(Connector):
    def __init__(self, credential: dict, log: logging.Logger):
        super(FileConnector, self).__init__('file', credential, log)
        # self.api_key = credential['api_key']
        # self.api_secret = credential['api_secret']
        # self.client = None

    async def connect(self):
        """Empty"""

    async def disconnect(self):
        """Empty"""

    async def get_symbol_list(self):
        raise NotImplementedError
