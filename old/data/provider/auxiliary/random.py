"""
Module that can get any configurations for strategies and from portfolio about simbpls, timeframse
and return price data for them that generated in random changes in random range(volatility) for each symbol
and generate it in timeframses intervals.
"""

from catcher_bot.model.module.connector import Connector
import logging


class RandomConnector(Connector):
    def __init__(self, credential: dict, log: logging.Logger):
        super(RandomConnector, self).__init__('random', credential, log)
        # self.api_key = credential['api_key']
        # self.api_secret = credential['api_secret']
        # self.client = None

    async def connect(self):
        """Empty"""

    async def disconnect(self):
        """Empty"""

    async def get_symbol_list(self):
        raise NotImplementedError
