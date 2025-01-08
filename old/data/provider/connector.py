import re
from abc import ABCMeta, abstractmethod, abstractproperty, ABC
from typing import NamedTuple, Literal
import logging
import asyncio
import os
from typing import Callable, Coroutine, Iterable, List, Optional
from itertools import chain
# import datetime
# import math
# import asyncio
# WEIGHT_PER_REQUEST = 1
# WEIGHT_LIMIT = 2200
# API_WEIGHT = {
#     'default': WEIGHT_PER_REQUEST,
# }
# USED_WEIGHT = {'request_time': datetime.datetime.fromtimestamp(0), 'used_weight': 0, 'is_showed': False}
#
# async def _wait_for_next_request():  # TODO add exhcnage type and there are limits and weight (init form exchange module)
#     global USED_WEIGHT
#     cur_datetime = datetime.datetime.now()
#     cur_minute = cur_datetime.replace(second=0, microsecond=0)
#     if USED_WEIGHT['request_time'] == cur_minute:
#         USED_WEIGHT['weight'] += WEIGHT_PER_REQUEST #API_WEIGHT.get(method, WEIGHT_PER_REQUEST)
#         if USED_WEIGHT['weight'] > WEIGHT_LIMIT:
#             time_difference = (cur_minute + datetime.timedelta(minutes=1)) - cur_datetime
#             seconds_for_wait = math.ceil(time_difference.total_seconds())
#             if not USED_WEIGHT['is_showed']:
#                 USED_WEIGHT['is_showed'] = True
#                 print(f"[WARNING] Wait for next request {seconds_for_wait} used weight {USED_WEIGHT['weight']}"
#                       f" for {USED_WEIGHT['request_time']}")
#             await asyncio.sleep(seconds_for_wait)
#     else:
#         USED_WEIGHT['request_time'] = cur_minute
#         USED_WEIGHT['weight'] = WEIGHT_PER_REQUEST
#         USED_WEIGHT['is_showed'] = False


class Connector(ABC):
    _metaclass__ = ABCMeta
    code = __name__
    description = __doc__
    api_key = None
    api_secret = None
    _client = None
    log: logging.Logger
    BASE_URL: str
    DEFAULT_REQUEST_LIMITS = os.environ.get('THREAD_LIMITS', 6)
    CALL_PUT_PATTERN = re.compile(r'(?=\w+\d+\.?\d*)([C|P])')

    def __init__(self, name: str, credential: dict | None = None, log: logging.Logger | None = None):
        self.name = name.upper()
        if log is None:
            self.log = logging.getLogger()
        else:
            self.log = log
        if credential:
            self.api_key = credential['api_key']
            self.api_secret = credential['api_secret']

    def _parse_type_code_from_option_id(self, option_id: str) -> Literal['c', 'p'] | None:
        type_code = self.CALL_PUT_PATTERN.search(option_id)
        return None if type_code is None else type_code.group(1).lower()

    async def _gather_with_concurrency(self, *tasks) -> tuple:
        """
        Process task by thread limits
        """
        semaphore = asyncio.Semaphore(self.DEFAULT_REQUEST_LIMITS)
        async def sem_task(task):
            async with semaphore:
                return await task

        tasks_res = await asyncio.gather(*(sem_task(task) for task in tasks))
        return tasks_res

    def get_name(self):
        return self.name

    @abstractmethod
    async def connect(self):
        raise NotImplementedError

    @abstractmethod
    async def disconnect(self):
        raise NotImplementedError

    async def _get(self, path: str, params: dict | None = None) -> list | dict:
        url = f'{self.BASE_URL}/{path}'
        resp = await self._client.get(url, params=params)
        if resp.status == 422:
            raise ValueError(f'[ERROR] in request format: {resp.status} {url} {params} {resp.text}')
        elif resp.status != 200:
            raise RuntimeError(f'Request error: {resp.status} {url} {params} {resp.text}')
        return await resp.json()

    @abstractmethod
    async def get_symbol_list(self):
        raise NotImplementedError

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

class ConnectorSocket(Connector, ABC):
    socket = None
    client = None
    # def __init__(self, name: str):
    #     self.name = name.upper()
    #
    # def get_name(self):
    #     return self.name

