from abc import ABC, abstractmethod
from option_lib.provider import AbstractProvider
from option_lib.provider import DataEngine


class AbstractExchange(AbstractProvider, ABC):

    @abstractmethod
    def __init__(self, engine: DataEngine, **kwargs):
        """"""
