"""Abstract exchange class module"""

from abc import ABC, abstractmethod
from option_lib.provider._provider_entities import DataEngine
from option_lib.provider._abstract_provider_class import AbstractProvider


class AbstractExchange(AbstractProvider, ABC):
    """Abstract exchange class"""

    @abstractmethod
    def __init__(self, engine: DataEngine, exchange_code: str, **kwargs):
        """"""
        super().__init__(exchange_code, **kwargs)
