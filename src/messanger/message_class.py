"""Base messanger module"""
from abc import ABC, abstractmethod


class AbstractMessanger(ABC):
    """Abstract message class"""

    @abstractmethod
    def send_message(self, text):
        """Send message"""""
