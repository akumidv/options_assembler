"""Telegram messenger module"""
import random
import requests
from messanger.message_class import AbstractMessanger


class TelegramMessanger(AbstractMessanger):
    """Telegram messaging"""

    def __init__(self, token: str, chat_id: str):
        self._token: str = token
        self._chat_id: str = chat_id

    def send_message(self, text):
        try:
            message = {'chat_id': self._chat_id, 'disable_web_page_preview': True, 'parse_mode': 'Markdown',
                       'random_id': random.randint(0, 10000000),
                       'text': text}
            requests.post(f'https://api.telegram.org/bot{self._token}/sendMessage', json=message, timeout=(5, 15))
        except (requests.ConnectionError, requests.Timeout) as err:
            print('[ERROR} Telegram send message', err)
