import os
from messanger import TelegramMessanger


def test_send_message():
    telegram_token = os.environ.get('TG_BOT_TOKEN')
    telegram_chat_id = os.environ.get('TG_CHAT')
    if telegram_token and telegram_chat_id:
        telegram = TelegramMessanger(telegram_token, telegram_chat_id)
        telegram.send_message('Test message')
    else:
        print('test_send_message skipped, due TG_BOT_TOKEN and TG_CHAT is not set')
