"""
Run deribit ETL for timeframe set in ETL_TIMEFRAME or 1h for default
Set path to store data in DATA_PATH environment
"""
import os
import pandas as pd
from options_assembler.entities import Timeframe
from messanger import TelegramMessanger
from exchange import DeribitExchange
from options_etl import EtlDeribit


if __name__ == '__main__':
    update_data_path = os.environ.get('UPDATE_DATA_PATH', '../../../data/update')
    deribit_exchange = DeribitExchange()
    telegram_token = os.environ.get('TG_BOT_TOKEN')
    telegram_chat_id = os.environ.get('TG_CHAT')
    timeframe = Timeframe(os.environ.get('ETL_TIMEFRAME', '1h'))
    messanger = TelegramMessanger(telegram_token, telegram_chat_id) if telegram_token and telegram_chat_id else None
    etl_deribit = EtlDeribit(deribit_exchange, DeribitExchange.CURRENCIES, timeframe, update_data_path,
                             messanger=messanger, is_detailed=True)
    etl_deribit.print_etl()
    etl_deribit.start()
    # Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
