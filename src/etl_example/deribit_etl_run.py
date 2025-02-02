"""
Run deribit ETL for timeframe set in ETL_TIMEFRAME or 1h for default
Set path to store data in DATA_PATH environment
"""
import os
from option_lib.provider.exchange import DeribitExchange
from option_lib.entities import Timeframe
from option_lib.etl.deribit_etl import EtlDeribit
from option_lib.etl.messanger import TelegramMessanger

if __name__ == '__main__':
    update_data_path = os.environ.get('DATA_PATH', '../../data/update')
    deribit_exchange = DeribitExchange()
    telegram_token = os.environ.get('TG_BOT_TOKEN')
    telegram_chat_id = os.environ.get('TG_CHAT')
    timeframe = Timeframe(os.environ.get('ETL_TIMEFRAME', '1h'))
    messanger = TelegramMessanger(telegram_token, telegram_chat_id) if telegram_token and telegram_chat_id else None
    etl_deribit = EtlDeribit(deribit_exchange, DeribitExchange.CURRENCIES, timeframe, update_data_path,
                             messanger=messanger)
    etl_deribit.print_etl()
    etl_deribit.start()
    # Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
