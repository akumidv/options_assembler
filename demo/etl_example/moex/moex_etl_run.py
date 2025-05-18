"""
Run deribit ETL for timeframe set in ETL_TIMEFRAME or 1h for default
Set path to store data in DATA_PATH environment
"""
import os
from option_lib.entities import Timeframe
from messanger import TelegramMessanger
from exchange import MoexExchange
from options_etl import EtlMoex


if __name__ == '__main__':
    data_path = os.environ.get('DATA_PATH', '../../../data')
    update_data_path = os.environ.get('UPDATE_DATA_PATH', os.path.join(data_path, 'update'))
    moex_exchange = MoexExchange()
    telegram_token = os.environ.get('TG_BOT_TOKEN')
    telegram_chat_id = os.environ.get('TG_CHAT')
    timeframe = Timeframe(os.environ.get('ETL_TIMEFRAME', '1h'))
    etl_cron = os.environ.get('ETL_CRON', None)
    messanger = TelegramMessanger(telegram_token, telegram_chat_id) if telegram_token and telegram_chat_id else None
    etl_deribit = EtlMoex(moex_exchange, None, timeframe=timeframe, update_data_path=update_data_path,
                             messanger=messanger, is_detailed=True, timeframe_cron=etl_cron)
    etl_deribit.print_etl()
    etl_deribit.start()
    # Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
