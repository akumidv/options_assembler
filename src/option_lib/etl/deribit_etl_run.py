"""Run deribit ETL for 1 minute timeframe
Set path to store data in DATA_PATH environment

"""
import os
from option_lib.provider.exchange import DeribitExchange
from option_lib.entities import Timeframe
from option_lib.etl.deribit_etl import EtlDeribit



if __name__ == '__main__':
    # TODO инициализация которая вызывае метод получения списка символов для загрузки и
    #  сохраняет их в олкальной переменной этот список обновляется в 0:0UTC
    update_data_path = os.environ.get('DATA_PATH', './data')
    deribit_exchange = DeribitExchange()
    etl_deribit = EtlDeribit(deribit_exchange, DeribitExchange.CURRENCIES, Timeframe.MINUTE_1, update_data_path)
    etl_deribit.print_etl()
    # thread = Thread(target=etl_deribit.start())
    # thread.start()
    etl_deribit.start()
    # Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
    # try:
    #     asyncio.get_event_loop().run_forever()
    # except (KeyboardInterrupt, SystemExit) as err:
    #     raise err
