"""
 Join deribit updated to EOD history files
"""
import os
from option_lib.entities import Timeframe
from option_etl.etl_updates_to_history import EtlHistory

from exchange.exchange_entities import ExchangeCode


if __name__ == '__main__':
    update_data_path = os.environ.get('UPDATE_DATA_PATH', '../../../data/update')
    data_path = os.environ.get('DATA_PATH', '../../../data')
    etl_hist = EtlHistory(ExchangeCode.DERIBIT, data_path, update_data_path, Timeframe.EOD)
    etl_hist.prepare()
