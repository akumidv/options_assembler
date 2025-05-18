"""
 Join deribit updated to EOD history files
"""
import os
from option_lib.entities import Timeframe
from options_etl.etl_updates_to_history import EtlHistory

from exchange.exchange_entities import ExchangeCode


if __name__ == '__main__':
    data_path = os.environ.get('DATA_PATH', '../../../data')
    update_data_path = os.environ.get('UPDATE_DATA_PATH', os.path.join(data_path, 'update'))
    params = {'ochl_model': False, 'full_history': True, 'source_fields': True}
    etl_hist = EtlHistory(ExchangeCode.MOEX, data_path, update_data_path, Timeframe.EOD, params=params)
    etl_hist.prepare()
