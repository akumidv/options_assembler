"""
 Join deribit updated to EOD history files
"""
import os
from options_assembler.entities import Timeframe
from options_etl.etl_updates_to_history import EtlHistory

from exchange.exchange_entities import ExchangeCode


if __name__ == '__main__':
    update_data_path = os.environ.get('UPDATE_DATA_PATH', '../../../data/update')
    data_path = os.environ.get('DATA_PATH', '../../../data')
    params = {'ochl_model': True, 'full_history': False, 'source_fields': True}
    etl_hist = EtlHistory(ExchangeCode.DERIBIT, data_path, update_data_path, Timeframe.EOD, params=params)
    etl_hist.prepare()
