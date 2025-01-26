import pytest
from option_lib.entities import Timeframe
from option_lib.etl.etl_updates_to_history import EtlHistory
from option_lib.provider.exchange.exchange_entities import ExchangeCode


@pytest.fixture(name='etl_history')
def etl_history_fixture(data_path):
    etl_history = EtlHistory(exchange_code=ExchangeCode.DERIBIT.name,data_path=data_path, timeframe=Timeframe.EOD)
    return etl_history


# TODO fill timestamp when it missed by creatin_time
# request_datetime to request_timestamp
# datetime to timestamp
# expirate date, timestamp to pd.Timestamp

def test_get_update_years(etl_history):
    years = etl_history.get_update_years()


def test_get_timeframes(etl_history):
    timeframes = etl_history.get_timeframes()
