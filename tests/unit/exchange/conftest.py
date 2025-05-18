import pytest
from exchange import RequestClass
from exchange.moex import MoexOptions, MoexExchange

@pytest.fixture(name='moex_options_client')
def moex_options_fixture():
    """Moex options data client"""
    client = RequestClass(api_url=MoexExchange.TEST_API_URL)
    moex_market = MoexOptions(client)
    return moex_market
