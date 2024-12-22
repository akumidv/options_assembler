import os
import pytest
from .client import AsyncClient

API_KEY = os.environ['BINANCE_API_KEY']
API_SECRET = os.environ['BINANCE_API_SECRET']


@pytest.mark.acyncio
async def test_ping():

    bc = AsyncClient(api_key=API_KEY, api_secret=API_SECRET)
    pass