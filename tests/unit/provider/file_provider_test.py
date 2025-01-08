"""Tests for file provider"""
import pytest
import pandas as pd

from option_lib.provider import RequestParameters
from option_lib.provider._file_provider import FileProvider


class TestFileProvider(FileProvider):
    """Test implementation of File Provider class"""
    def load_future_history(self, symbol: str, params: RequestParameters, columns: list | None = None) -> pd.DataFrame:
        pass

    def load_option_history(self, symbol: str, params: RequestParameters, columns: list | None = None) -> pd.DataFrame:
        pass


@pytest.fixture(name='file_provider')
def file_provider_fixture(exchange_code, data_path):
    """File provider instance"""
    file_provider = TestFileProvider(exchange_code, data_path)
    return file_provider


def test_file_provider_instance(exchange_code, data_path):
    with pytest.raises(TypeError):
        _ = FileProvider(exchange_code, data_path)


def test_get_symbols_list(file_provider):
    symbols = file_provider.get_symbols_list()
    assert isinstance(symbols, list)
    assert len(symbols) > 0
    assert isinstance(symbols[0], str) > 0


def test_fn_path_prepare(file_provider):
    symbols = file_provider.get_symbols_list()
    assert isinstance(symbols, list)
    assert len(symbols) > 0
    assert isinstance(symbols[0], str) > 0
