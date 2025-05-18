"""Tests for file provider"""
import pytest
import pandas as pd

from option_lib.entities import AssetKind, Timeframe
from provider import RequestParameters
from provider._file_provider import FileProvider


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


def test_get_symbols_list_for_options(file_provider):
    symbols = file_provider.get_assets_list(AssetKind.OPTION)
    assert isinstance(symbols, list)
    assert len(symbols) > 0
    assert isinstance(symbols[0], str) > 0


def test_fn_path_prepare(file_provider, option_symbol):
    fn_path = file_provider.fn_path_prepare(option_symbol, AssetKind.OPTION, Timeframe.EOD, 2024)
    assert isinstance(fn_path, str)
    assert len(fn_path) > 0
    assert AssetKind.OPTION.value in fn_path
    assert option_symbol in fn_path


def test_get_history_folder(file_provider, option_symbol):
    hist_dir = file_provider._get_history_folder(option_symbol, AssetKind.OPTION, Timeframe.EOD)
    assert isinstance(hist_dir, str)
    assert len(hist_dir) > 0
    assert AssetKind.OPTION.value in hist_dir
    assert Timeframe.EOD.value in hist_dir
    assert option_symbol in hist_dir


def test_get_symbol_history_years(file_provider, option_symbol):
    hist_years = file_provider.get_asset_history_years(option_symbol, AssetKind.OPTION, Timeframe.EOD)
    assert isinstance(hist_years, list)
    assert len(hist_years) > 0
    assert isinstance(hist_years[0], int)
