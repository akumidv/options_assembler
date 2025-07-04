"""Tests for file provider"""

# pylint: disable=protected-access,unused-argument,missing-function-docstring
import datetime

import pandas as pd
import pytest

from alphavar.core.dictionary import InstrumentKind
from alphavar.io.provider import RequestParameters
from alphavar.io.provider._file_provider import AbstractFileProvider
from alphavar.options.dictionary import Timeframe


class TestFileProvider(AbstractFileProvider):
    """Test implementation of File Provider class"""

    def load_futures_history(
        self, asset_code: str, params: RequestParameters, columns: list | None = None
    ) -> pd.DataFrame:
        return pd.DataFrame()

    def load_options_history(
        self, asset_code: str, params: RequestParameters, columns: list | None = None
    ) -> pd.DataFrame:
        return pd.DataFrame()

    def load_options_book(
        self,
        asset_code: str,
        settlement_datetime: datetime.datetime | None = None,
        timeframe: Timeframe = Timeframe.EOD,
        columns: list | None = None,
    ) -> pd.DataFrame:
        return pd.DataFrame()

    def load_futures_book(
        self,
        asset_code: str,
        settlement_datetime: datetime.datetime | None = None,
        timeframe: Timeframe = Timeframe.EOD,
        columns: list | None = None,
    ) -> pd.DataFrame:
        return pd.DataFrame()

    def load_options_chain(
        self,
        asset_code: str,
        settlement_datetime: datetime.datetime | None = None,
        expiration_date: datetime.datetime | None = None,
        timeframe: Timeframe = Timeframe.EOD,
        columns: list | None = None,
    ) -> pd.DataFrame:
        return None


@pytest.fixture(name="file_provider")
def file_provider_fixture(exchange_code, data_path) -> TestFileProvider:
    """File provider instance"""
    file_provider = TestFileProvider(exchange_code, data_path)
    return file_provider


def test_file_provider_instance(exchange_code, data_path):
    with pytest.raises(TypeError):
        _ = AbstractFileProvider(exchange_code, data_path)  # Pylint: disable=abstract-class-instantiated


def test_get_symbols_list_for_options(file_provider):
    symbols = file_provider.get_assets_list(InstrumentKind.OPTION)
    assert isinstance(symbols, list)
    assert len(symbols) > 0
    assert isinstance(symbols[0], str) > 0


def test_fn_path_prepare(file_provider, asset_code):
    fn_path = file_provider.fn_path_prepare(asset_code, InstrumentKind.OPTION, Timeframe.EOD, 2024)
    assert isinstance(fn_path, str)
    assert len(fn_path) > 0
    # Path uses the singular instrument-kind canon (ADR 0001).
    assert InstrumentKind.OPTION.value in fn_path
    assert asset_code in fn_path


def test_get_history_folder(file_provider, asset_code):
    hist_dir = file_provider._get_history_folder(asset_code, InstrumentKind.OPTION, Timeframe.EOD)
    assert isinstance(hist_dir, str)
    assert len(hist_dir) > 0
    assert InstrumentKind.OPTION.value in hist_dir
    assert Timeframe.EOD.value in hist_dir
    assert asset_code in hist_dir


def test_get_symbol_history_years(file_provider, asset_code):
    hist_years = file_provider.get_asset_history_years(asset_code, InstrumentKind.OPTION, Timeframe.EOD)
    assert isinstance(hist_years, list)
    assert len(hist_years) > 0
    assert isinstance(hist_years[0], int)


def test_load_reference_absent_returns_none(file_provider, asset_code):
    # The committed fixtures carry no reference layer yet (pre-migration wide files).
    asset, history = file_provider.load_reference(asset_code)
    assert asset is None
    assert history.empty


def test_load_reference_round_trip(tmp_path):
    from alphavar.io.provider import write_reference
    from alphavar.options.entities import AssetMeta

    exchange_dir = tmp_path / "DERIBIT"
    (exchange_dir).mkdir()
    provider = TestFileProvider("DERIBIT", str(tmp_path))
    meta = AssetMeta(asset_code="BTC", instrument_kind="option", currency="USD")
    write_reference(meta, pd.DataFrame(), str(exchange_dir / "BTC"))

    asset, _history = provider.load_reference("BTC")
    assert asset == meta
