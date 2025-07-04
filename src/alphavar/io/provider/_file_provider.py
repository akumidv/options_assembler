"""
Local provider
Data should be organized like EXCHANGE_CODE/ASSET_CODE/ASSET_KIND/TIMEFRAME_CODE/YEAR.parquet
Example: LME/WTI/options/EOD/2024.year
ASSET_KIND can be: options, futures, spot (spot - mean assets: currency, stock, crypto)

dataframe columns:
"""

import os
import re
from abc import ABC

import pandas as pd

from alphavar.core.dictionary import InstrumentKind
from alphavar.io.provider._abstract_provider_class import AbstractProvider
from alphavar.io.provider._reference_store import read_reference
from alphavar.options.dictionary import Timeframe
from alphavar.options.entities import AssetMeta
from alphavar.options.lib.normalization import validate_path_segment

# Legacy plural kind string -> canonical singular, for raw strings that may still carry the
# old plural spelling. InstrumentKind values are already singular and pass through.
_LEGACY_KIND_TO_SINGULAR: dict[str, str] = {
    "options": InstrumentKind.OPTION.value,  # options -> option
    "futures": InstrumentKind.FUTURE.value,  # futures -> future
}


def _instrument_kind_segment(asset_kind: InstrumentKind | str) -> str:
    """Singular instrument-kind path segment (R4.5). Accepts an InstrumentKind or a raw
    string (legacy plural or already-canonical singular)."""
    if isinstance(asset_kind, InstrumentKind):
        return asset_kind.value
    return _LEGACY_KIND_TO_SINGULAR.get(asset_kind, asset_kind)


class AbstractFileProvider(AbstractProvider, ABC):
    """Load data from files"""

    exchange_data_path: str

    def __init__(self, exchange_code: str, data_path: str) -> None:
        validate_path_segment(exchange_code, field="exchange_code")
        exchange_data_path: str = os.path.normpath(os.path.abspath(os.path.join(data_path, exchange_code)))
        if not os.path.isdir(exchange_data_path):
            raise FileNotFoundError(f"Folder {exchange_data_path} is not exist")
        self.exchange_data_path = exchange_data_path
        super().__init__(exchange_code=exchange_code)

    def load_reference(self, asset_code: str) -> tuple[AssetMeta | None, pd.DataFrame]:
        """Read the per-asset reference (``_asset.json`` + ``_meta.parquet``) from the asset root
        (R4.6, T25). ``(None, empty frame)`` when none is stored yet (e.g. pre-migration files)."""
        validate_path_segment(asset_code, field="asset_code")
        asset_dir = os.path.join(self.exchange_data_path, asset_code)
        return read_reference(asset_dir)

    def get_assets_list(self, asset_kind: InstrumentKind) -> list[str]:
        """Prepare list of underlying assets asset_codes"""
        asset_codes: list[str] = []
        for asset_code in os.listdir(self.exchange_data_path):
            asset_kinds: list[str] = os.listdir(os.path.join(self.exchange_data_path, asset_code))
            if _instrument_kind_segment(asset_kind) in asset_kinds:
                asset_codes.append(asset_code)
        return asset_codes

    def _get_history_folder(self, asset_code: str, asset_kind: InstrumentKind | str, timeframe: Timeframe | str) -> str:
        validate_path_segment(asset_code, field="asset_code")
        # Singular instrument-kind segment (ADR 0001); unknown kinds fall back to spot.
        asset_kind_value: str = _instrument_kind_segment(asset_kind)
        if asset_kind_value != InstrumentKind.OPTION.value and asset_kind_value != InstrumentKind.FUTURE.value:
            asset_kind_value = InstrumentKind.SPOT.value
        return (
            f"{self.exchange_data_path}/{asset_code}/{asset_kind_value}/"
            f"{timeframe if isinstance(timeframe, str) else timeframe.value}"
        )

    def get_asset_history_years(self, asset_code: str, asset_kind: InstrumentKind, timeframe: Timeframe) -> list[int]:
        """Get years of history data for asset_code"""
        fn_pattern = re.compile(r"\d{4}.parquet")
        history_folder: str = self._get_history_folder(asset_code, asset_kind, timeframe)
        if not os.path.isdir(history_folder):
            return []
        history_files: list[int] = [int(fn[:4]) for fn in os.listdir(history_folder) if fn_pattern.match(fn)]
        return history_files

    def fn_path_prepare(
        self,
        asset_code: str,
        asset_kind: InstrumentKind | str,
        timeframe: Timeframe | str,
        year: int,
    ) -> str:
        """Prepare path for files"""
        history_folder: str = self._get_history_folder(asset_code, asset_kind, timeframe)
        return f"{history_folder}/{year}.parquet"
