"""T25 slimming akmon: wide -> slim series + reference sidecar -> load -> equals wide.

Proves the slim/restore round-trip is lossless through the real `OptionsData` load path
(contract-level as-of rejoin + asset-level broadcast)."""

import pandas as pd

from alphavar.io.provider import RequestParameters
from alphavar.options.dictionary import OptionsTerm
from alphavar.options.lib.reference import (
    CONTRACT_KEY_COLUMNS,
    CONTRACT_REF_COLUMNS,
    append_on_change,
    split_reference,
)
from alphavar.options.option_data_class import OptionsData

T0 = pd.Timestamp("2025-01-01", tz="UTC")
T1 = pd.Timestamp("2025-02-01", tz="UTC")
EXP = pd.Timestamp("2025-03-28", tz="UTC")


def _wide():
    """A wide options frame: 2 contracts x 2 timestamps, with asset- and contract-level constants."""
    return pd.DataFrame(
        {
            OptionsTerm.ASSET_CODE: ["BTC"] * 4,
            OptionsTerm.INSTRUMENT_KIND: ["option"] * 4,
            OptionsTerm.CURRENCY: ["USD"] * 4,
            OptionsTerm.TIMESTAMP: [T0, T1, T0, T1],
            OptionsTerm.EXPIRATION_DATE: [EXP] * 4,
            OptionsTerm.STRIKE: [100000.0, 100000.0, 90000.0, 90000.0],
            OptionsTerm.OPTION_RIGHT: ["call", "call", "put", "put"],
            OptionsTerm.OPTION_STYLE: ["european"] * 4,
            OptionsTerm.EXCH_SYMBOL: [
                "BTC-28MAR25-100000-C",
                "BTC-28MAR25-100000-C",
                "BTC-28MAR25-90000-P",
                "BTC-28MAR25-90000-P",
            ],
            OptionsTerm.PRICE: [0.1, 0.12, 0.2, 0.18],
        }
    )


class _SlimProvider:
    """Serves a slim series + the reference sidecar (as the file provider would post-slim)."""

    def __init__(self, slim, asset, history):
        self._slim, self._asset, self._history = slim, asset, history

    def load_options_history(self, asset_code, params=None, columns=None):
        return self._slim.copy()

    def load_reference(self, asset_code):
        return self._asset, self._history.copy()


def test_slim_series_round_trips_to_the_wide_frame():
    wide = _wide()
    split = split_reference(wide)  # slim quotes + asset meta + contract reference
    key = [c for c in CONTRACT_KEY_COLUMNS if c in split.contracts.columns]
    attr = [c for c in CONTRACT_REF_COLUMNS if c in split.contracts.columns]
    history = append_on_change(pd.DataFrame(), split.contracts, wide[OptionsTerm.TIMESTAMP].min(), key, attr)

    # sanity: the slim series really dropped the reference columns
    assert OptionsTerm.EXCH_SYMBOL not in split.quotes.columns
    assert OptionsTerm.ASSET_CODE not in split.quotes.columns

    data = OptionsData(
        _SlimProvider(split.quotes, split.asset, history), "BTC", RequestParameters(), drop_na_price=False
    )
    restored = data.df_hist

    # every original column is back, with the original values (order-independent)
    assert set(wide.columns).issubset(restored.columns)
    pd.testing.assert_frame_equal(
        restored[wide.columns.tolist()].sort_values(list(wide.columns)).reset_index(drop=True),
        wide.sort_values(list(wide.columns)).reset_index(drop=True),
    )
