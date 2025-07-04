"""T42a: the analytic tidy producers (chain / desk / time_value_series) are on the Disc surface and
their real output validates against the pinned interchange schema."""

import pandas as pd

import alphavar.options.producers  # noqa: F401 — registers the producers onto the Disc surface
from alphavar.core.dictionary import ResultTerm
from alphavar.core.disc import catalog, describe
from alphavar.options.dictionary import OptionsTerm
from alphavar.options.lib.analytic.price import time_value_series_by_atm_distance
from alphavar.options.lib.analytic.risk import payoff_curve, payoff_legs
from alphavar.options.lib.chain import convert_chain_to_desk, select_chain
from alphavar.options.schemas import (
    ChainSchema,
    DeskSchema,
    PayoffCurveSchema,
    PayoffLegsSchema,
    TimeValueSeriesSchema,
)


def test_t42a_kinds_are_registered_with_inputs_and_schema():
    surface = catalog()
    for kind in ("chain", "desk", "time_value_series"):
        assert kind in surface, f"{kind} not registered"
        assert surface[kind].output_schema is not None, f"{kind} has no output schema"

    assert [str(i) for i in describe("chain").inputs] == ["options_history"]
    assert [str(i) for i in describe("desk").inputs] == ["chain"]  # a desk is a pivot of a chain
    assert [str(i) for i in describe("time_value_series").inputs] == ["options_history"]

    assert describe("chain").output_schema is ChainSchema
    assert describe("desk").output_schema is DeskSchema
    assert describe("time_value_series").output_schema is TimeValueSeriesSchema


def test_chain_output_validates_against_chain_schema(df_opt_hist):
    chain = select_chain(df_opt_hist)
    ChainSchema.validate(chain, lazy=True)  # raises SchemaError on contract breach


def test_desk_output_validates_against_desk_schema(df_opt_hist):
    desk = convert_chain_to_desk(select_chain(df_opt_hist))
    DeskSchema.validate(desk, lazy=True)


def test_time_value_series_output_validates_against_schema(df_opt_hist):
    series = time_value_series_by_atm_distance(df_opt_hist)
    TimeValueSeriesSchema.validate(series, lazy=True)


def test_payoff_kinds_registered_with_edge():
    surface = catalog()
    for kind in ("payoff_legs", "payoff_curve"):
        assert kind in surface, f"{kind} not registered"
    assert [str(i) for i in describe("payoff_legs").inputs] == ["chain"]
    # the combined curve is the sum of the legs — the edge encodes that computation
    assert [str(i) for i in describe("payoff_curve").inputs] == ["payoff_legs"]
    assert describe("payoff_legs").output_schema is PayoffLegsSchema
    assert describe("payoff_curve").output_schema is PayoffCurveSchema


def test_payoff_legs_and_curve_validate_and_curve_is_sum_of_legs(df_chain, structure_long_straddle):
    legs = payoff_legs(df_chain, structure_long_straddle)
    curve = payoff_curve(legs)

    PayoffLegsSchema.validate(legs, lazy=True)
    PayoffCurveSchema.validate(curve, lazy=True)

    # payoff_curve ← payoff_legs: the curve is the per-strike sum of the legs' P&L
    expected = legs.groupby(OptionsTerm.STRIKE)[ResultTerm.RISK_PNL].sum().sort_index()
    got = curve.set_index(OptionsTerm.STRIKE)[ResultTerm.RISK_PNL].sort_index()
    pd.testing.assert_series_equal(got, expected, check_names=False)
