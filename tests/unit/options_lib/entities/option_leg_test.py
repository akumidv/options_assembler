"""Test option leg entity"""
from options_lib.entities import LegType, OptionsLeg


def test_option_leg():
    struct_call = OptionsLeg(strike=100, lots=10, type=LegType.OPTIONS_CALL)
    assert isinstance(struct_call, OptionsLeg)
    assert hasattr(struct_call, 'strike')
    assert hasattr(struct_call, 'lots')
    assert hasattr(struct_call, 'type')
    assert isinstance(struct_call.type, LegType)
