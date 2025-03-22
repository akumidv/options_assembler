"""Test option leg entity"""
from options_assembler.entities import LegType, OptionLeg


def test_option_leg():
    struct_call = OptionLeg(strike=100, lots=10, type=LegType.OPTION_CALL)
    assert isinstance(struct_call, OptionLeg)
    assert hasattr(struct_call, 'strike')
    assert hasattr(struct_call, 'lots')
    assert hasattr(struct_call, 'type')
    assert isinstance(struct_call.type, LegType)
