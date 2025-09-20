"""Test enum exchange"""
import datetime
from options_lib.entities.enum_code import EnumCode, EnumDataFrameColumn


def test_enum_code_columns():
    class TestEnumCode(EnumCode):
        """Test Enum code"""
        TEST_1 = 'value1', 'code1'
        TEST_2 = 'value2', 'code2'

    assert TestEnumCode.TEST_1
    assert TestEnumCode('value2')
    test_code = TestEnumCode.TEST_1
    assert hasattr(test_code, 'value')
    assert hasattr(test_code, 'code')


def test_prov_fut_columns():
    def resample_func(x):
        return x

    class TestEnumDataFrameColumn(EnumDataFrameColumn):
        """Test Enum code type"""
        TEST_1 = 'datetime', datetime.date, resample_func
        TEST_2 = 'int', int, resample_func

    assert TestEnumDataFrameColumn.TEST_1
    assert TestEnumDataFrameColumn('int')
    test_code = TestEnumDataFrameColumn.TEST_1
    assert hasattr(test_code, 'value')
    assert hasattr(test_code, 'nm')
    assert hasattr(test_code, 'type')
    assert hasattr(test_code, 'resample_func')
    assert test_code.type == datetime.date
