"""Test enum exchange"""
import datetime

from option_lib.entities.enum_code import EnumCode, EnumDataFrameColumn


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
    class TestEnumDataFrameColumn(EnumDataFrameColumn):
        """Test Enum code type"""
        TEST_1 = 'datetime',  datetime.date
        TEST_2 = 'int',  int
        # TEST_1 = 'datetime', 'code1', datetime.date
        # TEST_2 = 'int', 'code2', int
    assert TestEnumDataFrameColumn.TEST_1
    assert TestEnumDataFrameColumn('int')
    test_code = TestEnumDataFrameColumn.TEST_1
    assert hasattr(test_code, 'value')
    assert hasattr(test_code, 'nm')
    assert hasattr(test_code, 'type')
    assert test_code.type == datetime.date
