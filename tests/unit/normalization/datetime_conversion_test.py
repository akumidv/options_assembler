"""Tests for date normalization"""
import datetime
import pytest

from option_lib.normalization.datetime_conversion import parse_expiration_date


@pytest.mark.parametrize('date_str', ['3FEB25', '19SEP22'])
def test_parse_expiration_date(date_str):
    expiration_datetime = parse_expiration_date(date_str)
    assert isinstance(expiration_datetime, datetime.date)


@pytest.mark.parametrize('date_str', [172384596, '25-01-01', '030225', '21 of Feb 20204'])
def test_parse_expiration_date_wrong(date_str):
    expiration_datetime = parse_expiration_date(date_str)
    assert expiration_datetime is None
