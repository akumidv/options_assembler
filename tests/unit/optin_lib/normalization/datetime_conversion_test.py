"""Tests for date normalization"""
import datetime
import pandas as pd
import pytest
from option_lib.normalization.datetime_conversion import parse_expiration_date, df_columns_to_timestamp


@pytest.mark.parametrize('date_str', ['3FEB25', '19SEP22'])
def test_parse_expiration_date(date_str):
    expiration_datetime = parse_expiration_date(date_str)
    assert isinstance(expiration_datetime, pd.Timestamp)


@pytest.mark.parametrize('date_str', [172384596, '25-01-01', '03/02/25', '21 of Feb 20204'])
def test_parse_expiration_date_wrong(date_str):
    expiration_datetime = parse_expiration_date(date_str)
    assert expiration_datetime is None


def test_df_columns_to_timestamp(df_opt_hist):
    df = df_opt_hist.iloc[:1000].copy()
    df['_test_date'] = df['timestamp'].apply(lambda x: x.date())
    columns = ['_test_date']
    assert isinstance(df.iloc[0]['_test_date'], datetime.date)
    df = df_columns_to_timestamp(df, columns)
    assert isinstance(df.iloc[0]['_test_date'], pd.Timestamp)
