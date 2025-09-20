"""Utils for date and datetime conversion"""
import datetime
import pandas as pd


def parse_expiration_date(date_str: str) -> datetime.date | None:
    """Parse datetime from expiration date string like 28FEB25 or 051025"""
    if not isinstance(date_str, str) or len(date_str) > 7 or len(date_str) < 6:
        return None
    try:
        year = int(date_str[-2:])
        if len(date_str) == 6 and date_str[-4] in ['0', '1']: # Month as number
            month = int(date_str[-4:-2])
            day = int(date_str[:-4])
        else:
            month = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6, 'JUL': 7,
                     'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}.get(date_str[-5:-2].upper())
            if month is None:
                return None
            day = int(date_str[:-5])
        # expiration_date = datetime.date(year=2000 + year, month=month, day=day)
        expiration_date = pd.Timestamp(year=2000 + year, month=month, day=day, tz=datetime.UTC)
    except ValueError as err:
        print(f'[ERROR] parsing expiration date {date_str}: {err}')
        return None
    return expiration_date


def df_columns_to_timestamp(df: pd.DataFrame, columns: list, unit: str | None = None) -> pd.DataFrame:
    """Convert dataframe columns to Timestamp"""
    if df.empty:
        return df
    row = df.iloc[0]
    converting_columns = [col for col in columns if col in df.columns and not isinstance(
        row[col], pd.Timestamp)]
    if len(converting_columns) > 0:
        """
        for 2_500_000 rec of datetime.date in one columns of three columns in 10 attempt
        * pd.to_datetime(df[converting_column], utc=True) ~ 0.2 sec
        - df[converting_columns].apply(pd.to_datetime) - ~ 0.24 sec, without utc=True do not guaranty conversion
        * dates unique cache: apply(lamda v: dates(v) 1.5 sec
        * df[converting_columns].apply(pd.to_datetime).apply(lambda x: x.dt.date) -- for one column 3.8 s
        * dates unique cache: rename(dates) 13 sec
        """
        for col in converting_columns:
            df[col] = pd.to_datetime(df[col], utc=True, cache=True, unit=unit)
    return df


def normalize_timestamp(df: pd.DataFrame, columns: list[str], freq: str = '1s') -> pd.DataFrame:
    """
    Normalize timeframes, because some time request for
    2025-02-05 04:36:00.001040+00:00 have timestamp 2025-02-05 04:35:59.997000+00:00 and
    due merging it will be for 04:35 1m timeframe and not for  04:36
    """
    for col in columns:
        df[col] = df[col].dt.round(freq)
    return df
