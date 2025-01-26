"""Utils for date and datetime conversion"""
import datetime


def parse_expiration_date(date_str: str) -> datetime.date | None:
    """Parse datetime from expiration date string like 28FEB25"""
    if not isinstance(date_str, str) or len(date_str) > 7 or len(date_str) < 6:
        return None
    try:
        year = date_str[-2:]
        month = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04', 'MAY': '05', 'JUN': '06', 'JUL': '07',
                 'AUG': '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'}.get(date_str[-5:-2].upper())
        if month is None:
            return None
        day = date_str[:-5]
        if len(day) == 1:
            day = '0' + day
        expiration_date = datetime.date.fromisoformat(f'20{year}-{month}-{day}')
    except ValueError as err:
        print(f'[ERROR] parsing expiration date {date_str}: {err}')
        return None
    return expiration_date
