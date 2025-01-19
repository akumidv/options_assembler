"""Utils for date and datetime conversion"""
import datetime


def parse_expiration_date(date_str: str) -> datetime.date | None:
    """Parse datetime from expiration date string"""
    if not isinstance(date_str, str) or 6 < len(date_str) < 5:
        return None
    year = date_str[-2:]
    month = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04', 'MAY': '05', 'JUN': '06', 'JUL': '07',
             'AUG': '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'}.get(date_str[-5:-2].upper())
    if month is None:
        return None
    day = date_str[:-5]
    if len(day) == 1:
        day = '0' + day
    return datetime.date.fromisoformat(f'20{year}-{month}-{day}')
