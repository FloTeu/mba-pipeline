from datetime import datetime, date
from typing import Union
import pytz

def datetime_to_integer(dt_obj: datetime) -> int:
    """ Transforms datetime object to integer representation of datetime (only until seconds, because microseconds can have variable amount of digits)
        Example: date(2021,11,25,12,45,55) -> 20211125124555
    """
    digits_sec, digits_min, digits_hour = 2, 2, 2
    return 10 ** (digits_sec+digits_min+digits_hour) * date_to_integer(dt_obj) + 10 ** (digits_sec + digits_min) * dt_obj.hour + 10 ** digits_sec * dt_obj.minute + dt_obj.second

def date_to_integer(date_obj: Union[datetime, date]) -> int:
    """ Transforms datetime or date object to integer representation of date
        Example: date(2021,11,25) -> 20211125
    """
    digits_day, digits_month, digits_year = 2, 2, 4
    return 10**(digits_day+digits_month) * date_obj.year + 10**(digits_day) * date_obj.month + date_obj.day

def dt_obj_to_integer(dt_obj: Union[datetime, date]) -> int:
    """
        If dt_obj is datetime -> datetime integer will be returned
        If dt_obj is date -> datetime integer will be returned
    """
    # check first if element is datetime, because every datetime is also date object but not other way around
    if isinstance(dt_obj, datetime):
        return datetime_to_integer(dt_obj)
    elif isinstance(dt_obj, date):
        return date_to_integer(dt_obj)
    else:
        raise NotImplementedError

def get_berlin_timestamp(without_tzinfo=False):
    return get_timestamp("Europe/Berlin").replace(tzinfo=None) if without_tzinfo else get_timestamp("Europe/Berlin")

def get_england_timestamp(without_tzinfo=False):
    return get_timestamp("Europe/London").replace(tzinfo=None) if without_tzinfo else get_timestamp("Europe/London")

def get_timestamp(timezone="Europe/Berlin"):
    return datetime.now(pytz.timezone(timezone))