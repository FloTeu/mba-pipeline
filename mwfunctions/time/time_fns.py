from datetime import datetime, date
import pytz

def get_berlin_timestamp(without_tzinfo=False):
    return get_timestamp("Europe/Berlin").replace(tzinfo=None) if without_tzinfo else get_timestamp("Europe/Berlin")

def get_timestamp(timezone="Europe/Berlin"):
    return datetime.now(pytz.timezone(timezone))