from datetime import datetime, date, timedelta
from typing import Union


def get_first_date_of_current_month(current_date: Union[datetime, date]) -> str:
    if type(current_date) == datetime:
        current_date = current_date.date()
    return str(current_date.replace(day=1))


def get_last_date_of_current_month(current_date: Union[datetime, date]) -> str:
    current_year = current_date.year
    current_month = current_date.month
    if type(current_date) == datetime:
        current_date = current_date.date()
    return str(current_year + int(current_month / 12), current_month % 12 + 1, 1) - timedelta(days=1)


def get_yesterday_date() -> str:
    return str((datetime.today() - timedelta(1)).date())


def get_today_date() -> str:
    return str(date.today())
