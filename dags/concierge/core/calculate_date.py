# -*- coding: utf-8 -*-
from copy import copy
from datetime import datetime, timedelta, time


def read_json(name):
    import inspect
    import os.path
    from json import loads
    caller_file = inspect.stack()[1][1]
    caller_dir = os.path.dirname(os.path.realpath(caller_file))
    file_path = os.path.join(caller_dir, name)
    with open(file_path) as lang_file:
        data = lang_file.read()
    return loads(data)


WORKING_DAYS = read_json('working_days.json')


def set_specific_hour(date_time, hour):
    """Reset datetime's time to {hour}:00:00, while saving timezone data

    Example:
        2018-1-1T14:12:55+02:00 -> 2018-1-1T02:00:00+02:00, for hour=2
        2018-1-1T14:12:55+02:00 -> 2018-1-1T18:00:00+02:00, for hour=18
    """

    return datetime.combine(date_time.date(), time(hour % 24, tzinfo=date_time.tzinfo))


def get_closest_working_day(date_, backward=False):
    """Search closest working day

    :param date_: date to start counting
    :param backward: search in the past when set to True
    :type: date_: datetime.date
    :type backward: bool
    :rtype: datetime.data
    """
    cursor = copy(date_)

    while True:
        cursor += timedelta(1) if not backward else -timedelta(1)
        if not is_holiday(cursor):
            return cursor


def round_out_day(time_cursor, reverse):
    time_cursor += timedelta(days=1) if not reverse else timedelta()
    time_cursor = set_specific_hour(time_cursor, 0)
    return time_cursor


def is_holiday(date):
    """Check if date is holiday
    Calculation is based on WORKING_DAYS dictionary, constructed in following format:
        <date_string>: <bool>

    where:
        - `date_string` - string representing the date in ISO 8601 format, `YYYY-MM-DD`.
        - `bool` - boolean representing work status of the day:
            - `True` **IF IT'S A HOLIDAY** but the day is not at weekend
            - `False` if day is at weekend, but it's a working day
    :param date: date to check
    :type date: datetime.timedelta
    :return: True if date is work day, False if it isn't
    :rtype: bool
    """

    date_iso = date.date().isoformat()
    return (
        date.weekday() in [5, 6] and  # date's weekday is Saturday or Sunday
        WORKING_DAYS.get(date_iso, True) or  # but it's not a holiday
        WORKING_DAYS.get(date_iso, False)  # or date in't at weekend, but it's holiday
    )


def calculate_business_date(start, delta, context, working_days=False, specific_hour=None):
    """This method calculates end of business period from given start and timedelta

    The calculation of end of business period is complex, so this method is used project-wide.
    Also this method provides support of accelerated calculation, useful while testing.

    The end of the period is calculated **exclusively**, for example:
        Let the 1-5 days of month (e.g. September 2008) be working days.
        So, when the calculation will be initialized with following params:

            start = datetime(2008, 9, 1)
            delta = timedelta(days=2)
            working_days = True

        The result will be equal to `datetime(2008, 9, 3)`.

    :param start: the start of period
    :param delta: duration of the period
    :param context: object, that holds data related to particular business process,
        usually it's Auction model's instance. Must be present to use acceleration
        mode.
    :param working_days: make calculations taking into account working days
    :param specific_hour: specific hour, to which date of period end should be rounded
    :type start: datetime.datetime
    :type delta: datetime.timedelta
    :type context: openprocurement.api.models.Tender
    :type working_days: bool
    :return: the end of period
    :rtype: datetime.datetime

    """
    if not working_days:
        return start + delta

    time_cursor = copy(start)
    reverse_calculations = delta < timedelta()
    days_to_collect = abs(delta.days)

    while days_to_collect > 0:
        if days_to_collect == 1:  # last day logic is extracted from the loop due to it's complexity
            break
        time_cursor = get_closest_working_day(time_cursor, backward=reverse_calculations)
        days_to_collect -= 1

    if is_holiday(start) or reverse_calculations:
        time_cursor = get_closest_working_day(time_cursor, backward=reverse_calculations)
        if specific_hour:
            time_cursor = set_specific_hour(time_cursor, specific_hour)
        else:
            time_cursor = round_out_day(time_cursor, reverse_calculations)
    else:
        if specific_hour:
            if abs(delta.days) == 1:  # if loop hadn't worked
                time_cursor = get_closest_working_day(time_cursor, backward=reverse_calculations)
            time_cursor = set_specific_hour(time_cursor, specific_hour)
        else:
            time_cursor = get_closest_working_day(time_cursor, backward=reverse_calculations)

    return time_cursor
