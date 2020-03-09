import time
import logging
#####
import logging_factory
#####
from typing import Optional
from datetime import datetime
#####
logger_err = logging_factory.get_module_logger("date_utils_err", logging.ERROR)
logger = logging_factory.get_module_logger("date_utils", logging.DEBUG)


def get_iso_date_str(timestamp: int, utc_offset: Optional[str] = None):
    """
    Function that returns a formatted ISO-8601 timestamp given the milliseconds epoch value and the utc_offset

    :param timestamp: int - the timestamp in epoch milliseconds
    :param utc_offset: str/None - the utc code (default GMT)
    :return: timestamp: str - formatted timestamp
    """

    # Standard time format
    time_format = "%Y-%m-%d %H:%M:%S"

    timestamp = datetime.utcfromtimestamp(timestamp).strftime(time_format)

    if utc_offset is not None:
        date = datetime.strptime(timestamp + "+{}".format(utc_offset), time_format + "%z")
    else:
        date = datetime.strptime(timestamp + "+0000", time_format + "%z")

    date = date.isoformat()

    return date


def get_numeric_timestamp_from_iso(timestamp: int):
    """
    Function that returns the milliseconds epoch value given a formatted ISO-8601 timestamp

    :param timestamp: int - the ISO-8601 formatted timestamp
    :return: timestamp: int - epoch milliseconds value
    """

    # ISO-8601 format
    time_format = "%Y-%m-%dT%H:%M:%S%z"

    date = datetime.strptime(timestamp, time_format).timestamp()

    return int(date)


def get_current_time_iso_str(utc_offset: str):
    """
    Function that returns the current time

    :param utc_offset: str - the utc code
    :return: str - the current ISO-8061 formatted date
    """
    return get_iso_date_str(int(time.time()), utc_offset)


def get_current_timestamp(utc_offset: str):
    """
    Function that returns the current timestamp

    :param utc_offset: str - the utc code
    :return: int - the current timestamp
    """
    # Time format for UTC
    time_format = "%Y-%m-%d %H:%M:%S"

    timestamp = datetime.utcfromtimestamp(int(time.time())).strftime(time_format)

    date = datetime.strptime(timestamp + "+" + utc_offset, time_format + "%z")
    timestamp = date.timestamp()

    return int(timestamp)


def extract_hour_from_timestamp(date):
    """
    Function that returns the hours associated to a timestamp (int or iso_string format)

    :param date: int/str - the date in epoch millis int or in iso 8601 format string
    :return hour: int - the hour associated to the post

    """

    try:
        if isinstance(date, str):
            d = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S%z')
            return d.hour
        elif isinstance(date, int):
            d = get_iso_date_str(date)
            d = datetime.strptime(d, '%Y-%m-%dT%H:%M:%S%z')
            return d.hour
        else:
            logger_err.error("Not compatible format of date")
    except ValueError as e:
        logger_err.error("Error with date format", e)


