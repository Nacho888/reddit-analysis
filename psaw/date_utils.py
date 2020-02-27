import time
#####
from datetime import datetime


def get_iso_date_str(timestamp, utc_offset):
    """
    Function that returns a formatted ISO-8601 timestamp given the milliseconds epoch value and the utc_offset

    :param timestamp: int - the timestamp in epoch milliseconds
    :param utc_offset: str - the utc code
    :return: timestamp: str - formatted timestamp
    """

    # Standard time format
    time_format = "%Y-%m-%d %H:%M:%S"

    timestamp = datetime.utcfromtimestamp(timestamp).strftime(time_format)
    date = datetime.strptime(timestamp + "+{}".format(utc_offset), time_format + "%z")
    date = date.isoformat()

    return date


def get_numeric_timestamp_from_iso(timestamp):
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
    return get_current_time_iso_str(int(time.time()), utc_offset)


def get_current_timestamp(utc_offset: str):
    """
    Function that returns the current timestamp

    :param utc_offset: str - the utc code
    :return: int - the current timestamp
    """
    # Time format for UTC
    time_format = "%Y-%m-%d %H:%M:%S"

    timestamp = datetime.utcfromtimestamp(int(time.time())).strftime(time_format)

    date = datetime.strptime(timestamp + utc_offset, time_format + "%z")
    timestamp = date.timestamp()

    return int(timestamp)
