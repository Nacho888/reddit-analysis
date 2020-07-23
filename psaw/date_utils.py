import time
import pytz
import logging
#####
import logging_factory
#####
from typing import Optional
from datetime import datetime
#####
logger_err = logging_factory.get_module_logger("date_utils_err", logging.ERROR)
logger = logging_factory.get_module_logger("date_utils", logging.DEBUG)

# Time format for ISO-8601
time_format = "%Y-%m-%dT%H:%M:%S"


def convert_to_iso_date_str(timestamp: int):
    """
    Function that returns a formatted ISO-8601 timestamp given the milliseconds epoch value

    :param timestamp: int - the timestamp in epoch milliseconds
    :return: str - formatted timestamp
    """

    return datetime.utcfromtimestamp(timestamp).strftime(time_format)


def convert_from_iso_date_str(timestamp: str):
    """
    Function that returns the milliseconds epoch value given a formatted ISO-8601 timestamp

    :param timestamp: int - the ISO-8601 formatted timestamp
    :return: int - epoch milliseconds value
    """

    return int(datetime.strptime(timestamp, time_format).timestamp())


def get_current_date(is_str: bool):
    """
    Function that returns the current date as an int or as a formatted string

    :param is_str: bool - true for the date to be returned as a string, false otherwise
    :return: int/str - the current date
    """

    return datetime.fromtimestamp(time.time()).strftime(time_format) if is_str else int(time.time())


def extract_field_from_timestamp(date, field: str):
    """
    Function that returns the hours associated to a timestamp (int or iso_string format)

    :param date: int/str - the date in epoch millis int or in iso 8601 format string
    :param field: str - the field to extract (hour/month)
    :return hour: int - the hour/month associated to the post
    """

    if field == "hour" or field == "month":
        try:
            d = None
            if isinstance(date, str):
                d = datetime.strptime(date, time_format)
            elif isinstance(date, int):
                d = convert_to_iso_date_str(date)
                d = datetime.strptime(d, time_format)
            else:
                logger_err.error("Not compatible format of date")
            if d is not None:
                if field == "hour":
                    return d.hour
                elif field == "month":
                    return d.month
        except ValueError as e:
            logger_err.error("Error with date format", e)
    else:
        logger_err.error("Invalid field name: {}".format(field))

