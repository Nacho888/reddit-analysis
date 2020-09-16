import time
import logging
#####
import logging_factory
#####
from datetime import datetime
from dateutil.relativedelta import relativedelta
#####
logger_err = logging_factory.get_module_logger("date_utils_err", logging.ERROR)
logger = logging_factory.get_module_logger("date_utils", logging.DEBUG)

# Time format for ISO-8601
time_format = "%Y-%m-%dT%H:%M:%S"


def convert_to_iso_date_str(epoch: int):
    """
    Function that returns a formatted ISO-8601 timestamp given the milliseconds epoch value

    :param epoch: int - the timestamp in epoch milliseconds
    :return: str - formatted timestamp
    """

    return datetime.utcfromtimestamp(epoch).strftime(time_format)


def convert_to_iso_date(epoch: int):
    """
    Function that returns a date given the milliseconds epoch value

    :param epoch: int - the timestamp in epoch milliseconds
    :return: date - the converted date
    """

    return datetime.utcfromtimestamp(epoch)


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


def add_days_to_epoch(date_epoch: int, d: int):
    """
    Function that given a timestamp in epoch format, adds up to it the amount of days given

    :param date_epoch: int - epoch milliseconds value
    :param d: int - days to add
    :return: int - modified epoch milliseconds value
    """

    if d <= 0:
        logger.debug("Date not modified")
        return date_epoch
    else:
        return int((convert_to_iso_date(date_epoch) + relativedelta(days=+d)).timestamp())


def substract_days_from_epoch(date_epoch: int, d: int):
    """
    Function that given a timestamp in epoch format, subtracts from it the amount of days given

    :param date_epoch: int - epoch milliseconds value
    :param d: int - days to substract
    :return: int - modified epoch milliseconds value
    """

    if d <= 0:
        logger.debug("Date not modified")
        return date_epoch
    else:
        return int((convert_to_iso_date(date_epoch) + relativedelta(days=-d)).timestamp())
