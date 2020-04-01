import json
import os
import logging
#####
import logging_factory
#####
logger_err = logging_factory.get_module_logger("file_manager_err", logging.ERROR)
logger = logging_factory.get_module_logger("file_manager", logging.DEBUG)


def write_to_file(data: dict, save_path: str, filename: str):
    """
    Function that writes data to a .jsonl file in the required format and returns true if successful and
    False if errored

    :param data: dict - the data to be saved as .jsonl format
    :param save_path: str - the path to the file
    :param filename: str - the name of the file
    :return: True/False - true if backup successfully, false otherwise
    """

    # Write .jsonl file backup
    try:
        with open(os.path.join(save_path, filename), 'a') as outfile:
            json.dump(data, outfile)
            outfile.write('\n')
            return True
    except (OSError, IOError):
        logger_err.error("Read/Write error has occurred with file '{}'".format(filename))
        return False
    except UnicodeEncodeError:
        logger_err.error("Encoding error has occurred")
        return False


def count_lines_file(path: str):
    """
    Function that returns the number of lines of a file given its path

    :param path: str - the path to the file
    :return: int - the number of lines of the file
    """

    with open(path) as f:
        for i, l in enumerate(f):
            pass
    return i + 1
