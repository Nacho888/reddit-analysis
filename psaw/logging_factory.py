import logging
import os
#####


def get_module_logger(mod_name: str, level: str):
    """
    Function that creates a logger based on the module that's going to use it and
    gives it a level to show by console and save to file, or just to save to file

    :param mod_name: str - the name of the module that requests the log
    :param level: str (logging.ERROR or logging.DEBUG) - to decide how to show/store the log
    :return: logger - the configured logger

    """

    check_structure()

    logger = logging.getLogger(mod_name)
    logger.setLevel(level)

    # Remove all handlers to add ours
    for handler in logger.handlers:
        logger.removeHandler(handler)

    # ERROR messages: file - YES, console - NO
    if level is logging.ERROR:
        fh = logging.FileHandler("./logs/errors.log")
        fh.setLevel(logging.ERROR)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)-4s [%(filename)s:%(lineno)d] %(message)s")
        fh.setFormatter(file_formatter)
        logger.addHandler(fh)

    # DEBUG messages: file - YES, console - YES
    elif level is logging.DEBUG:
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        console_formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
        ch.setFormatter(console_formatter)
        logger.addHandler(ch)

        fh = logging.FileHandler("./logs/debug.log")
        fh.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)-4s [%(filename)s:%(lineno)d] %(message)s")
        fh.setFormatter(file_formatter)
        logger.addHandler(fh)

    return logger


def check_structure():
    logs_path = "./logs/"
    # Create, if not present, folder to store program's logs (and logs)
    try:
        if not os.path.isdir(logs_path):
            os.mkdir(logs_path)
            open(os.path.join(logs_path, "debug.log"), "x")
            open(os.path.join(logs_path, "errors.log"), "x")
    except FileExistsError as e:
        print("File {} already exists".format(e))
    except FileNotFoundError as e:
        print("File {} not found".format(e))
