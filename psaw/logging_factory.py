import logging


def get_module_logger(mod_name, level):
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
