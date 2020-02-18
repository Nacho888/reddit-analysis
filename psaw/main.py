import os
import sys
import logging
#####
import fetcher
import logging_factory
import elastic
#####
logger_err = logging_factory.get_module_logger("main_err", logging.ERROR)
logger = logging_factory.get_module_logger("main", logging.DEBUG)


def main(argv):
    if len(argv) == 7:
        try:
            argv[1] = int(argv[1])
            argv[6] = int(argv[6])

            fetcher.extract_posts(argv[0], argv[1])
            elastic.index_from_file("./backups", argv[2], argv[3], argv[4], argv[5], argv[6])
        except ValueError:
            logger_err.error("Invalid type of parameters")
            sys.exit(1)
    else:
        logger_err.error("Invalid amount of parameters")
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])

# python main.py ./excel/little_scales.xlsm 100000 localhost 9200 depression_index reddit_doc 1000
