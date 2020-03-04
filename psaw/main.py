import sys
import logging
#####
import fetcher
import logging_factory
import indexer
import file_manager
#####
logger_err = logging_factory.get_module_logger("main_err", logging.ERROR)
logger = logging_factory.get_module_logger("main", logging.DEBUG)


def main(argv):
    """
    Function to invoke the program

    :param argv: list - the array with the parameters
        argv[0]: str - option (
            0: fetch,
            1: fetch + index,
            2: index,
            append an "r" to the number to delete previously stored backups to perform a clean fetch)
        argv[1]: str - path to the excel file
        argv[2]: int - maximum number of posts per query
        argv[3]: str - host to connect to (ElasticSearch)
        argv[4]: str - port to connect to (ElasticSearch)
        argv[5]: str - name of the index (ElasticSearch)
        argv[6]: str - type of the document (ElasticSearch)
        argv[7]: int - size of the documents' batches to index with the bulk indexer

    """
    if len(argv) == 8:
        try:
            deleting = 0
            if 1 < len(argv[0]) < 3:
                option, deleting = argv[0][:len(argv[0])//2], argv[0][len(argv[0])//2:]
            elif len(argv[0]) == 1:
                option = argv[0]
            else:
                logger_err.error("Invalid option format (should be: [0-2](r)?)")
                sys.exit(1)

            argv[2] = int(argv[2])
            argv[7] = int(argv[7])

            if deleting != 0:
                if deleting == "r":
                    file_manager.del_backups()
                else:
                    logger_err.error("Invalid option format (expected: 'r')")
                    sys.exit(1)

            if option == "0":
                fetcher.extract_posts_from_scales(argv[1], argv[2])
            elif option == "1":
                fetcher.extract_posts_from_scales(argv[1], argv[2])
                indexer.index_from_file(".\\backups", argv[3], argv[4], argv[5], argv[6], argv[7])
            elif option == "2":
                indexer.index_from_file(".\\backups", argv[3], argv[4], argv[5], argv[6], argv[7])
            else:
                logger_err.error("Invalid option format (should be: [0-2](r)?)")
                sys.exit(1)
        except ValueError:
            logger_err.error("Invalid type of parameters (expected: <str> <str> <int> <str> <str> <str> <str> <int>)")
            sys.exit(1)
    else:
        logger_err.error("Invalid amount of parameters (expected: 8)")
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])

# python main.py 0 .\excel\little_scales.xlsx 1000 localhost 9200 depression_index reddit_doc 100
