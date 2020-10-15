import logging
import gzip
import json
#####
import logging_factory
#####
from elasticsearch import Elasticsearch, helpers, ConnectionTimeout, ConnectionError
from elasticsearch.helpers import BulkIndexError
#####
logger_err = logging_factory.get_module_logger("indexer_err", logging.ERROR)
logger = logging_factory.get_module_logger("indexer", logging.DEBUG)


def decode_file(file_handler, is_csv: bool):
    """
    Given a file handler (for .csv and .jsonl) formats all the info and returns an index and a dictionary with the
    required data

    :param file_handler: the file handler containing the lines to be processed
    :param is_csv: bool - True if the file handler is for .csv files, False if it's for .jsonl files
    :return: yielded index (str) and a dictionary containing the account identifier, the username, the date of
    creation of the account, the date of retrieval and the comment and link karma punctuations
    """

    es_fields_keys = ("acc_id", "username", "created", "updated", "comment_karma", "link_karma")

    # If it's a .csv file, skip the header
    if is_csv:
        try:
            next(file_handler)
        except StopIteration:
            logger_err.error("Empty .csv file")

    for line in file_handler:
        if is_csv:
            usr = line.split(",")
            acc_id, username, created, updated, comment, karma = \
                usr[0], usr[1], usr[2], usr[3], usr[4], usr[5].replace("\n", "")
            # The identifier for the document will be account identifier of the user
            _id = acc_id
        else:
            usr = json.loads(line)
            acc_id, username, created, updated, comment, karma = \
                usr["acc_id"], usr["username"], usr["created"], usr["updated"], usr["comment_karma"], usr["link_karma"]
            # The identifier for the document will be account identifier of the user
            _id = usr["acc_id"]

        # Cast all fields to numeric except the account identifier and the username
        es_fields_values = (acc_id, username, int(created), int(updated), int(comment), int(karma))

        yield _id, dict(zip(es_fields_keys, es_fields_values))


def es_add_bulk(path: str, index_name: str):
    """
    Given the path of a file containing all the data of the authors (by now in .gzip + .csv format and .jsonl),
    index all the data in an Elastic Search index (it MAY take quite a while if there are too many documents to index)

    :param path: str - path to the (.gzip + .csv) or .jsonl file containing all the authors info
    :param index_name: str - the name of the index to save the data
    """

    valid = False
    is_csv, fh = None, None

    # Check whether is two of the allowed extensions .csv or .jsonl
    extension = path.split(".")
    if extension[len(extension) - 1] == "jsonl":
        is_csv = False
        valid = True
        fh = open(path, "r")
    elif extension[len(extension) - 1] == "gz" and extension[len(extension) - 2] == "csv":
        is_csv = True
        valid = True
        fh = gzip.open(path, "rt")
    else:
        logger_err.error("Provide a valid file format: (.gzip + .csv) or .jsonl")

    if valid:
        logger.debug("Starting indexing...")
        host, port = "localhost", 9200
        es = Elasticsearch(hosts=[{"host": host, "port": port}])

        # Setup the generator
        k = ({
            "_index": index_name,
            "_type": "reddit",
            "_id": _id,
            "_source": es_dict,
        } for _id, es_dict in decode_file(fh, is_csv))

        try:
            # Index all the data
            helpers.bulk(es, k)
            fh.close()
        except (ConnectionError, ConnectionTimeout):
            logger_err.error("Error communicating with Elasticsearch - host: {}, port: {}".format(host, port))
        except BulkIndexError:
            logger_err.error("Errored encountered while indexing the data")


# es_add_bulk("./backups/subr_authors_info_backup.jsonl", "r_depression_users_info")
# es_add_bulk("D:/OneDrive - Universidad de Oviedo/tfg/69M_reddit_accounts.csv.gz", "reddit_users_info")
