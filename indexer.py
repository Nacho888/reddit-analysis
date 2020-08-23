import logging
import gzip
#####
import logging_factory
#####
from elasticsearch import Elasticsearch, helpers
#####
logger_err = logging_factory.get_module_logger("indexer_err", logging.ERROR)
logger = logging_factory.get_module_logger("indexer", logging.DEBUG)


def decode_csv_gzip(file_handler):
    """
    Given a file handler (for .csv) formats all the info and returns an index and a dictionary with the required data

    :param file_handler: the file handler containing the lines to be processed
    :return: yielded index (str) and a dictionary containing the account identificator, the username, the date of
    creation of the account, the date of retrieval and the comment and link karma punctuations
    """

    for line in file_handler:
        info = line.split(",")
        acc_id, username, created, updated, comment_karma, link_karma = \
            info[0], info[1], info[2], info[3], info[4], info[5].replace("\n", "")

        index = acc_id

        es_fields_keys = ("acc_id", "username", "created", "updated", "comment_karma", "link_karma")
        es_fields_values = (acc_id, username, created, updated, comment_karma, link_karma)
        es_dict = dict(zip(es_fields_keys, es_fields_values))

        yield index, es_dict


def es_add_bulk(path: str):
    """
    Given the path of a file containing all the data of the authors (by now in .gzip + .csv format), index all the data
    in an Elastic Search index

    :param path: str - path to the .gzip + .csv file containing all the authors info
    """

    with gzip.open(path, "rt") as file:
        es = Elasticsearch(hosts=[{"host": "localhost", "port": 9200}])

        k = ({
            "_index": "reddit_users",
            "_type": "reddit",
            "_id": index,
            "_source": es_dict,
        } for index, es_dict in decode_csv_gzip(file))

        helpers.bulk(es, k)


# es_add_bulk("D:/OneDrive - Universidad de Oviedo/tfg/69M_reddit_accounts.csv.gz")
