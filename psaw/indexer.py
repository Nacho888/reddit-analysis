import json
import sys
import uuid
import logging
import os
import elasticsearch
#####
import logging_factory
#####
from typing import Optional
from elasticsearch import Elasticsearch, helpers
#####
logger_err = logging_factory.get_module_logger("indexer_err", logging.ERROR)
logger = logging_factory.get_module_logger("indexer", logging.DEBUG)

global_id = 1


def setup_for_index(data: list, _index: str, _type: str):
    """
    Function to add the necessary fields to index with ElasticSearch

    :param data: list - the data to modify and set ready to index
    :param _index: str - the index name
    :param _type: str - the document type
    :return yielded document with all the required data
    """

    for doc in data:
        yield {
            "_index": _index,
            "_id": uuid.uuid4(),
            "_type": _type,
            "_source": doc
        }


def index_data(data: list, host: str, port: str, _index: str, _type: str):
    """
    Function to index the data with ElasticSearch

    :param data: list - the data to index (coming as a list of json line objects)
    :param host: str - host to connect to
    :param port: str - port to connect to
    :param _index: str - the index name
    :param _type: str - the document type
    :return: int - number of documents successfully indexed
    """

    resp = None

    try:
        es = Elasticsearch([{"host": host, "port": port}])
        # logger.debug("Connection established")

        # Create index (if not exists)
        if not es.indices.exists(index=_index):
            es.indices.create(index=_index, ignore=[400, 404])

        # Load data
        try:
            resp = helpers.bulk(es, setup_for_index(data, _index, _type), index=_index, doc_type=_type)
        except helpers.BulkIndexError as e:
            print(e)
            logger_err.error("helpers.bulk() - ERROR\n")
            pass
    except (elasticsearch.ConnectionTimeout, elasticsearch.ConnectionError):
        logger_err.error("ElasticSearch client problem (check if open)")
        pass

    return resp[0] if resp is not None else 0


def index_from_file(path: str, host: str, port: str, _index: str, _type: str, limit: int):
    """
    Function that given the path of the backups file, indexes all the documents

    :param path: str - path where the backups file is
    :param host: str - host to connect to
    :param port: str - port to connect to
    :param _index: str - the index name
    :param _type: str - the document type
    :param limit: int - amount of lines to be
    """

    lines = []
    ok_docs = 0
    count = 0

    logger.debug("Parameters to establish connection with ElasticSearch -> (host: '{}', port: '{}')".format(host, port))

    with open(os.path.join(path), "r") as readfile:
        for line in readfile:
            count += 1
            if len(lines) < limit - 1:  # - 1 because we are going to append one line in the 'else' part
                lines.append(line)
            else:
                lines.append(line)
                ok_docs += index_data(lines, host, port, _index, _type)
                lines = []

        # There's remaining documents
        if len(lines) > 0:
            ok_docs += index_data(lines, host, port, _index, _type)

    logger.debug("{} documents indexed successfully (expected {})".format(ok_docs, count))


def main(argv):
    if len(argv) == 6:
        try:
            argv[0] = str(argv[0])
            argv[1] = str(argv[1])
            argv[2] = str(argv[2])
            argv[3] = str(argv[3])
            argv[4] = str(argv[4])
            argv[5] = int(argv[5])
            index_from_file(argv[0], argv[1], argv[2], argv[3], argv[4], argv[5])
        except ValueError:
            logger_err.error("Invalid type of parameters (expected: <str> <str> <str> <str> <str> <int>)")
            sys.exit(1)
    else:
        logger_err.error("Invalid amount of parameters (expected: 6)")
        sys.exit(1)


if __name__ == "__indexer__":
    main(sys.argv[1:])


# index_from_file("./backups/", "localhost", "9200", "r_depression_train", "reddit_doc", 1000)
