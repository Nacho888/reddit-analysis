import uuid
import logging
import sys
import os
import elasticsearch
#####
import logging_factory
#####
from typing import Optional
from elasticsearch import Elasticsearch, helpers

#####
logger_err = logging_factory.get_module_logger("elastic_err", logging.ERROR)
logger = logging_factory.get_module_logger("elastic", logging.DEBUG)


def setup_for_index(data: list, _index: str, _type: str, _id: Optional[int] = None):
    """
    Function to add the necessary fields to index with ElasticSearch

    :param data: list - the data to modify and set ready to index
    :param _index: str - the index name
    :param _type: str - the document type
    :param _id: int/None - the id to assign to the document or if None an automatically generated uuid4
    :return yielded document with all the required data

    """

    for doc in data:
        yield {
            "_index": _index,
            "_id": uuid.uuid4() if _id is None else _id,
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

    try:
        es = Elasticsearch([{"host": host, "port": port}])
        # logger.debug("Connection established")

        # Create index (if not exists)
        if not es.indices.exists(index=_index):
            es.indices.create(index=_index, ignore=[400, 404])

        # Load data
        try:
            resp = helpers.bulk(es, setup_for_index(data, _index, _type, None), index=_index, doc_type=_type)
        except helpers.BulkIndexError:
            logger_err.error("helpers.bulk() - ERROR\n")
            sys.exit(1)
    except (elasticsearch.ConnectionTimeout, elasticsearch.ConnectionError):
        logger_err.error("ElasticSearch client problem (check if open)")
        sys.exit(1)

    return resp[0] if resp is not None else 0


def index_from_file(path: str, host: str, port: str, _index: str, _type: str, limit: int):
    """
    Function that given the path where all the backups are stored, indexes all the documents

    :param path: str - path where the backups' folder is
    :param host: str - host to connect to
    :param port: str - port to connect to
    :param _index: str - the index name
    :param _type: str - the document type
    :param limit: int - amount of lines to be

    """

    # TODO: seems to be indexing one document less per file...

    lines = []
    ok_docs = 0

    logger.debug("Parameters to establish connection with ElasticSearch -> (host: '{}', port: '{}')".format(host, port))

    for subdir, dirs, files in os.walk(path):
        for file in files:
            for line in open(os.path.join(subdir, file)):
                if len(lines) == limit:
                    ok_docs += index_data(lines, host, port, _index, _type)
                    lines = []
                else:
                    lines.append(line)
            # There's remaining documents
            if len(lines) > 0:
                ok_docs += index_data(lines, host, port, _index, _type)
                lines = []

    logger.debug("{} documents indexed successfully\n".format(ok_docs))


# index_from_file("./backups", "localhost", "9200", "depression_index", "reddit_doc", 500)
