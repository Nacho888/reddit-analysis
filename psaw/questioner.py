import elasticsearch
import json
import os
#####
import logging_factory
import logging
#####
from elasticsearch import Elasticsearch
#####
logger_err = logging_factory.get_module_logger("questioner_err", logging.ERROR)
logger = logging_factory.get_module_logger("questioner", logging.DEBUG)


def perform_search(index: str, host: str, port: str, body: dict, description: str):
    """
    Function that performs a query against ElasticSearch

    :param index: str - index to perform the query against to
    :param host: str - host to perform the query against to
    :param port: str - port to perform the query against to
    :param body: dict - the body of the query
    :param description: str - description of the query performed
    :return: response: dict - dictionary containing the response (empty if errored)

    """
    body = json.dumps(body)
    response = {}
    try:
        es = Elasticsearch([{"host": host, "port": port}])
        try:
            response = es.search(index=index, body=body)
        except (elasticsearch.NotFoundError, elasticsearch.RequestError):
            logger_err.error("Error when performing the query against ElasticSearch - {}".format(description))
    except (elasticsearch.ConnectionTimeout, elasticsearch.ConnectionError):
        logger_err.error("ElasticSearch client problem (check if open)")
        pass

    return response if response is not {} else {}


def extract_queries(path: str, filename: str):
    """
    Function that extracts the queries from a file (with the required ElasticSearch .json format) given a path and
    prints the response if OK

    :param path: str - the path to the queries folder
    :param filename: str - the name of the file

    """
    with open(os.path.join(path, filename), 'r') as file:
        data = json.load(file)
        for i, query in enumerate(data):
            if i != 0:
                response = perform_search("depression_index", "localhost", "9200", query, data[0]["descriptions"][i])
                if response is not {}:
                    print(response)
                else:
                    logger_err.error("No results found for query {} - {}".format(i, data[0]["descriptions"][i]))


extract_queries(".", "queries.json")
