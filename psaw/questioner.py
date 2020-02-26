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


def perform_search(index: str, host: str, port: str, body: dict, title: str):
    body = json.dumps(body)
    response = {}
    try:
        es = Elasticsearch([{"host": host, "port": port}])
        try:
            response = es.search(index=index, body=body)
        except (elasticsearch.NotFoundError, elasticsearch.RequestError):
            logger_err.error("Error when performing the query against ElasticSearch - {}".format(title))
    except (elasticsearch.ConnectionTimeout, elasticsearch.ConnectionError):
        logger_err.error("ElasticSearch client problem (check if open)")
        pass

    return response if response is not {} else {}


def extract_queries(path: str):
    with open(os.path.join(path, "queries.json"), 'r') as readfile:
        data = json.load(readfile)
        for i, query in enumerate(data):
            if i != 0:
                response = perform_search("depression_index", "localhost", "9200", query, data[0]["comments"][i])
                if response is not {}:
                    print(response)
                else:
                    logger_err.error("No results found for query {} - {}".format(i, data[0]["comments"][i]))


extract_queries(".")
