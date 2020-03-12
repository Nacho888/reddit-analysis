import elasticsearch
import json
import os
import logging
#####
import date_utils
import fetcher
import file_manager
import logging_factory
#####
from elasticsearch import Elasticsearch, helpers
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

    return response if bool(response) else {}


def extract_queries(path: str, filename: str):
    """
    Function that extracts the queries from a file (with the required ElasticSearch .json format) given a path and
    prints the response if OK

    :param path: str - the path to the queries folder
    :param filename: str - the name of the file

    """
    with open(os.path.join(path, filename), "r") as file:
        data = json.load(file)
        for i, query in enumerate(data):
            if i != 0:
                response = perform_search("depression_index", "localhost", "9200", query, data[0]["descriptions"][i])
                if bool(response):
                    print(response)
                else:
                    logger_err.error("No results found for query {} - {}".format(i, data[0]["descriptions"][i]))


def extract_posts_ordered_by_timestamp(generate_file: bool, max_block_size: int, posts_per_block: int, base_date: int):
    """
    Function that writes to a file all the posts in ElasticSearch (sorted by descending date)

    :param generate_file: bool - True if you want to merge all docs in a single document ordered by date, False if you
    just want to generate the reference collection using ElasticSearch
    :param max_block_size: int - number of posts per date interval
    :param posts_per_block: int - number of posts to obtain per interval
    :param base_date: int - the limit timestamp (posts must be older that this)

    """
    # To put the timestamp in the filename
    timestamp = date_utils.get_current_timestamp("0100")
    filename = "all_queries_{}.jsonl".format(timestamp)

    body = {"query": {"match_all": {}}, "sort": [{"created_utc": {"order": "desc"}}]}
    try:
        es = Elasticsearch([{"host": "localhost", "port": "9200"}], timeout=30)
        try:
            # Use scan to return a generator
            # preserve_order = True -> may impact performance but we need to preserve the date order of the query
            response = helpers.scan(es, query=body, preserve_order=True, index="depression_index-2")
            if bool(response):
                if generate_file:
                    for post in response:
                        file_manager.write_to_file(post["_source"], "./backups", filename)
                # Finally generate the reference collection using the response from ElasticSearch
                fetcher.obtain_reference_collection("", max_block_size,
                                                    posts_per_block, base_date, response)
        except (elasticsearch.NotFoundError, elasticsearch.RequestError):
            logger_err.error("Error when performing the query against ElasticSearch - {}".format("Posts ordered"
                                                                                                 "by timestamp"))
    except (elasticsearch.ConnectionTimeout, elasticsearch.ConnectionError) as e:
        logger_err.error("ElasticSearch client problem (check if open)")
        logger_err.error(e)
        pass


def obtain_posts_per_hour_interval():
    """
    Function that prints all the hour intervals [0-23] with their document count

    """
    for i in range(24):
        to = i + 1 if i < 23 else 0

        body = {"size": 0,
                "aggs": {"Hour ranges": {"range": {"field": "post_hour", "ranges": [{"from": i, "to": to}]}}}
                }

        response = perform_search("depression_index", "localhost", "9200", body,
                                  "Posts from {} hours to {} hours".format(i, to))
        print(response)


def obtain_posts_per_hour():
    """
    Function that print all the hours [0-23] (not intervals) with their corresponding number of posts

    """
    name = "Posts per hour"
    key_name = "Hour"
    body = {"size": 0, "aggs": {
        name: {"composite": {"size": 24, "sources": [{key_name: {"terms": {"field": "post_hour"}}}]}}
    }
            }

    response = perform_search("depression_index", "localhost", "9200", body, "Posts per hour")
    resp_dict = {}
    for key in response["aggregations"][name]["buckets"]:
        resp_dict[key["key"][key_name]] = key["doc_count"]

    print(resp_dict)


# obtain_posts_per_hour()
# extract_queries(".", "queries.json")
extract_posts_ordered_by_timestamp(False, 1000, 1000, 1577836800)
