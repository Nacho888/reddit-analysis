import codecs
import json
import time
import traceback
import os
import logging
#####
import elastic
import excel_reader
import logging_factory
#####
from psaw import PushshiftAPI
from datetime import datetime
#####
logger = logging_factory.get_module_logger("main", logging.DEBUG)


def convert_response(gen, max_cache_size):
    """ Function to convert the response into a list with all the required data

        Parameters:
            gen -- generator/iterator
                the generator containing the data of the query
            max_cache_size -- int
                maximum number of posts to be extracted from the generator until it stops
        Returns:
            cache -- list
                a list of dictionaries with only the necessary data
    """

    cache = []

    for post in gen:
        dict_to_add = {"title": post.d_["title"],
                       "author": post.d_["author"],
                       "subreddit": post.d_["subreddit"],
                       "created_utc": post.d_["created_utc"],
                       "selftext": post.d_["selftext"]}

        cache.append(dict_to_add)

        if len(cache) >= max_cache_size:
            break

    return cache


def extract_posts(excel_path, want_to_backup, want_to_index, host, port, _index, _type, max_posts_per_query):
    """ Main function that given an excel spreadsheet with the required format (could be changed in the module
    excel_reader.py) performs the queries for each scale and extract posts storing them in .json format as backup
    and if needed indexes them to ElasticSearch

    Parameters:
            excel_path -- string
                path to the excel spreadsheet
            want_to_backup -- boolean
                true if you want to save the documents, false otherwise
            want_to_index -- boolean
                true if you want to index the documents to ElasticSearch, false otherwise
            host -- string
                host to connect to ElasticSearch
            port -- string
                port to connect to ElasticSearch
            _index -- string
                the index name for ElasticSearch
            _type -- string
                the document type for ElasticSearch
            max_posts_per_query -- int
                maximum number of posts to be extracted with each query (adjust it carefully taking into account
                the time compromise)

    """

    # List to store all queries
    all_queries = []

    # Time format for UTC
    time_format = "%Y-%m-%d %H:%M:%S"

    # Parameters

    # From row [5 - end]
    # Column 2 (B) stores scale names, column 4 (D) stores queries
    queries_and_scales = excel_reader.get_queries_and_scales(excel_path, 5, 2, 4)

    thematic = True
    _index = _index
    _type = _type

    # API
    api = PushshiftAPI()

    # Time calculation
    total_elapsed_time = 0

    #####

    # Used to show the progress of completion when making the requests
    total_queries = sum([len(x) for x in queries_and_scales.values()])
    current_query = 1

    logger.debug("Starting...")

    for scale in queries_and_scales:
        for query in queries_and_scales[scale]:

            # Measure elapsed time
            start = time.time()

            # To store failed queries and directory errors
            errors = []

            # Base call with 25 posts to extract most recent date
            response = api.search_submissions(q=query, limit=25)
            submissions_list = convert_response(response, max_posts_per_query)

            if len(submissions_list) > 0:  # We have obtained some result for the query

                # Posts before the most recent post obtained
                most_recent_utc = submissions_list[0]["created_utc"]
                response = api.search_submissions(q=query, before=most_recent_utc)
                submissions_list = convert_response(response, max_posts_per_query)

                # Extra fields: current timestamp and query data
                for sub in submissions_list:
                    timestamp = datetime.utcfromtimestamp(int(time.time())).strftime(time_format)
                    # +0001 for Spain GMT
                    date = datetime.strptime(timestamp + "+0001", time_format + "%z")
                    timestamp = date.timestamp()
                    sub["parameters"] = {"query": query, "scale": scale, "thematic": thematic}
                    sub["timestamp"] = int(timestamp)

                #####

                if want_to_index:
                    # Index with ElasticSearch posts' list
                    elastic.index_data(host, port, submissions_list, _index, _type)
                    logger.debug("Documents indexed")

                # Add to global queries list
                all_queries.append(submissions_list)

                #####

                if want_to_backup:
                    # File backup
                    try:
                        save_path = "./backups/"
                        # Create, if not present, folder to store posts" backups
                        if not os.path.isdir(save_path):
                            os.mkdir(save_path)

                        query_name_file = query.replace(" ", "-")
                        filename = "{}_{}_backup.json".format(query_name_file, scale)

                        # One directory per scale
                        directory = save_path + scale + "/"
                        os.mkdir(directory)
                    except FileExistsError:
                        # Add error to print it later
                        errors.append("Directory '{}' already exists".format(directory))
                        pass

                    save_path = directory

                    # Write .json file backup
                    try:
                        with codecs.open(save_path + filename, "w", encoding="utf8") as outfile:
                            json.dump(submissions_list, outfile, indent=4)
                        logger.debug("File saved")
                    except UnicodeEncodeError:
                        logger.exception("Encoding error has occurred")
            else:
                # Add error to print it later
                errors.append("No results found for query: '{}'".format(query))

            end = time.time()
            elapsed_time = end - start
            total_elapsed_time += elapsed_time

            # Print stats
            logger.debug("Query {} of {} performed in {} seconds".format(
                current_query,
                total_queries,
                '%.3f' % elapsed_time))
            current_query += 1

    # Show errors (if present)
    if len(errors) > 0:
        for e in errors:
            logger.error(e)

    logger.debug("All {} queries performed in a total of {} seconds".format(
        total_queries,
        '%.3f' % total_elapsed_time))

    #####

    if want_to_backup:
        # File backup (all queries in a single file)
        save_path = "./backups/"
        filename = "all_queries_backup.json"

        # Write .json file backup
        try:
            with codecs.open(save_path + filename, "w", encoding="utf8") as outfile:
                json.dump(all_queries, outfile, indent=4)
            logger.debug("File saved")
        except UnicodeEncodeError:
            logger.exception("Encoding error has occurred")

    #####


extract_posts("./excel/little_scales.xlsm", True, True, "localhost", "9200", "depression_index", "reddit_doc", 5)
