import logging
import time
#####
import excel_reader
import file_manager
import logging_factory
#####
from psaw import PushshiftAPI
from datetime import datetime
#####
logger_err = logging_factory.get_module_logger("fetcher_err", logging.ERROR)
logger = logging_factory.get_module_logger("fetcher", logging.DEBUG)


def get_timestamp():
    """
    Function that returns the current timestamp

    :return: timestamp: int - current timestamp
    """

    # Time format for UTC
    time_format = "%Y-%m-%d %H:%M:%S"

    timestamp = datetime.utcfromtimestamp(int(time.time())).strftime(time_format)
    # +0001 for Spain GMT
    date = datetime.strptime(timestamp + "+0001", time_format + "%z")
    timestamp = date.timestamp()

    return int(timestamp)


def convert_response(post: dict):
    """
    Function to convert the response into a list with all the required data

    :param post: dict - the input dictionary with all the data of the post (we only mind the key "d_" where all the
    important data is stored)
    :return: updated_post: dict - the post the return containing only the required fields returned by the API

    """
    try:
        p_id = post.d_[id]
    except KeyError:
        p_id = "no_post_id_found"
        pass

    try:
        updated_post = dict(id=post.d_["id"], url=post.d_["url"], title=post.d_["title"], author=post.d_["author"],
                            selftext=post.d_["selftext"], created_utc=post.d_["created_utc"],
                            retrieved_on=post.d_["retrieved_on"], subreddit=post.d_["subreddit"],
                            subreddit_id=post.d_["subreddit_id"], subreddit_type=post.d_["subreddit_type"],
                            domain=post.d_["domain"], gildings=post.d_["gildings"],
                            num_comments=post.d_["num_comments"], score=post.d_["score"], over_18=post.d_["over_18"],
                            permalink=post.d_["permalink"])

    except KeyError as e:
        logger_err.error("Key not found '{}' - skipping post with id: {}".format(e, p_id))
        updated_post = {}  # if it doesn't have all the requested fields, discard the whole post
        pass

    return updated_post


def extract_posts(excel_path: str, max_posts_per_query: int):
    """
    Main function that given an excel spreadsheet with the required format (could be changed in the module
    excel_reader.py) performs the queries for each scale and extract posts storing them in .jsonl format as backup

    :param  excel_path: str - path to the excel spreadsheet containing the scales and the queries
    :param  max_posts_per_query: int - maximum number of posts to be extracted with each query
    (adjust it carefully taking into account the time compromise)

    """

    # Parameters

    # From row [5 - end]
    # Column 2 (B) stores scale names, column 4 (D) stores queries
    queries_and_scales = excel_reader.get_queries_and_scales(excel_path, 5, 2, 4, 5)

    thematic = True

    # API
    api = PushshiftAPI()

    # Time calculation
    total_elapsed_time = 0

    #####

    # Used to show the progress of completion when making the requests
    total_queries = sum([len(x) for x in queries_and_scales.values()])
    current_query = 1

    # Count of correct docs saved
    ok_docs = 0

    # To store failed queries and directory errors
    errors = []

    logger.debug("Starting...\n")

    for scale in queries_and_scales:
        for query_data in queries_and_scales[scale]:

            # Related scales
            related_codes = query_data[1]
            related_scales = query_data[2]

            # Actual query
            query = query_data[0]

            logger.debug("Trying to perform query: '{}'".format(query))

            # Timestamp for the filename
            query_timestamp = get_timestamp()

            # Measure elapsed time
            start = time.time()

            # Base call with 1 post to extract most recent date
            response = api.search_submissions(q=query, sort_type="created_utc", sort="desc", limit=1)
            base_dict = convert_response(next(response))

            if base_dict:  # We have obtained some result for the query

                # Posts before the most recent post obtained
                most_recent_utc = base_dict["created_utc"]
                response = api.search_submissions(q=query, before=most_recent_utc, limit=max_posts_per_query)

                for resp_post in response:
                    post = convert_response(resp_post)

                    if post is not {}:
                        # Extra fields: current timestamp and query data
                        post["parameters"] = {"query": query, "scale": scale, "thematic": thematic, "related": {
                            "codes": related_codes,
                            "related_scales": related_scales
                        }}
                        post["timestamp"] = get_timestamp()

                        #####

                        # File backup
                        saved = file_manager.write_post_to_backup(post, query, scale, query_timestamp)
                        if saved:
                            ok_docs += 1
            else:
                # Add error to print it later
                errors.append("No results found for query: '{}'".format(query))

            end = time.time()
            elapsed_time = end - start
            total_elapsed_time += elapsed_time

            # Print stats
            logger.debug("{} documents successfully saved (expected {})".format(ok_docs, max_posts_per_query))
            ok_docs = 0

            logger.debug("Query: '{}' - {} of {} performed in {} seconds\n".format(
                query,
                current_query,
                total_queries,
                '%.3f' % elapsed_time))
            current_query += 1

    # Show errors (if present)
    if len(errors) > 0:
        for e in errors:
            logger_err.error(e)

    logger.debug("All {} queries performed in a total of {} seconds\n".format(
        total_queries,
        '%.3f' % total_elapsed_time))


# extract_posts("./excel/scales.xlsx", 10)
