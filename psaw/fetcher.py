import logging
import os
import time
import json
#####
import excel_reader
import file_manager
import logging_factory
import date_utils
#####
from psaw import PushshiftAPI
#####
logger_err = logging_factory.get_module_logger("fetcher_err", logging.ERROR)
logger = logging_factory.get_module_logger("fetcher", logging.DEBUG)


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
        p_id = "no_id"
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
        logger_err.error("Key not found [{}] - skipping post with id: {}".format(e, p_id))
        updated_post = {}  # if it doesn't have all the requested fields, discard the whole post
        pass

    return updated_post


def extract_posts_from_scales(excel_path: str, max_posts_per_query: int):
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
            query_timestamp = date_utils.get_current_timestamp("0100")

            # Measure elapsed time
            start = time.time()

            # Base call with 1 post to extract most recent date
            response = api.search_submissions(q=query, sort_type="created_utc", sort="desc", limit=1)
            base_dict = convert_response(next(response, False))

            if base_dict:  # We have obtained some result for the query

                # Posts before the most recent post obtained
                most_recent_utc = base_dict["created_utc"]
                response = api.search_submissions(q=query, before=most_recent_utc, limit=max_posts_per_query)

                for resp_post in response:
                    post = convert_response(resp_post)

                    if bool(post):
                        # Extra fields (only if post is not empty): current timestamp and query data (thematic
                        # set to True)
                        post["parameters"] = {"query": query, "scale": scale, "thematic": thematic, "related": {
                            "codes": related_codes,
                            "related_scales": related_scales
                        }}
                        # Change date formats to ISO-8601 strings
                        post["created_utc"] = date_utils.get_iso_date_str(post["created_utc"], "0000")
                        post["retrieved_on"] = date_utils.get_iso_date_str(post["retrieved_on"], "0000")
                        post["timestamp"] = date_utils.get_iso_date_str(query_timestamp, "0100")

                        #####

                        # File backup
                        saved = file_manager.write_scale_post_to_backup(post, query, scale, query_timestamp)
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


def extract_posts(start_date: str, end_date: str, size: int, timestamp: str):
    """
    Function that extracts N (size) posts in a given time interval

    :param start_date: str - the date to search from
    :param end_date: str - the date to search to
    :param size: int - maximum number of posts to be retrieved
    :param timestamp: str - timestamp for the filename
    :return list - elapsed time performing the query and number of successfully saved documents

    """
    # Parameters
    thematic = False

    # API
    api = PushshiftAPI()

    #####

    # Count of correct docs saved
    ok_docs = 0

    # Measure elapsed time
    start = time.time()

    response = api.search_submissions(query="", sort_type="created_utc", sort="desc", before=start_date, after=end_date,
                                      limit=size)

    for resp_post in response:
        post = convert_response(resp_post)

        if bool(post):
            # Extra fields (only if post is not empty): no query and 'random_baseline' for the scale (thematic
            # set to False)
            post["parameters"] = {"query": "", "scale": "random_baseline", "thematic": thematic}

            # Change date formats to ISO 8601
            post["created_utc"] = date_utils.get_iso_date_str(post["created_utc"], "0000")
            post["retrieved_on"] = date_utils.get_iso_date_str(post["retrieved_on"], "0000")

            # File backup
            saved = file_manager.write_to_file(post, "./backups/", "reference_collection_{}.jsonl".format(timestamp))
            if saved:
                ok_docs += 1

    end = time.time()
    elapsed_time = end - start

    return [elapsed_time, ok_docs]


def obtain_reference_collection(path: str, max_block_size: int, posts_per_block: int, base_date: int):
    """
    Function that given the path of the backups file and the size of the posts' intervals creates a reference
    collection of random posts

    # 1 January 2020 00:00:00 (GMT +00:00) -> 1577836800
    # 4 March 2020 00:00:00 (GMT +00:00) -> 1583280000

    :param path: str - the path to the backups file
    :param max_block_size: int - number of posts per date interval
    :param posts_per_block: int - number of posts to obtain per interval
    :param base_date: int - date to start searching

    """

    # To put the timestamp in the filename
    timestamp = date_utils.get_current_timestamp("0100")

    # Actual count of posts
    current_block_size = 0

    # Skipped posts because of newer date
    skipped = 0

    # To store the time intervals
    start_date = None
    end_date = None
    last_post = None

    # Stats
    total_time = 0
    ok_docs = 0
    initial_date = None

    with open(path, "r") as readfile:
        for line in readfile:
            temp = json.loads(line)
            last_post = temp

            if start_date is None:  # First time
                # Set initial date
                start_date = date_utils.get_numeric_timestamp_from_iso(temp["created_utc"])
                if start_date > base_date:  # Skip posts newer than date passed as parameter
                    start_date = None
                    skipped += 1
                else:
                    initial_date = temp["created_utc"]

            else:
                current_block_size += 1

                if current_block_size == max_block_size:
                    temp = json.loads(line)
                    end_date = date_utils.get_numeric_timestamp_from_iso(temp["created_utc"])

                    # Write interval of random posts to file
                    resp = extract_posts(start_date, end_date, posts_per_block, timestamp)
                    total_time += resp[0]
                    ok_docs += resp[1]

                    # Reset
                    current_block_size = 0
                    start_date = end_date

    if current_block_size > 0:
        # Write remaining
        if start_date < base_date:
            end_date = date_utils.get_numeric_timestamp_from_iso(last_post["created_utc"])
            extract_posts(start_date, end_date, posts_per_block,
                          timestamp)

    logger.debug("Generated documents between {} and {} with {} documents per interval (size {})".format(
        initial_date,
        end_date,
        posts_per_block,
        max_block_size))
    logger.debug("Total elapsed time generating the collection: {} seconds".format(total_time))
    logger.debug("{} documents where skipped because newer than {} and {} documents where generated".format(
        skipped,
        date_utils.get_iso_date_str(base_date),
        ok_docs))


# extract_posts_from_scales("./excel/one_query.xlsx", 1000)
obtain_reference_collection("./backups/all_queries_1583741552.jsonl", 1000, 1000, 1582243200)
