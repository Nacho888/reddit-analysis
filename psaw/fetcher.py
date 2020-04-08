import logging
import sys
import time
import json
#####
import excel_reader
import file_manager
import logging_factory
import date_utils
#####
from psaw import PushshiftAPI
from typing import Optional, Iterable
#####
logger_err = logging_factory.get_module_logger("fetcher_err", logging.ERROR)
logger = logging_factory.get_module_logger("fetcher", logging.DEBUG)


def convert_response(post: dict, full_data: bool):
    """
    Function to convert the response into a list with all the required data

    :param post: dict - the input dictionary with all the data of the post (we only mind the key "d_" where all the
    important data is stored)
    :param full_data: bool - if you want the full data of the post (even if errored)
    :return: updated_post: dict - the post the return containing only the required fields returned by the API
    """

    keys_as_str = ["id", "url", "title", "author", "selftext", "created_utc", "retrieved_on", "subreddit",
                   "subreddit_id", "subreddit_type", "domain", "gildings", "num_comments", "score", "over_18",
                   "permalink"]

    if not full_data:
        try:
            p_id = post.d_["id"]
        except KeyError:
            p_id = "no_id"

        try:
            post_keys = [*post.d_]
            to_use_keys = [value for value in keys_as_str if value in post_keys]

            updated_post = {}

            for key in to_use_keys:
                if key == "created_utc" or key == "retrieved_on":
                    # Change date formats to ISO-8601 strings
                    updated_post[key] = date_utils.get_iso_date_str(post.d_[key], "0000")
                else:
                    updated_post[key] = post.d_[key]

        except KeyError as e:
            logger_err.error("Key not found [{}] - skipping post with id: {}".format(e, p_id))
            updated_post = {}  # if it doesn't have all the requested fields, discard the whole post
    else:
        updated_post = post.d_

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

    # Timestamp for the filename
    query_timestamp = date_utils.get_current_timestamp("0100")

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

            # Measure elapsed time
            start = time.time()

            # Base call with 1 post to extract most recent date
            response = api.search_submissions(q=query, sort_type="created_utc", sort="desc", limit=1)
            base_dict = convert_response(next(response), False)

            if base_dict:  # We have obtained some result for the query

                # Posts before the most recent post obtained
                most_recent_utc = base_dict["created_utc"]
                response = api.search_submissions(q=query, before=most_recent_utc, limit=max_posts_per_query)

                for resp_post in response:
                    post = convert_response(resp_post, False)
                    test = json.dumps(post)

                    if bool(post) and test.startswith('{"id":'):
                        # Extra fields (only if post is not empty): current timestamp and query data (thematic
                        # set to True)
                        post["parameters"] = {"query": query, "scale": scale, "thematic": thematic, "related": {
                            "codes": related_codes,
                            "related_scales": related_scales}}
                        # Change date formats to ISO-8601 strings
                        post["timestamp"] = date_utils.get_iso_date_str(query_timestamp, "0100")

                        #####

                        # File backup
                        saved = file_manager.write_to_file(post, "./backups/", "all_queries_{}".format(query_timestamp),
                                                           "a")
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


def extract_historic_for_subreddit(subreddit: str, start_date: int):
    """
    Function that given a subreddit and the date to search from, performs a full historical search of all the posts
    of that subreddit and dumps them to a file

    :param subreddit: str - the subreddit name
    :param start_date: int - the base date to search from
    """

    # To put the timestamp in the filename
    timestamp = date_utils.get_current_timestamp("0100")

    # API
    api = PushshiftAPI()

    #####

    # Count of correct docs saved
    ok_docs = 0

    # Measure elapsed time
    start = time.time()

    if subreddit is not None:
        response = api.search_submissions(q="", subreddit=subreddit, sort_type="created_utc", sort="desc",
                                          before=start_date)

        for resp_post in response:
            post = convert_response(resp_post, False)
            test = json.dumps(post)

            if bool(post) and test.startswith('{"id":'):
                # File backup
                saved = file_manager.write_to_file(post, "./backups/", "r_{}_{}.jsonl".format(subreddit, timestamp),
                                                   "a")
                if saved:
                    ok_docs += 1

        end = time.time()
        elapsed_time = end - start
        logger.debug("Total elapsed time performing the historical search: {} seconds".format(elapsed_time))
        logger.debug("Total posts obtained: {}".format(ok_docs))
    else:
        logger_err.error("Errored subreddit format: {}".format(subreddit))


def extract_posts_for_interval(start_date: int, end_date: int, size: int, timestamp: int, query: str, params: bool):
    """
    Function that extracts N (size) posts in a given time interval

    :param start_date: str - the date to search from
    :param end_date: str - the date to search to
    :param size: int - maximum number of posts to be retrieved
    :param timestamp: int - timestamp for the filename
    :param query: str/None - query to be performed
    :param params: bool - if you want to add parameters to the post or not
    :return dict - elapsed time performing the query and number of successfully saved documents
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

    if query is not None:
        response = api.search_submissions(q="", sort_type="created_utc", sort="desc", before=start_date,
                                          after=end_date, limit=size)

        for resp_post in response:
            post = convert_response(resp_post, False)
            test = json.dumps(post)

            if bool(post) and test.startswith('{"id":'):
                if params:
                    # Extra fields (only if post is not empty): no query and 'random_baseline' for the scale (thematic
                    # set to False)
                    post["parameters"] = {"query": "", "scale": "random_baseline", "thematic": thematic}

                # File backup
                saved = file_manager.write_to_file(post, "./backups/",
                                                   "ref_col_{}_{}.jsonl".format(size, timestamp), "a")
                if saved:
                    ok_docs += 1

        end = time.time()
        elapsed_time = end - start

        return {"elapsed_time": elapsed_time, "ok_docs": ok_docs}

    else:
        logger_err.error("Errored query format")
        return [0, 0]


def obtain_reference_collection(path: str, max_block_size: int, posts_per_block: int, base_date: int, params: bool,
                                posts: Optional[Iterable] = None):
    """
    Function that given the path of the backups file and the size of the posts' intervals creates a reference
    collection of random posts

    # 1 January 2020 00:00:00 (GMT +00:00) -> 1577836800
    # 4 March 2020 00:00:00 (GMT +00:00) -> 1583280000

    :param path: str - the path to the backups file
    :param max_block_size: int - number of posts per date interval
    :param posts_per_block: int - number of posts to obtain per interval
    :param base_date: int - the limit timestamp (posts must be older that this)
    :param params: bool - if you want to add parameters to the post or not
    :param posts: Iterable - a generator from ElasticSearch (omits file specified in 'path' if so) /
    None -> defaults to textIO from file in the path specified as parameter
    """

    # To put the timestamp in the filename
    timestamp = date_utils.get_current_timestamp("0100")

    if posts is not None:
        print("Data coming from ES loaded...")
        resp = generate_blocks(posts, True, max_block_size, posts_per_block, base_date, timestamp, params)
    else:
        with open(path, "r") as readfile:
            resp = generate_blocks(readfile, False, max_block_size, posts_per_block, base_date, timestamp, params)

    end_date = resp["end_date"]
    if resp["current_block_size"] > 0:
        # Write remaining
        if resp["start_date"] < base_date:
            end_date = date_utils.get_numeric_timestamp_from_iso(resp["last_post"]["created_utc"])
            second_resp = extract_posts_for_interval(resp["start_date"], end_date, posts_per_block,
                                                     timestamp, "", params)
            resp["total_time"] += second_resp["elapsed_time"]  # Add the time spent with the remaining documents
            resp["ok_docs"] += second_resp["ok_docs"]  # Add the successful documents

    logger.debug("Generated documents between {} and {} with {} documents per interval (size {})".format(
        resp["initial_date"],
        date_utils.get_iso_date_str(end_date),
        posts_per_block,
        max_block_size))
    logger.debug("Total elapsed time generating the collection: {} seconds".format(resp["total_time"]))
    logger.debug("{} documents where skipped because newer than {} and {} documents where generated".format(
        resp["skipped"],
        date_utils.get_iso_date_str(base_date),
        resp["ok_docs"]))


def generate_blocks(posts: Iterable, es: bool, max_block_size: int, posts_per_block: int, base_date: int,
                    timestamp: int, params: bool):
    """
    Function that given an Iterable containing the posts, the interval between posts, the posts to generate within that
    interval, the base date (posts must be older than this date) and a timestamp for the filename

    :param posts: Iterable - posts to obtain the intervals from
    :param es: bool - True if the posts are coming from ElasticSearch, False otherwise
    :param max_block_size: int - the posts to skip to find a new date
    :param posts_per_block: int - the amount of posts to be generated within the interval
    :param base_date: int - the limit timestamp (posts must be older that this)
    :param timestamp: int - the timestamp for the filename
    :param params: bool - if you want to add parameters to the post or not
    :return: dict - all the data obtained during the generation of the collection
        current_block_size: int - posts remaining
        ok_docs: int - documents successfully saved
        skipped: int - documents skipped (newer than date)
        total_time: float - total time spent generating the collecting
        skipped: int - start date of the final interval if there were documents remaining
        end_date: int - end date of the last interval
        initial_date: str - the first date obtained older that the base date
        last_post: dict - the last post obtained
    """

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

    for line in posts:
        if es:
            temp = line["_source"]
        else:
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
                if es:
                    temp = line["_source"]
                else:
                    temp = json.loads(line)
                end_date = date_utils.get_numeric_timestamp_from_iso(temp["created_utc"])

                # Write interval of random posts to file
                resp = extract_posts_for_interval(start_date, end_date, posts_per_block, timestamp, "", params)
                total_time += resp["elapsed_time"]
                ok_docs += resp["ok_docs"]

                # Reset
                current_block_size = 0
                start_date = end_date

    result = {"current_block_size": current_block_size, "ok_docs": ok_docs, "skipped": skipped,
              "total_time": total_time, "start_date": start_date, "end_date": end_date,
              "initial_date": initial_date, "last_post": last_post}

    return result


def main(argv):
    if len(argv) == 2:
        try:
            argv[0] = str(argv[0])
            argv[1] = int(argv[1])
            extract_posts_from_scales(argv[0], argv[1])
        except ValueError:
            logger_err.error("Invalid type of parameters (expected: <str> <str>)")
            sys.exit(1)
    else:
        logger_err.error("Invalid amount of parameters (expected: 2)")
        sys.exit(1)


if __name__ == "__fetcher__":
    main(sys.argv[1:])

# extract_posts_from_scales("./excel/one_query.xlsx", 1000)
# obtain_reference_collection("./backups/r_depression_1585651968.jsonl", 1000, 5000, 1577836800, False, None)
# extract_historic_for_subreddit("depression", 1577836800)
