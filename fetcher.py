import logging
import time
import json
import os
#####
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
                updated_post[key] = post.d_[key]

        except KeyError as e:
            logger_err.error("Key not found [{}] - skipping post with id: {}".format(e, p_id))
            updated_post = {}  # if it doesn't have all the requested fields, discard the whole post
    else:
        updated_post = post.d_

    return updated_post


def extract_historic_for_subreddit(subreddit: str, start_date: int):
    """
    Function that given a subreddit and the date to search from, performs a full historical search of all the posts
    of that subreddit and dumps them to a file

    :param subreddit: str - the subreddit name
    :param start_date: int - the base date to search from
    """

    # To put the timestamp in the filename
    timestamp = date_utils.get_current_date(False)

    # API
    api = PushshiftAPI()

    #####

    # Count of correct docs saved
    ok_docs = 0

    # Measure elapsed time
    start = time.time()

    print("Starting generation of '{}' subreddit historic...".format(subreddit))

    if subreddit is not None:
        response = api.search_submissions(q="", subreddit=subreddit, sort_type="created_utc", sort="desc",
                                          before=start_date)

        try:
            with open(os.path.join("./backups/", "r_{}_{}.jsonl".format(subreddit, timestamp)), "a") as outfile:
                for resp_post in response:
                    post = convert_response(resp_post, False)
                    test = json.dumps(post)

                    if bool(post) and test.startswith('{"id":'):
                        try:
                            # File backup
                            json.dump(post, outfile)
                            outfile.write('\n')
                            saved = True
                        except UnicodeEncodeError:
                            logger_err.error("Encoding error has occurred")
                            continue

                        if saved:
                            ok_docs += 1

                end = time.time()
                elapsed_time = end - start
                logger.debug("Total elapsed time performing the historical search: {} seconds".format(elapsed_time))
                logger.debug("Total posts obtained: {}".format(ok_docs))
        except (OSError, IOError):
            logger_err.error("Read/Write error has occurred with file '{}'".format
                             ("r_{}_{}.jsonl".format(subreddit, subreddit)))
    else:
        logger_err.error("Errored subreddit format: {}".format(subreddit))


def extract_posts_for_interval(start_date: int, end_date: int, size: int, timestamp: int, query: str, params: bool,
                               exclude: Optional[str] = None):
    """
    Function that extracts N (size) posts in a given time interval

    :param start_date: str - the date to search from
    :param end_date: str - the date to search to
    :param size: int - maximum number of posts to be retrieved
    :param timestamp: int - timestamp for the filename
    :param query: str/None - query to be performed
    :param params: bool - if you want to add parameters to the post or not
    :param exclude: str/None - the subreddit to skip
    :return dict - elapsed time performing the query and number of successfully saved documents
    """

    # Parameters
    thematic = False
    to_skip = exclude if exclude is not None else ""

    # API
    api = PushshiftAPI()

    #####

    # Count of correct docs saved
    ok_docs = 0

    # Measure elapsed time
    start = time.time()

    if query is not None:
        print("Interval: [{} - {}]".format(date_utils.convert_to_iso_date_str(start_date),
                                           date_utils.convert_to_iso_date_str(end_date)))

        response = api.search_submissions(q="", sort_type="created_utc", sort="desc", before=start_date,
                                          after=end_date)

        try:
            with open(os.path.join("./backups/", "ref_col_{}_{}.jsonl".format(size, timestamp)), "a") as outfile:
                for resp_post in response:
                    if ok_docs == size:
                        break
                    post = convert_response(resp_post, False)
                    test = json.dumps(post)

                    if bool(post) and test.startswith('{"id":'):
                        try:
                            if to_skip != post["subreddit"]:
                                if params:
                                    # Extra fields (only if post is not empty): no query and 'random_baseline' for
                                    # the scale (thematic set to False)
                                    post["parameters"] = {"query": "", "scale": "random_baseline", "thematic": thematic}

                                try:
                                    # File backup
                                    json.dump(post, outfile)
                                    outfile.write('\n')
                                    saved = True
                                except UnicodeEncodeError:
                                    logger_err.error("Encoding error has occurred")
                                    continue

                                if saved:
                                    ok_docs += 1
                        except KeyError:
                            logger_err.error("Missing subreddit key and skipping post")
                            continue

                end = time.time()
                elapsed_time = end - start

                return {"elapsed_time": elapsed_time, "ok_docs": ok_docs}
        except (OSError, IOError):
            logger_err.error("Read/Write error has occurred with file '{}'".format
                             ("ref_col_{}_{}.jsonl".format(size, timestamp)))
    else:
        logger_err.error("Errored query format")
        return [0, 0]


def obtain_reference_collection(path: str, max_block_size: int, posts_per_block: int, base_date: int, params: bool,
                                exclude: Optional[str] = None, posts: Optional[Iterable] = None):
    """
    Function that given the path of the backups file and the size of the posts' intervals creates a reference
    collection of random posts

    # 1 January 2020 00:00:00 (GMT +00:00) -> 1577836800

    :param path: str - the path to the backups file
    :param max_block_size: int - number of posts per date interval
    :param posts_per_block: int - number of posts to obtain per interval
    :param base_date: int - the limit timestamp (posts must be older that this)
    :param params: bool - if you want to add parameters to the post or not
    :param exclude: str/None - the subreddit to skip
    :param posts: Iterable - a generator from ElasticSearch (omits file specified in 'path' if so) /
    None -> defaults to textIO from file in the path specified as parameter
    """

    # To put the timestamp in the filename
    timestamp = date_utils.get_current_date(False)

    print("Starting generation of the reference collection...")

    if posts is not None:
        print("Data coming from ES loaded...")
        resp = generate_blocks(posts, True, max_block_size, posts_per_block, base_date, timestamp, params, exclude)
    else:
        with open(path, "r") as readfile:
            resp = generate_blocks(readfile, False, max_block_size, posts_per_block, base_date, timestamp, params,
                                   exclude)

    end_date = resp["end_date"]
    if resp["current_block_size"] > 0:
        # Write remaining
        if resp["start_date"] < base_date:
            end_date = resp["last_post"]["created_utc"]
            second_resp = extract_posts_for_interval(resp["start_date"], end_date, resp["current_block_size"] + 1,
                                                     timestamp, "", params, exclude)
            # Join remaining
            merged = merge_backups("ref_col_{}_{}.jsonl".format(max_block_size, timestamp),
                                   "ref_col_{}_{}.jsonl".format(resp["current_block_size"] + 1, timestamp))
            # Delete once completed
            if merged:
                file_manager.remove_file("./backups/ref_col_{}_{}.jsonl".format(resp["current_block_size"] + 1,
                                                                                timestamp))

            resp["total_time"] += second_resp["elapsed_time"]  # Add the time spent with the remaining documents
            resp["ok_docs"] += second_resp["ok_docs"]  # Add the successful documents

    logger.debug("Generated documents between {} and {} with {} documents per interval (size {})".format(
        date_utils.convert_to_iso_date_str(resp["initial_date"]),
        date_utils.convert_to_iso_date_str(end_date),
        posts_per_block,
        max_block_size))
    logger.debug("Total elapsed time generating the collection: {} seconds".format(resp["total_time"]))
    logger.debug("{} documents where skipped because newer than {} and {} documents where generated".format(
        resp["skipped"],
        date_utils.convert_to_iso_date_str(base_date),
        resp["ok_docs"]))


def generate_blocks(posts: Iterable, es: bool, max_block_size: int, posts_per_block: int, base_date: int,
                    timestamp: int, params: bool, exclude: Optional[str] = None):
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
    :param exclude: str/None - the subreddit to skip
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
            start_date = temp["created_utc"]
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
                end_date = temp["created_utc"]

                # Write interval of random posts to file
                resp = extract_posts_for_interval(start_date, end_date, posts_per_block, timestamp, "", params, exclude)
                total_time += resp["elapsed_time"]
                ok_docs += resp["ok_docs"]

                print("Total number of documents collected: {}".format(ok_docs))

                # Reset
                current_block_size = 0
                start_date = end_date

    result = {"current_block_size": current_block_size, "ok_docs": ok_docs, "skipped": skipped,
              "total_time": total_time, "start_date": start_date, "end_date": end_date,
              "initial_date": initial_date, "last_post": last_post}

    return result


def merge_backups(file1: str, file2: str):
    """
    Function that merges two backup file into a single one, appending the contents of one to the other

    :param file1: str - the name of the first file
    :param file2: str - the name of the second file
    """

    try:
        with open(os.path.join("./backups/", file2), "r") as input_file:
            with open(os.path.join("./backups/", file1), "a") as append_file:
                for line in input_file:
                    try:
                        json.dump(line, append_file)
                        append_file.write("\n")
                    except UnicodeEncodeError:
                        logger_err.error("Encoding error has occurred")
                        return False
    except (OSError, IOError):
        logger_err.error("Error merging the files, skipping...")
        return False
    return True


def obtain_authors(subr_path: str, ref_path: str):
    """
    Given the path of the backups, extract the authors and fill two sets, were the elements of the second one are also
    not present in the first one. Generates two .txt files containing the authors in each backup.

    :param subr_path: str - path to the first file (i.e subreddit file)
    :param ref_path: str - path to the second file (i.e reference collection based on the previous one)
    """

    subr_authors = set()
    ref_authors = set()

    for i, file in enumerate([subr_path, ref_path]):
        with open(file, "r") as input_file:
            for line in input_file:
                try:
                    loaded = json.loads(line)
                    author = loaded["author"]
                    if i == 0 and author not in subr_authors:
                        subr_authors.add(author)
                    elif i == 1 and author not in subr_authors and not author in ref_authors:
                        ref_authors.add(author)
                except KeyError:
                    logger_err.error("Error in author key with post with ID: {}".format(loaded["id"]))

    with open("./data/subr_authors.txt", "w") as output:
        subr_authors_list = list(subr_authors)
        for a in subr_authors_list:
            output.write(a + "\n")

    with open("./data/ref_authors.txt", "w") as output:
        ref_authors_list = list(ref_authors)
        for a in ref_authors_list:
            output.write(a + "\n")


def obtain_authors_sample(authors_info_path: str, sample_size: int):
    """
    Given the path to a .jsonl file containing the info of the authors, generates another .jsonl file and an excel file
    containing the authors selected using systematic sampling (of the size given as parameter)

    :param authors_info_path: str - path to the .jsonl file containing the info of the authors
    :param sample_size: int- the size of the sample to be generated
    """

    import random
    import math
    import pandas as pd
    authors = []

    with open(authors_info_path, "r") as input_file:
        for auth in input_file:
            authors.append(json.loads(auth))
    logger.debug("Total amount of authors in file: {}".format(len(authors)))

    selected = []
    k = len(authors)/sample_size
    starting_point = random.random() * k
    while starting_point <= len(authors):
        index = math.ceil(starting_point) - 1
        selected.append(authors[index])
        starting_point += k

    with open("./data/authors_selected.jsonl", "w+") as output:
        for auth in selected:
            output.write(json.dumps(auth))
            output.write("\n")

    df = pd.DataFrame(selected)
    df.to_excel("./data/authors_selected.xlsx")


# obtain_reference_collection("./backups/r_depression_base.jsonl", 100, 100, 1577836800, False, "depression", None)
# extract_historic_for_subreddit("depression", 1577836800)
# obtain_authors_sample("./data/ref_authors_info_backup.jsonl", 10000)
# obtain_authors("./backups/r_depression_base.jsonl", "./backups/reference_collection.jsonl")
