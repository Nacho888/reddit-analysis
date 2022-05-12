import logging
import time
import json
import os
#####
import file_manager
import logging_factory
import date_utils
import indexer
import questioner
import tools
#####
from psaw import PushshiftAPI
from typing import Optional, Iterable
#####
logger_err = logging_factory.get_module_logger("fetcher_err", logging.ERROR)
logger = logging_factory.get_module_logger("fetcher", logging.DEBUG)


def convert_response(data: dict, full_data: bool, comment: bool = False):
    """
    Function to convert the response into a list with all the required data

    :param data: dict - the input dictionary with all the data of the post/comment (we only mind the key "d_" where all the
    important data is stored)
    :param full_data: bool - if you want the full data of the post/comment (even if errored)
    :param comment: bool - True if you want to convert a comment, False for a post
    :return: updated_data: dict - the post/comment the return containing only the required fields returned by the API
    """

    keys_as_str = ["id", "url", "title", "author", "selftext", "created_utc", "retrieved_on", "subreddit",
                   "subreddit_id", "subreddit_type", "domain", "gildings", "num_comments", "score", "over_18",
                   "permalink"] if not comment else ["id", "link_id", "parent_id", "author", "body", "created_utc", "retrieved_on", "subreddit",
                   "subreddit_id", "guilded"]

    if not full_data:
        try:
            d_id = data.d_["id"]
        except KeyError:
            d_id = "no_id"

        try:
            d_keys = [*data.d_]
            to_use_keys = [value for value in keys_as_str if value in d_keys]

            updated_data = {}

            for key in to_use_keys:
                updated_data[key] = data.d_[key]

        except KeyError as e:
            logger_err.error("Key not found [{}] - skipping post with id: {}".format(e, d_id))
            updated_data = {}  # if it doesn't have all the requested fields, discard the whole post
    else:
        updated_data = data.d_

    return updated_data


def extract_historic_for_subreddit(subreddit: str, comments: bool = False, start_date: Optional[int] = None):
    """
    Function that given a subreddit and the date to search from, performs a full historical search of all the posts
    of that subreddit and dumps them to a file

    :param subreddit: str - the subreddit name
    :param start_date: [int]/None - the base date to search from
    :param comments: bool - True if you want to extract the comments, False for submissions
    """

    # To put the timestamp in the filename
    timestamp = date_utils.get_current_date(False)

    # API
    api = PushshiftAPI()

    ###

    file_str = "comments" if comments else "posts"

    # Count of correct docs saved
    ok_docs = 0

    # Measure elapsed time
    start = time.time()

    logger.debug("Starting generation of '{}' ({}) subreddit historic...".format(subreddit, file_str))

    if subreddit is not None:
        start_date = start_date if start_date is not None else timestamp
        if comments:
            response = api.search_comments(q="", subreddit=subreddit, sort_type="created_utc", sort="desc",
                                          before=start_date)
        else:
            response = api.search_submissions(q="", subreddit=subreddit, sort_type="created_utc", sort="desc",
                                          before=start_date)

        try:
            with open(os.path.join("./backups/", "r_{}_{}_{}_base.jsonl".format(subreddit, file_str, start_date)), "a") as outfile:
                for resp in response:
                    post = convert_response(resp, False, comments)
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
                             ("r_{}_base.jsonl".format(subreddit, subreddit)))
    else:
        logger_err.error("Errored subreddit format: {}".format(subreddit))


def extract_posts_for_interval(start_date: int, end_date: int, size: int, timestamp: int,
                               exclude: Optional[list] = None):
    """
    Function that extracts N (size) posts in a given time interval

    :param start_date: str - the date to search from
    :param end_date: str - the date to search to
    :param size: int - maximum number of posts to be retrieved
    :param timestamp: int - timestamp for the filename
    :param exclude: list[str]/None - the subreddits to skip
    :return dict - elapsed time performing the query and number of successfully saved documents
    """

    to_skip = exclude if exclude is not None else []

    # API
    api = PushshiftAPI()

    #####

    # Count of correct docs saved
    ok_docs = 0

    # Measure elapsed time
    start = time.time()

    logger.debug("Interval: [{} - {}]".format(date_utils.convert_to_iso_date_str(start_date),
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
                        if post["subreddit"] not in to_skip:
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


def obtain_reference_collection(path: str, max_block_size: int, posts_per_block: int, base_date: int,
                                exclude: Optional[list] = None, posts: Optional[Iterable] = None):
    """
    Function that given the path of the backups file and the size of the posts' intervals creates a reference
    collection of random posts

    # 1 January 2020 00:00:00 (GMT +00:00) -> 1577836800

    :param path: str - the path to the backups file
    :param max_block_size: int - limit of posts per date interval
    :param posts_per_block: int - number of posts to obtain per interval
    :param base_date: int - the limit timestamp (posts must be older that this)
    :param exclude: list[str]/None - the subreddits to skip
    :param posts: Iterable - a generator from ElasticSearch (omits file specified in 'path' if so) /
    None -> defaults to textIO from file in the path specified as parameter
    """

    # To put the timestamp in the filename
    timestamp = date_utils.get_current_date(False)

    logger.debug("Starting generation of the reference collection...")

    resp = {}
    if posts is not None:
        logger.debug("Data coming from ES loaded...")
        resp = generate_blocks(posts, True, max_block_size, posts_per_block, base_date, timestamp, exclude)
    else:
        try:
            with open(path, "r") as readfile:
                resp = generate_blocks(readfile, False, max_block_size, posts_per_block, base_date, timestamp,
                                       exclude)
        except (OSError, IOError):
            logger_err.error("Read/Write error has occurred")

    if resp:
        end_date = resp["end_date"]
        if resp["current_block_size"] > 0 and resp["start_date"] < base_date:
            # Write remaining
            end_date = resp["last_post"]["created_utc"]
            second_resp = extract_posts_for_interval(resp["start_date"], end_date, resp["current_block_size"],
                                                     timestamp, exclude)
            # Join remaining
            merged = file_manager.merge_backups("ref_col_{}_{}.jsonl".format(max_block_size, timestamp),
                                                "ref_col_{}_{}.jsonl".format(resp["current_block_size"],
                                                                             timestamp))
            # Delete once completed
            if merged:
                file_manager.remove_file("./backups/ref_col_{}_{}.jsonl".format(resp["current_block_size"],
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
                    timestamp: int, exclude: Optional[list] = None):
    """
    Function that given an Iterable containing the posts, the interval between posts, the posts to generate within that
    interval, the base date (posts must be older than this date) and a timestamp for the filename returns a dictionary
    containing the information about the operation performed

    :param posts: Iterable - posts to obtain the intervals from
    :param es: bool - True if the posts are coming from ElasticSearch, False otherwise
    :param max_block_size: int - the posts to skip to find a new date
    :param posts_per_block: int - the amount of posts to be generated within the interval
    :param base_date: int - the limit timestamp (posts must be older that this)
    :param timestamp: int - the timestamp for the filename
    :param exclude: list[str]/None - the subreddits to skip
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
                current_block_size += 1

        else:
            current_block_size += 1

            if current_block_size == max_block_size:
                if es:
                    temp = line["_source"]
                else:
                    temp = json.loads(line)
                end_date = temp["created_utc"]

                # Write interval of random posts to file
                resp = extract_posts_for_interval(start_date, end_date, posts_per_block, timestamp, exclude)
                total_time += resp["elapsed_time"]
                ok_docs += resp["ok_docs"]

                logger.debug("Total number of documents collected: {}".format(ok_docs))

                # Reset
                current_block_size = 0
                start_date = end_date

    result = {"current_block_size": current_block_size, "ok_docs": ok_docs, "skipped": skipped,
              "total_time": total_time, "start_date": start_date, "end_date": end_date,
              "initial_date": initial_date, "last_post": last_post
              }

    return result


def search_author_posts(username: str, save_path: str, before_date: int, exclude: Optional[list] = None):
    """
    Given an author searches all its posts in all subreddits (skipping, if necessary, the subreddits passed as
    parameter) and writes them to a .jsonl file

    :param username: str - the author's username
    :param save_path: str - the path to save the posts of the selected author
    :param before_date: int - the epoch to starting searching from
    :param exclude: list[str]/None - the subreddits to skip
    :return num_post: int - total number of posts found for the author
    """

    to_skip = exclude if exclude is not None else []
    num_posts = 0

    api = PushshiftAPI()
    response = api.search_submissions(author=username,
                                      sort_type="created_utc",
                                      sort="desc",
                                      before=before_date)

    try:
        with open(save_path, "a") as outfile:
            for resp_post in response:
                post = convert_response(resp_post, False)
                test = json.dumps(post)

                if bool(post) and test.startswith('{"id":'):
                    try:
                        if post["subreddit"] not in to_skip:
                            try:
                                # File backup
                                json.dump(post, outfile)
                                outfile.write("\n")
                                num_posts += 1
                            except UnicodeEncodeError:
                                logger_err.error("Encoding error has occurred")
                    except KeyError:
                        logger_err.error("Missing subreddit key and skipping post")
                        continue
    except (OSError, IOError):
        logger_err.error("Read/Write error has occurred")

    return num_posts


def extract_authors_posts(path: str, save_path: str, before_date: int, log: bool, exclude: Optional[list] = None):
    """
    Given a path to file containing the data of the authors (.jsonl) creates a file containing all the posts made by
    that users (a list of subreddits to skip can be also provided so that posts in that subreddits will be skipped)

    :param path: str - the path to the file containing the data of the authors
    :param save_path: str - the path to the file to save all the posts of each author
    :param before_date: int - the epoch to starting searching from
    :param log: bool - activates/deactivates logging of authors
    :param exclude: list[str]/None - the subreddits to skip
    """

    logger.debug("Extracting author posts...")

    # Measure elapsed time
    start = time.time()

    # Total posts found
    total_posts = 0

    file_manager.clear_file(save_path)
    try:
        with open(path) as input_file:
            total_authors = file_manager.count_lines_file(path)
            for i, a in enumerate(input_file, 1):
                author_data = json.loads(a)
                posts_found = search_author_posts(author_data["username"], save_path, before_date, exclude)
                total_posts += posts_found
                if log:
                    logger.debug("{}/{} - ({}: {} posts)".format(i, total_authors, author_data["username"],
                                                                 posts_found))
    except (OSError, IOError):
        logger_err.error("Read/Write error has occurred")

    # Sort file by created_utc (oldest to newest)
    file_manager.sort_file(save_path, "created_utc")

    end = time.time()
    elapsed_time = end - start
    logger.debug("Total elapsed time: {} for a total of {} posts found".format(elapsed_time, total_posts))


def count_author_posts(username: str, before_date: int, exclude: list):
    """
    Function that given an author name, the date to start searching from and the list of subreddits to skip, returns the
    total post count

    :param username: str - the author's name
    :param before_date: int - the epoch to starting searching from
    :param exclude: list[str] - list of subreddit to skip
    :return: int - the total count of posts
    """

    api = PushshiftAPI()
    response = api.search_submissions(author=username, before=before_date)

    count = 0
    for hit in response:
        post = convert_response(hit, False)
        test = json.dumps(post)

        if bool(post) and test.startswith('{"id":'):
            try:
                if post["subreddit"] not in exclude:
                    count += 1
            except KeyError:
                logger_err.error("Missing subreddit key and skipping post")
                continue

    return count


def obtain_authors_samples(authors_info_path: str, sample_size: int, subreddit_authors: str, days_diff: int,
                           similarity_karma: float):
    """
    Function that given the path containing the information of the users (preferably ordered by any means, i.e account
    identifier) and the size desired, performs a systematic sampling to randomly extract users (and backups them) and,
    afterwards, generates another collection based on the previous one so that the users are similar in account
    creation time (by means of an user defined interval of days) and in comment and link karma punctuations
    (by means of a percentage controlled by the user)

    :param authors_info_path: str - path to the .jsonl file containing the info of the authors
    :param sample_size: int - the size of the sample to be generated
    :param subreddit_authors: str - path to the file containing the name of the authors in a certain subreddit and used
    to skip them (.txt)
    :param days_diff: int - interval of difference in days between accounts creation
    [base - days_diff, base, base + days_diff]
    :param similarity_karma: float - (0-1.0] Percentage of deviation of comment and karma punctuations between the users
    provided and the users to be found
    """

    import questioner

    tools.systematic_authors_sample(authors_info_path, sample_size)
    questioner.generate_reference_authors("./data/subr_authors_selected.jsonl", subreddit_authors, days_diff,
                                          similarity_karma)


def generate_subreddit_datasets(subreddit: str, before_date: int, max_block_size: int, posts_per_block: int):
    """
    Given a subreddit name, the date to search from and the granularity of blocks, generates two datasets, a historic
    of the subreddit and a reference collection with similar posts in the following way: extracts intervals of posts
    of max_block_size and generates the amount of posts passed as parameter (posts_per_block) between the dates of the
    first and last posts of the interval (NOTE: by default omits posts of the same subreddit as the historic)

    Recommended sizes: max_block_size and posts_per_block -> 100

    :param subreddit: str - the subreddit name
    :param before_date: int - the date to search from
    :param max_block_size: int - size of the interval in the historic
    :param posts_per_block: int - amount of posts to find in the interval
    """

    logger.debug("Starting generation of datasets...")

    extract_historic_for_subreddit(subreddit, before_date)
    obtain_reference_collection("./backups/r_depression_base.jsonl", max_block_size, posts_per_block, before_date,
                                tools.list_excluded_subreddits("./data/dep_subreddits.txt", [subreddit]), None)
    logger.debug("Datasets generated")


def generate_authors_samples(subreddit: str, sample_size: int, before_date: int, days_diff: int,
                             similarity_karma: float, reddit_authors_path: Optional[str] = None,
                             historic_path: Optional[str] = None):
    """
    Given a subreddit name, the sample size (for the systematic sampling), the difference in days and similarity
    (explained below), the paths (optional) to the historic and to the file containing information about the redditors
    and, if path is not provided, the date to search from in the historic generation

    :param subreddit: str - the subreddit name
    :param sample_size: int - the size of the sample to be generated
    :param before_date: int - the date to search from
    :param days_diff: int - interval of difference in days between accounts creation
    [base - days_diff, base, base + days_diff]
    :param similarity_karma: float - (0-1.0] Percentage of deviation of comment and karma punctuations between the users
    provided and the users to be found
    :param reddit_authors_path: str/None - path to the information about redditors
    :param historic_path: str/None - path to the historic file
    """

    logger.debug("Starting generation of datasets...")

    if historic_path is None:
        # If we don't have the historic, generate it
        extract_historic_for_subreddit(subreddit, before_date)
    else:
        # Extract the usernames from the historic
        tools.obtain_usernames("./backups/r_depression_base.jsonl")
        if reddit_authors_path is not None:
            # If not already, index data about as many redditors as possible
            indexer.es_add_bulk(reddit_authors_path, "reddit_users_info")
        # Then, extract the information about the authors and index it aswell
        questioner.extract_authors_info("./data/subr_authors.txt")
        # Obtain the samples of authors
        obtain_authors_samples("./backups/subr_authors_info_backup.jsonl", sample_size, "./data/subr_authors.txt",
                               days_diff, similarity_karma)
        # Extract posts for both samples
        extract_authors_posts("./data/subr_authors_selected.jsonl", "./backups/subr_author_posts.jsonl", before_date,
                              False, [subreddit])
        extract_authors_posts("./data/ref_authors_selected.jsonl", "./backups/ref_author_posts.jsonl", before_date,
                              False, [subreddit])
    logger.debug("Datasets generated")


# obtain_reference_collection("./backups/r_depression_base.jsonl", 100, 100, 1577836800, ["depression"], None)
#extract_historic_for_subreddit("immigration")
extract_historic_for_subreddit("immigration", True, 1652365845)
# tools.systematic_authors_sample("./backups/subr_authors_info_backup.jsonl", 12000)
# obtain_authors_samples("./backups/subr_authors_info_backup.jsonl", 10000, "./data/subr_authors.txt", 30, 0.10)
# extract_authors_posts("./data/subr_authors_selected.jsonl", "./backups/subr_author_posts.jsonl", 1577836800, False,
#                       ["depression"])
# extract_authors_posts("./data/ref_authors_selected.jsonl", "./backups/ref_author_posts.jsonl", 1577836800, False,
#                       ["depression"])
# print(count_author_posts("phoenixfireball", 1577836800, ["depression"]))