import logging
import json
import os
#####
import logging_factory
#####
from typing import Optional
#####
logger_err = logging_factory.get_module_logger("tools_err", logging.ERROR)
logger = logging_factory.get_module_logger("tools", logging.DEBUG)


def obtain_usernames(subr_path: str):
    """
    Given the path of the backup, generates one .txt file containing the authors in the backup

    :param subr_path: str - path to the file (i.e subreddit file)
    """

    subr_authors = set()

    try:
        with open(subr_path, "r") as input_file:
            for line in input_file:
                try:
                    loaded = json.loads(line)
                    author = loaded["author"]
                    if author != "[deleted]":
                        subr_authors.add(author)
                except KeyError:
                    logger_err.error("Error in author key with post with ID: {}".format(loaded["id"]))
    except (OSError, IOError):
        logger_err.error("Read/Write error has occurred")

    try:
        with open("./data/subr_authors.txt", "w") as output:
            subr_authors_list = list(subr_authors)
            for a in subr_authors_list:
                output.write(a + "\n")
    except (OSError, IOError):
        logger_err.error("Read/Write error has occurred")


def list_excluded_subreddits(path: str, additional: Optional[list] = None):
    """
    Given a path to a file containing names of subreddits to be excluded and any other additional (and optional)
    subreddits to be skipped returns a list containing all of them

    :param path: str - path to the file (.txt - one name per line) with the subreddits to be skipped
    :param additional: list - any other subreddits to be skipped (convenient for combining with other methods in the
    project)
    :return: list - the list with all the subreddits to be skipped
    """

    subreddits = []
    try:
        if os.path.isfile(path):
            with open(path, "r") as output:
                for subreddit in output:
                    subreddits.append(subreddit.strip("\n"))
    except (OSError, IOError):
        logger_err.error("Read/Write error has occurred")

    if additional is not None:
        for s in additional:
            if s not in subreddits:
                subreddits.append(additional)

    return subreddits


def systematic_authors_sample(authors_info_path: str, sample_size: int):
    """
    Given the path to a .jsonl file containing the info of the authors, generates another .jsonl file and an excel file
    containing the authors selected using systematic sampling (of the size given as parameter)

    :param authors_info_path: str - path to the .jsonl file containing the info of the authors
    :param sample_size: int - the size of the sample to be generated
    """

    import random
    import math
    import pandas as pd

    logger.debug("Starting systematic sampling generation...")

    authors = []
    # Load all the collection of users' data
    try:
        with open(authors_info_path, "r") as input_file:
            for auth in input_file:
                authors.append(json.loads(auth))
    except (OSError, IOError):
        logger_err.error("Read/Write error has occurred")
    logger.debug("Total amount of authors in file: {}".format(len(authors)))

    selected = []
    # Systematic sampling
    k = len(authors) / sample_size
    starting_point = random.random() * k
    while starting_point <= len(authors):
        index = math.ceil(starting_point) - 1
        selected.append(authors[index])
        starting_point += k

    # Backup list to .jsonl and .xlsx
    try:
        with open("./data/subr_authors_selected.jsonl", "w+") as output:
            for auth in selected:
                output.write(json.dumps(auth))
                output.write("\n")
    except (OSError, IOError):
        logger_err.error("Read/Write error has occurred")

    df = pd.DataFrame(selected)
    df.to_excel("./data/subr_authors_selected.xlsx")

    logger.debug("Sample generated")


def link_comments_and_submissions(submissions_path: str, comments_path: str, merge_path: str, remove_op: bool = False):
    """
    Given the paths to the submissions and comments files, generates a .jsonl file containing the links between them

    :param submissions_path: str - path to the submissions file
    :param comments_path: str - path to the comments file
    :param merge_path: str - path to the file to be generated
    :param remove_op: bool - whether to remove OP comments
    """

    merge_path = merge_path if not remove_op else merge_path.replace(".jsonl", "_no_op.jsonl")
    with open(merge_path, "a") as outfile:
        with open(submissions_path, "r") as submissions_file:
            for submission in submissions_file:
                submission = json.loads(submission)
                submission_id = submission["id"]
                op = submission["author"]
                comments = get_all_comments_for_id(submission_id, comments_path, remove_op, op)
                submission["comments"] = comments
                if submission["num_comments"] != len(comments):
                    logger.debug(f"Submission {submission_id} has{submission['num_comments']} comments, but {len(comments)} comments were found")
                json.dump(submission, outfile)


def get_all_comments_for_id(submissions_id: str, comments_path: str, remove_op: bool = False, op: Optional[str] = None):
    """
    Given the ID of a submission, returns all the comments related to it

    :param submissions_id: str - ID of the submission
    :return: list - list of comments
    """

    comments = []
    with open(comments_path, "r") as comments_file:
        for comment in comments_file:
            comment = json.loads(comment)
            comment_id = comment["link_id"]
            if comment_id == submissions_id:
                if remove_op and comment["author"] == op:
                    continue
                else:
                    comments.append(comment)

    return comments
