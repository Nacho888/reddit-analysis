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
