import json
import logging
import pandas as pd
#####
import logging_factory
import indexer
#####
from elasticsearch import Elasticsearch, ConnectionTimeout, TransportError, ConnectionError
from elasticsearch_dsl import Search, Q
#####
logger_err = logging_factory.get_module_logger("questioner_err", logging.ERROR)
logger = logging_factory.get_module_logger("questioner", logging.DEBUG)


def extract_authors_info(authors_path: str):
    """
    Given a .txt file containing the names of the authors, searches in an Elasticsearch index their corresponding
    information (for reddit: account identifier, username, date of creation, date of retrieval, comment and
    link karma punctuation). Generates a .jsonl file containing all the authors info sorted by their account id.

    :param authors_path: str - path to the .txt file containing the authors
    """

    import math

    host, port = "localhost", 9200
    es = Elasticsearch(hosts=[{"host": host, "port": port}])
    search = Search(using=es, index="reddit_users")
    max_query_size = 50000

    authors = []
    result = []
    # Extract the author names
    try:
        with open(authors_path, "r") as input_file:
            for author in input_file:
                authors.append(author.replace("\n", ""))
            logger.debug("Authors loaded ({})".format(len(authors)))
            n_chunks = math.ceil(len(authors) / max_query_size)

            processed = 1
            # Divide the list of author names in chunks of the maximum size allowed in order to speed up the search
            for chunk in [authors[round(len(authors) / n_chunks * i):round(len(authors) / n_chunks * (i + 1))] for i in
                          range(n_chunks)]:
                # Query with all the chunk at the same time
                try:
                    search = search.filter("terms", username=chunk)
                    for hit in search.scan():
                        result.append({"acc_id": hit.acc_id,
                                       "username": hit.username,
                                       "created": hit.created,
                                       "updated": hit.updated,
                                       "comment_karma": hit.comment_karma,
                                       "link_karma": hit.link_karma
                                       })
                except (ConnectionError, ConnectionTimeout):
                    logger_err.error("Error communicating with Elasticsearch - host: {}, port: {}".format(host, port))
                except TransportError:
                    logger_err.error("Errored Elasticsearch query: 'filter'")

                logger.debug("Chunk {}/{} processed".format(processed, n_chunks))
                processed += 1
    except (OSError, IOError):
        logger_err.error("Read/Write error has occurred")
    logger.debug("Information successfully found of {} authors".format(len(result)))

    result = sorted(result, key=lambda k: int(k["acc_id"]))

    # Save data to file
    try:
        with open("./data/subr_authors_info_backup.jsonl", "w") as output:
            for line in result:
                output.write(json.dumps(line))
                output.write("\n")
    except (OSError, IOError):
        logger_err.error("Read/Write error has occurred")

    # Save to data to Elasticsearch
    indexer.es_add_bulk("./data/subr_authors_info_backup.jsonl", "r_depression_authors_info")
    logger.debug("Data successfully indexed")


def clean_sample(not_found: list, authors_info: str):
    """
        Given a list of users that for whom a pair was not found and the path to the original sample,
        overwrites the sample with the users not appearing in that list.

        :param not_found: list[str] - the usernames of the users to remove
        :param authors_info: str - path to the original sampled file containing all the information about
        the authors
    """
    authors = []
    try:
        with open(authors_info, "r") as file:
            for author in file:
                authors.append(json.loads(author))
        with open(authors_info, "w") as output:
            for author in authors:
                if author["username"] not in not_found:
                    output.write(json.dumps(author))
                    output.write("\n")
    except (OSError, IOError):
        logger_err.error("Read/Write error has occurred")

    df = pd.DataFrame(authors)
    df.to_excel("./data/subr_authors_selected.xlsx")


def generate_reference_authors(authors_info: str, subreddit_authors: str, days_diff: int, similarity_karma: float):
    """
    Function that given a file containing data about the author selected via systematic sampling and a file containing
    the names of the authors to omit, generates two files (.jsonl and .xlsx) with authors that are similar in account
    creation time (by means of an user defined interval of days) and in comment and link karma punctuations
    (by means of a percentage controlled by the user)

    :param authors_info: str - path to the file containing the data of the authors selected (.jsonl format)
    :param subreddit_authors: str - path to the file containing the name of the authors in a certain subreddit and used
    to skip them (.txt)
    :param days_diff: int - interval of difference in days between accounts creation
    [base - days, base, base + days]
    :param similarity_karma: float - (0-1.0] Percentage of deviation of comment and karma punctuations between the users
    provided and the users to be found
    """

    import date_utils as d

    logger.debug("Starting reference authors generation...")

    # List of users with no pair found for this configuration
    not_found = []

    # Usernames already found
    usernames_found = set()

    host, port = "localhost", 9200
    es = Elasticsearch(hosts=[{"host": host, "port": port}])
    # Indices with the information of all users and with only r/depression users
    s_all = Search(using=es, index="reddit_users_info")
    s_dep = Search(using=es, index="r_depression_users_info")

    # Load selected users
    authors_selected = []
    try:
        with open(authors_info, "r") as input_file:
            for author_info in input_file:
                authors_selected.append(author_info)
    except (OSError, IOError):
        logger_err.error("Read/Write error has occurred")

    # Load all usernames of the authors of the subreddit
    dep_authors = set()
    try:
        with open(subreddit_authors, "r") as input_file:
            for author in input_file:
                dep_authors.add(author.replace("\n", ""))
    except (OSError, IOError):
        logger_err.error("Read/Write error has occurred")

    result = []
    for i, author in enumerate(authors_selected):
        try:
            # Extract user info based on its account id (unique)
            response = s_dep.query("match", acc_id=json.loads(author)["acc_id"]).execute()

            if len(response.hits) > 0:
                found = response.hits[0]
                # Extract the information
                created, comment, link = int(found.created), int(found.comment_karma), int(found.link_karma)

                # And create the ranges
                ranges = [[d.substract_days_from_epoch(created, days_diff), d.add_days_to_epoch(created, days_diff)],
                          [comment - comment * similarity_karma, comment + comment * similarity_karma],
                          [link - link * similarity_karma, link + link * similarity_karma]]

                # Define queries for each field to be contained in the given intervals
                q = Q("range", created={"gte": ranges[0][0], "lte": ranges[0][1]}) & \
                    Q("range", comment_karma={"gte": ranges[1][0], "lte": ranges[1][1]}) & \
                    Q("range", link_karma={"gte": ranges[2][0], "lte": ranges[2][1]})

                try:
                    # Query the "all" users index to find potential similar users
                    response2 = s_all.filter(q)

                    is_found = False
                    for hit in response2:
                        # Make sure that our user is not present in the list of all users who have ever published in the
                        # subreddit (i.e r/depression), is not the same we are using to find the pair and is not already
                        # in the list of users found (and that complies with the interval of posts)
                        if hit.username not in dep_authors and hit.username not in usernames_found \
                                and found.username is not hit.username and hit.username != "[deleted]":
                            is_found = True
                            usernames_found.add(hit.username)

                            result.append({"acc_id": hit.acc_id,
                                           "username": hit.username,
                                           "created": hit.created,
                                           "updated": hit.updated,
                                           "comment_karma": hit.comment_karma,
                                           "link_karma": hit.link_karma,
                                           })
                            break  # We only want the first user found
                    if is_found is False:
                        not_found.append(found.username)
                except TransportError:
                    logger_err.error("Errored Elasticsearch query: {}".format(str(q)))
        except (ConnectionError, ConnectionTimeout):
            logger_err.error("Error communicating with Elasticsearch - host: {}, port: {}".format(host, port))
        except TransportError:
            logger_err.error("Errored Elasticsearch query: 'match'")

    logger.debug("Total amount of authors found: {}".format(len(result)))

    if len(not_found) > 0:
        logger.debug("Total amount of authors with no pair found: {} (Cleaning...)".format(len(not_found)))
        clean_sample(not_found, authors_info)

    # Backup list to .jsonl and .xlsx
    try:
        with open("./data/ref_authors_selected.jsonl", "w") as output:
            for line in result:
                output.write(json.dumps(line))
                output.write("\n")
    except (OSError, IOError):
        logger_err.error("Read/Write error has occurred")

    df = pd.DataFrame(result)
    df.to_excel("./data/ref_authors_selected.xlsx")

# extract_authors_info("./data/subr_authors.txt")
# generate_reference_authors("./data/subr_authors_selected.jsonl", "./data/subr_authors.txt", 30, 0.10)
