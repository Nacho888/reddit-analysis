import json
import logging
#####
import logging_factory
import indexer
#####
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
#####
logger_err = logging_factory.get_module_logger("questioner_err", logging.ERROR)
logger = logging_factory.get_module_logger("questioner", logging.DEBUG)


def extract_authors_info(authors_path: str):
    """
    Given a .txt file containing the names of the authors, searches in an ElasticSearch index their corresponding
    information (for reddit: account identifier, username, date of creation, date of retrieval, comment and
    link karma punctuation). Generates a .jsonl file containing all the authors info sorted by their account id.

    :param authors_path: str - path to the .txt file containing the authors
    """

    import math

    es = Elasticsearch(hosts=[{"host": "localhost", "port": 9200}])
    search = Search(using=es, index="reddit_users")
    max_query_size = 50000

    authors = []
    result = []
    # Extract the author names
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
            search = search.filter("terms", username=chunk)
            for hit in search.scan():
                result.append({"acc_id": hit.acc_id,
                               "username": hit.username,
                               "created": hit.created,
                               "updated": hit.updated,
                               "comment_karma": hit.comment_karma,
                               "link_karma": hit.link_karma
                               })
            logger.debug("Chunk {}/{} processed".format(processed, n_chunks))
            processed += 1
    logger.debug("Information successfully found of {} authors".format(len(result)))

    result = sorted(result, key=lambda k: int(k["acc_id"]))

    # Save data to file
    with open("./data/authors_info_backup.jsonl", "w") as output:
        for line in result:
            output.write(json.dumps(line))
            output.write("\n")

    # Save to data to ElasticSearch
    indexer.es_add_bulk("./data/authors_info_backup.jsonl", "r_depression_authors")
    logger.debug("Data successfully indexed")


def generate_reference_authors(authors_info: str, subreddit_authors: str, months_diff: int, similarity_karma: float):
    """
    Function that given a file containing data about the author selected via systematic sampling and a file containing
    the names of the authors to omit, generates two files (.jsonl and .xlsx) with authors that are similar in account
    creation time (by means of an user defined interval of months) and in comment and link karma punctuations
    (by means of a percentage controlled by the user)

    :param authors_info: str - path to the file containing the data of the authors selected (.jsonl format)
    :param subreddit_authors: str - path to the file containing the name of the authors in a certain subreddit and used
    to skip them (.txt)
    :param months_diff: int - interval of difference in months between accounts creation
    [base - months_diff, base, base + months_diff]
    :param similarity_karma: float - (0-1.0] Percentage of deviation of comment and karma punctuations between the
    users provided and the users to be found
    """

    import date_utils as d
    import pandas as pd

    logger.debug("Starting reference authors generation...")

    es = Elasticsearch(hosts=[{"host": "localhost", "port": 9200}])
    # Indices with the information of all users and with only r/depression users
    s_all = Search(using=es, index="reddit_users_info")
    s_dep = Search(using=es, index="r_depression_users_info")

    # Load selected users
    authors_selected = []
    with open(authors_info, "r") as input_file:
        for author_info in input_file:
            authors_selected.append(author_info)

    # Load all usernames of the authors of the subreddit
    dep_authors = set()
    with open(subreddit_authors, "r") as input_file:
        for author in input_file:
            dep_authors.add(author.replace("\n", ""))

    result = []
    for i, author in enumerate(authors_selected):
        # Extract user info based on its account id (unique)
        response = s_dep.query("match", acc_id=json.loads(author)["acc_id"]).execute()

        if len(response.hits) > 0:
            found = response.hits[0]
            # Extract the information
            created, comment, link = int(found.created), int(found.comment_karma), int(found.link_karma)

            # And create the ranges
            ranges = [[d.substract_month_from_epoch(created, months_diff), d.add_month_to_epoch(created, months_diff)],
                      [comment - comment * similarity_karma, comment + comment * similarity_karma],
                      [link - link * similarity_karma, link + link * similarity_karma]]

            # Define queries for each field to be contained in the given intervals
            q = Q("range", created={"gte": ranges[0][0], "lte": ranges[0][1]}) & \
                Q("range", comment_karma={"gte": ranges[1][0], "lte": ranges[1][1]}) & \
                Q("range", link_karma={"gte": ranges[2][0], "lte": ranges[2][1]})

            # Query the "all" users index to find potential similar users
            response2 = s_all.filter(q)

            for hit in response2:
                # Make sure that our user is not present in the list of all users who have ever published in the
                # subreddit (i.e r/depression)
                if hit.username not in dep_authors:
                    result.append({"acc_id": hit.acc_id,
                                   "username": hit.username,
                                   "created": hit.created,
                                   "updated": hit.updated,
                                   "comment_karma": hit.comment_karma,
                                   "link_karma": hit.link_karma
                                   })
                    break  # We only want the first user found

    logger.debug("Total amount of authors found: {}".format(len(result)))

    # Backup list to .jsonl and .xlsx
    with open("./data/ref_authors_selected.jsonl", "w") as output:
        for line in result:
            output.write(json.dumps(line))
            output.write("\n")

    df = pd.DataFrame(result)
    df.to_excel("./data/ref_authors_selected.xlsx")


# extract_authors_info("./data/subr_authors.txt")
# generate_reference_authors("./data/subr_authors_selected.jsonl", "./data/subr_authors.txt", 6, 0.25)
