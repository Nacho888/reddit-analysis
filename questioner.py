import json
import logging
#####
import logging_factory
#####
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
#####
logger_err = logging_factory.get_module_logger("questioner_err", logging.ERROR)
logger = logging_factory.get_module_logger("questioner", logging.DEBUG)


def extract_authors_info(authors_path: str):
    es = Elasticsearch(hosts=[{"host": "localhost", "port": 9200}])

    authors = []
    result = []
    with open(authors_path, "r") as input_file:
        for author in input_file:
            authors.append(author.replace("\n", ""))
        print("Authors loaded ({})".format(len(authors)))
        n_chunks = 10
        processed = 1
        for chunk in [authors[round(len(authors) / n_chunks * i):round(len(authors) / n_chunks * (i + 1))] for i in
                      range(n_chunks)]:
            s = Search(using=es, index="reddit_users")
            s = s.filter("terms", username=chunk)
            for hit in s.scan():
                result.append({"acc_id": hit.acc_id,
                               "username": hit.username,
                               "created": hit.created,
                               "updated": hit.updated,
                               "comment_karma": hit.comment_karma,
                               "link_karma": hit.link_karma})
            print("Chunk {}/{} processed".format(processed, n_chunks))
            processed += 1
    print("Information successfully found of {} authors".format(len(result)))

    result = sorted(result, key=lambda k: int(k["acc_id"]))

    with open("./data/authors_info_backup.jsonl", "w") as output:
        for line in result:
            output.write(json.dumps(line))
            output.write("\n")


extract_authors_info("./data/subr_authors.txt")
