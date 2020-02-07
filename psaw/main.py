import codecs
import json
import time
import traceback
import excel_reader
import os
import elastic
from psaw import PushshiftAPI
from tqdm import tqdm
from datetime import datetime


def convert_response(gen):
    """ Function to convert the response into a list with all the required data

        Parameters:
            gen -- generator/iterator
                the generator containing the data of the query
        Returns:
            result -- list
                a list of dictionaries with only the necessary data
    """

    _list = list(gen)
    result = []

    for s in _list:
        dict_to_add = {"title": s.d_["title"],
                       "author": s.d_["author"],
                       "subreddit": s.d_["subreddit"],
                       "created_utc": s.d_["created_utc"],
                       "selftext": s.d_["selftext"]}

        result.append(dict_to_add)

    return result


# List to store all queries with its _id's
all_queries = []
_id = 1

# Time format for UTC
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# Parameters
queries_and_scales = excel_reader.get_queries_and_scales("./excel/scales.xlsm")
thematic = True
_index = "depression_index"
_type = "reddit_doc"

# API
api = PushshiftAPI()

#####

# Used to show the progress of completion when making the requests
total_queries = sum([len(x) for x in queries_and_scales.values()])

with tqdm(desc="Queries performed", total=total_queries, position=0, leave=True) as pbar:
    for scale in queries_and_scales:
        for query in queries_and_scales[scale]:

            # To store failed queries and directory errors
            errors = []

            response = api.search_submissions(q=query, limit=5)
            submissions_list = convert_response(response)

            if len(submissions_list) > 0:

                # Posts before the most recent post obtained
                most_recent_utc = submissions_list[0]["created_utc"]
                # TODO: if we omit "limit" a full historical search will be performed (time compromise?)
                response = api.search_submissions(q=query, before=most_recent_utc, limit=5)
                submissions_list = convert_response(response)

                # Extra fields: current timestamp and query data
                for sub in submissions_list:
                    timestamp = datetime.utcfromtimestamp(int(time.time())).strftime(TIME_FORMAT)
                    # +0001 for Spain GMT
                    date = datetime.strptime(timestamp + "+0001", TIME_FORMAT + "%z")
                    timestamp = date.timestamp()
                    sub["parameters"] = {"query": query, "scale": scale, "thematic": thematic}
                    sub["timestamp"] = timestamp

                #####

                # Index with ElasticSearch posts' list
                elastic.index_data(submissions_list, _index, _type)

                # Add to global queries list
                all_queries.append(submissions_list)
                _id += 1

                #####

                # File backup
                try:
                    save_path = "./backups/"
                    # Create, if not present, folder to store posts" backups
                    if not os.path.isdir(save_path):
                        os.mkdir(save_path)

                    query_name_file = query.replace(" ", "-")
                    filename = "%s_%s_backup.json" % (query_name_file, scale)

                    # One directory per scale
                    directory = save_path + scale + "/"
                    os.mkdir(directory)
                except FileExistsError:
                    # Add error to print it later
                    errors.append("Directory '%s' already exists" % directory)
                    pass

                save_path = directory

                # Write .json file backup
                try:
                    with codecs.open(save_path + filename, "w", encoding="utf8") as outfile:
                        json.dump(submissions_list, outfile, indent=4)
                except UnicodeEncodeError:
                    print(traceback.format_exc())
            else:
                # Add error to print it later
                errors.append("No results found for query: '%s'" % query)

            pbar.update()

# Show errors (if present)
if len(errors) > 0:
    for e in errors:
        print(e)

#####

# File backup (all queries in a single file)
save_path = "./backups/"
filename = "all_queries_backup.json"

# Write .json file backup
try:
    with codecs.open(save_path + filename, "w", encoding="utf8") as outfile:
        json.dump(all_queries, outfile, indent=4)
except UnicodeEncodeError:
    print(traceback.format_exc())

#####
