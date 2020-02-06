import json
import elastic


def load_json(path):
    """ Function to load the json used as backup

        Parameters:
            path -- path to the json file
    """

    try:
        with open(path, "r") as open_file:
            yield json.load(open_file)
    except Exception as err:
        print(err)


elastic.index_data(load_json("./backups/all_queries_backup.json"), "depression_index", "reddit_doc")
