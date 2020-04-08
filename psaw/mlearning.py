from sklearn.neighbors import KNeighborsClassifier
from sklearn.feature_extraction.text import CountVectorizer
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import re
import pandas as pd

import date_utils
import file_manager
import questioner

stop_words = stopwords.words('english')
stop_words_string = ""
for word in stop_words:
    stop_words_string += word + "|"
stop_words_string = stop_words_string[:-1]  # Last '|'


def tokenize_text(text):
    """
    Given a text, returns a list with all the tokens identified

    :param text: str - the text to be processed
    :return: the list of tokens
    """
    return nltk.word_tokenize(text)


def strip_non_alpha_num(text):
    """
    Given a text string, remove all non-alphanumeric characters (using Unicode definition of alphanumeric)

    :param text: str - the text to be processed
    :return: the formatted text
    """
    return re.compile(r'\W+', re.UNICODE).split(text)


def remove_stop_words(word_list, stp_words):
    """
    Given a list of words, remove any that are in a list of stop words.

    :param word_list: list - list of words to be processed
    :param stp_words: list - the list of words to remove
    :return: the list of of words omitting the ones in the stopwords list
    """
    return [w for w in word_list if w not in stp_words]


def append_to_data_frame(post):
    pp1, pp2, pp3, count_depression_keywords, count_swearing = 0, 0, 0, 0, 0

    hour = date_utils.extract_field_from_timestamp(post["created_utc"], "hour")
    month = date_utils.extract_field_from_timestamp(post["created_utc"], "month")

    return {"pp1": pp1, "pp2": pp2, "pp3": pp3, "count_depression_keywords": count_depression_keywords,
            "count_swearing": count_swearing, "hour": hour, "month": month
            }


df_obj = pd.DataFrame(columns=["pp1", "pp2", "pp3", "count_depression_keywords", "count_swearing", "hour", "month"])


def obtain_most_common_terms(size: int, stp_words: str):
    def obtain_most_common_in(field: str, s: int):
        query = {
            "size": 0,
            "aggs": {
                "Most common terms": {
                    "terms": {
                        "field": field,
                        "size": s,
                        "exclude": stp_words
                    }
                }
            }
        }
        response = questioner.perform_search("r_depression", "localhost", "9200", query,
                                             "Most common terms in {}".format(field))
        for hit in response["aggregations"]["Most common terms"]["buckets"]:
            to_write = {hit["key"]: hit["doc_count"]}
            file_manager.write_to_file(to_write, "./data", "most_common_terms_{}.jsonl".format(field), "a")

    fields = ["selftext", "title"]
    for f in fields:
        obtain_most_common_in(f, size)


obtain_most_common_terms(5000, stop_words_string)

# Load dataset files

# # Depression (train)
# train_data_depression = pd.DataFrame(pd.read_json("backups/r_depression_1000.jsonl", lines=True))
# train_data_depression["depression_related"] = [1] * len(train_data_depression.index)  # Dep. identifier: true
# # Non-depression (train)
# train_data_control = pd.DataFrame(pd.read_json("backups/control_1000.jsonl", lines=True))
# train_data_control["depression_related"] = [0] * len(train_data_control.index)  # Dep. identifier: false
# # Join both data-frames
# train = train_data_depression.append(train_data_control, ignore_index=True)
# print(train.shape)
#
# # Testing
# test = pd.DataFrame(pd.read_json("backups/testing.jsonl", lines=True))
# print(test.shape)
#
# # Only required columns for training
# cols = ["title", "selftext", "created_utc", "num_comments"]
# # Training parameters
# X = train.loc[:, cols]
# print(X.shape)
# y = train.depression_related
# print(y.shape)
# # Testing parameters
# X_test = test.loc[:, cols]
#
# # Nearest neighbours
# knn = KNeighborsClassifier(n_neighbors=1)
# knn.fit(X, y)
#
# # Predict
# new_pred_class = knn.predict(X_test)
# depression_data = pd.DataFrame({'id': test.id, 'depression_related': new_pred_class}).set_index('id')
# depression_data.to_csv('predicted_depression.csv')
