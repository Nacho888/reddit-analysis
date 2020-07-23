import numpy as np
import pandas as pd
import array
#####
import date_utils
import excel_reader
import file_manager
#####
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from sklearn.neighbors import KNeighborsClassifier
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from nltk import PorterStemmer
from sklearn.naive_bayes import MultinomialNB
from sklearn import svm
from sklearn import tree
from sklearn.metrics import confusion_matrix, accuracy_score, precision_score
from collections import Counter

#####
VOCABULARY_SIZE = 5000


def get_keywords(docs: list, cv, word_count_vector, feature_names):
    """
    Given the list of the text in all the documents, returns a list containing tuples, each one, related to a
    document and containing its top keywords with its associated score in descending order

    :param docs: list - the list that contains the text associated to each post
    :return: a list of tuples
    """

    # Generate tf-idf for all documents in the list
    tfidf_transformer = TfidfTransformer(smooth_idf=True, use_idf=True)
    tfidf_transformer.fit(word_count_vector)
    tf_idf_vector = tfidf_transformer.transform(cv.transform(docs))

    results = []
    for i in range(tf_idf_vector.shape[0]):
        # Vector for a single document
        curr_vector = tf_idf_vector[i]
        # Sort the tf-idf vector by descending order of scores
        sorted_items = sort_coo(curr_vector.tocoo())
        # Extract only the top n
        top_keywords = extract_top_from_vector(feature_names, sorted_items, 10)

        results.append(top_keywords)

    return results


def sort_coo(coo_matrix):
    """
    Function that given the sparse matrix returns a list of ordered tuples in descending order

    :param coo_matrix: array - the sparse matrix containing the values
    :return: a list of ordered tuples
    """
    tuples = zip(coo_matrix.col, coo_matrix.data)
    return sorted(tuples, key=lambda x: (x[1], x[0]), reverse=True)


def extract_top_from_vector(ft_names: list, sorted_items: list, top: int = 10):
    """
    Given the list of keywords and the the list of keywords with its scores, extracts the requested top elements

    :param ft_names: list - the keyword"s names
    :param sorted_items: list - the keywords with its corresponding scores
    :param top: int - the number of elements to extract (default 10)
    :return: and ordered tuple with the keywords and its scores ordered from highest to lowest
    """

    # Obtain the top-n items from vector
    sorted_items = sorted_items[:top]

    score_values = []
    feature_values = []

    for idx, score in sorted_items:
        feature_values.append(ft_names[idx])  # Append the keyword
        score_values.append(round(score, 3))  # Append its corresponding score

    return zip(feature_values, score_values)  # Ordered tuple


# def get_scales_keywords(path: str):
#     """
#
#     :param path:
#     :return:
#     """
#
#     queries = excel_reader.get_queries(path, 5, 4)
#     queries = [pre_process(x) for x in queries]
#     cv = CountVectorizer(stop_words=stopwords.words('english'), max_features=250, min_df=2, max_df=0.85,
#                          ngram_range=(1, 2))
#     word_count_vector = cv.fit_transform(queries)
#     feature_names = cv.get_feature_names()
#     print(feature_names)


def initialize_vocabulary(documents):
    words_list = []
    for doc in documents:
        processed = pre_process(doc).split(" ")
        words_list += processed

    vocabulary = Counter(words_list)
    vocabulary = vocabulary.most_common(VOCABULARY_SIZE)

    vocabulary_list = []
    [vocabulary_list.append(w) for w in vocabulary if w not in stopwords.words("english")]

    # print(vocabulary_list)
    return vocabulary_list


def initialize_text_features(documents, vocabulary):
    total_documents = len(documents)
    # Init matrix
    matrix = np.zeros(total_documents, VOCABULARY_SIZE)

    # Preprocess text and compute tf-idf
    doc_id = 0
    vocabulary_words = [t[0] for t in vocabulary]
    for doc in documents:
        for word in doc.split(" "):
            if word in vocabulary_words:
                word_id = vocabulary_words.index(word)
                tf = doc.count(word)
                df = df_dict[word]
                idf = np.log(total_documents / (df + 1))  # + 1 to avoid division by 0 if term not present
                tf_idf = tf * idf
                matrix[doc_id, word_id] = tf_idf
        doc_id += 1

    return matrix


def compute_df(documents):
    dfs = {}
    for doc in documents:
        for word in set(doc.split(" ")):
            if word in dfs.keys():
                dfs[word] += 1
            else:
                dfs[word] = 1
    return dfs


def populate_datasets(training_size, test_proportion):
    testing_size = int(training_size * test_proportion)
    file_manager.clear_file("datasets/training", "training_depression_{}.jsonl".format(training_size))
    file_manager.clear_file("datasets/training", "training_control_{}.jsonl".format(training_size))
    file_manager.clear_file("datasets/testing",
                            "testing_depression_{}.jsonl".format(testing_size))
    file_manager.clear_file("datasets/testing",
                            "testing_control_{}.jsonl".format(testing_size))
    # Training
    file_manager.populate_dataset("backups/r_depression_full.jsonl", "datasets/training",
                                  "training_depression_{}.jsonl".format(training_size), 0,
                                  training_size)
    file_manager.populate_dataset("backups/ref_col_5000_1585758038.jsonl", "datasets/training",
                                  "training_control_{}.jsonl".format(training_size), 0,
                                  training_size)
    # Testing
    file_manager.populate_dataset("backups/r_depression_full.jsonl", "datasets/testing",
                                  "testing_depression_{}.jsonl".format(testing_size),
                                  training_size,
                                  testing_size)
    file_manager.populate_dataset("backups/ref_col_5000_1585758038.jsonl", "datasets/testing",
                                  "testing_control_{}.jsonl".format(testing_size),
                                  training_size, testing_size)


def evaluate_text(mode, posts):
    if mode == "train":
        # Text processing
        print("\tProcessing training data...")
        try:
            train["selftext"]
        except KeyError:
            train["text"] = train["title"]
        else:
            train["text"] = train["title"] + train["selftext"]
        train["text"] = train["text"].apply(lambda x: pre_process(x))

        # Compute the training matrix
        print("\tExtracting training features...")
        matrix = initialize_text_features(posts, vocabulary)
    else:
        # Text processing
        print("\tProcessing testing data...")
        try:
            test["selftext"]
        except KeyError:
            test["text"] = test["title"]
        else:
            test["text"] = test["title"] + test["selftext"]
        test["text"] = test["text"].apply(lambda x: pre_process(x))

        # Extract text as list and compute tf
        posts = test["text"].tolist()
        df_dict = compute_df(posts)

        # Compute the testing matrix
        print("\tExtracting testing features...")
        matrix = initialize_text_features(posts, vocabulary)

    return matrix


def setup_models(x, y):
    fitted_models = [MultinomialNB().fit(x, y), KNeighborsClassifier(3).fit(x, y), svm.SVC.fit(x, y),
                     tree.DecisionTreeClassifier().fit(x, y)]
    return fitted_models


def write_results_to_excel(spreadsheet_name, data):
    # confusion matrix 00 tp 01 fp 10 fn 11 tn
    to_write = {
        "Type": "",
        "Model": "",
        "Size training": 0,
        "Size testing": 0,
        "TP": [],
        "FP": [],
        "FN": [],
        "TN": []
    }
    df = pd.DataFrame(to_write, ["Type", "Model", "Size training", "Size testing", "TP", "FP", "FN", "TN"])
    df.to_excel(spreadsheet_name)


# # Populate datasets
# print("\tPopulating datasets...")
# train_size = 10000
# proportion = 0.25
# if file_manager.check_dataset_present(train_size):
#     populate_datasets(train_size, proportion)
# test_size = int(10000 * proportion)
#
# ################
# ### Training ###
# ################
#
# # Load dataset files
# print("\tLoading training datasets...")
# # Depression (train)
# train_data_depression = pd.DataFrame(
#     pd.read_json("datasets/training/training_depression_{}.jsonl".format(train_size), lines=True))
# train_data_depression["depression_related"] = [1] * len(train_data_depression.index)  # Dep. identifier: true
# dep_size = len(train_data_depression.index)
#
# # Non-depression (train)
# train_data_control = pd.DataFrame(
#     pd.read_json("datasets/training/training_control_{}.jsonl".format(train_size), lines=True))
# train_data_control["depression_related"] = [0] * len(train_data_control.index)  # Dep. identifier: false
# non_dep_size = len(train_data_control.index)
#
# # Join both data-frames
# train = train_data_depression.append(train_data_control, ignore_index=True)
#
# # Pronouns proportion
# # train["pp1"] = train["text"].apply(lambda x: get_pronoun_proportion(word_tokenize(x), "pp1"))
# # train["pp2"] = train["text"].apply(lambda x: get_pronoun_proportion(word_tokenize(x), "pp2"))
# # train["pp3"] = train["text"].apply(lambda x: get_pronoun_proportion(word_tokenize(x), "pp3"))
# # Post times
# train["hour"] = train["created_utc"].apply(lambda x: date_utils.extract_field_from_timestamp(x, "hour"))
# train["month"] = train["created_utc"].apply(lambda x: date_utils.extract_field_from_timestamp(x, "month"))
#
# # Initialize training label matrix
# total_size = dep_size + non_dep_size
# training_labels = np.zeros(total_size)
# training_labels[0:dep_size] = 1
#
# ### TEXT ###
# # # Extract text as list and create the dictionary
# # print("\tCreating the vocabulary...")
# # posts = train["text"].tolist()
# # global vocabulary
# # vocabulary = initialize_vocabulary(posts)
# #
# # # Compute df
# # df_dict = compute_df(posts)
# #
# # # Compute training matrix
# # training_matrix = evaluate_text("train", posts)
#
# ### HOURS ###
# training_matrix = np.reshape(train["hour"].tolist(), (-1, 1))
# # knn_model = KNeighborsClassifier(n_neighbors=3)
# # knn_model.fit(training_matrix, training_labels)
# svc_model = svm.SVC()
# svc_model.fit(training_matrix, training_labels)
#
# # Train the model
# # print("\tTraining Bayes Model...")
# # bayes_model = MultinomialNB()
# # bayes_model.fit(training_matrix, training_labels)
#
# ###############
# ### Testing ###
# ###############
#
# # Load dataset files
# print("\tLoading testing datasets...")
# # Depression (test)
# test_data_depression = pd.DataFrame(
#     pd.read_json("datasets/testing/testing_depression_{}.jsonl".format(test_size), lines=True))
# test_data_depression["depression_related"] = [1] * len(test_data_depression.index)  # Dep. identifier: true
# dep_size = len(test_data_depression.index)
#
# # Non-depression (test)
# test_data_control = pd.DataFrame(
#     pd.read_json("datasets/testing/testing_control_{}.jsonl".format(test_size), lines=True))
# test_data_control["depression_related"] = [0] * len(test_data_control.index)  # Dep. identifier: false
# non_dep_size = len(test_data_control.index)
#
# # Join both data-frames
# test = test_data_depression.append(test_data_control, ignore_index=True)
#
# # Pronouns proportion
# # test["pp1"] = test["text"].apply(lambda x: get_pronoun_proportion(word_tokenize(x), "pp1"))
# # test["pp2"] = test["text"].apply(lambda x: get_pronoun_proportion(word_tokenize(x), "pp2"))
# # test["pp3"] = test["text"].apply(lambda x: get_pronoun_proportion(word_tokenize(x), "pp3"))
# # Post times
# test["hour"] = test["created_utc"].apply(lambda x: date_utils.extract_field_from_timestamp(x, "hour"))
# test["month"] = test["created_utc"].apply(lambda x: date_utils.extract_field_from_timestamp(x, "month"))
#
# # Initialize testing label matrix
# total_size = dep_size + non_dep_size
# testing_labels = np.zeros(total_size)
# testing_labels[0:dep_size] = 1
#
# # Compute testing matrix
# # testing_matrix = evaluate_text("test")
# testing_matrix = np.reshape(test["hour"].tolist(), (-1, 1))
#
# # Prediction for the model
# print("\tConfusion matrix for the testing data")
# prediction = svc_model.predict(testing_matrix)
# print(confusion_matrix(testing_labels, prediction))
#
# result_dict = {
#     "Type": types,
#     "Model": models,
#     "Size training": train_sizes,
#     "Size testing": test_sizes,
#     "TP": tps,
#     "FP": fps,
#     "FN": fns,
#     "TN": tns}
