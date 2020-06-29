import pandas as pd
from nltk import word_tokenize, PorterStemmer


def pre_process(text: str):
    """
    Function that pre-processes the text:
        1) Remove line breaks/carriage returns
        2) Lowercase
        3) Only alphanumerics are left
        4) Stem words

    :param text: str - the text to be processed
    :return: the processed text
    """
    import re
    ps = PorterStemmer()

    if pd.isna(text):  # Missing value in post title or selftext, treated as NaN in Pandas
        return " "
    else:
        processed_text = text.replace("\n", " ").replace("\r", "")  # Remove line breaks/carriage returns

        processed_text = processed_text.lower()  # Lowercase

        pattern = re.compile(r"\W+", re.UNICODE)  # Only alphanumerics are left
        processed_text = pattern.sub(" ", processed_text)

        words = word_tokenize(processed_text)  # Stem words
        stemmed_words = [ps.stem(word) for word in words]
        processed_text = " ".join(stemmed_words)

        return processed_text


def get_pronoun_proportion(text: list, person_key: str):
    first_person = ["i", "me", "my", "mine", "myself"]
    second_person = ["you", "your", "yours", "yourself"]
    third_person = ["he", "him", "his", "himself", "she", "her", "hers", "herself"]

    pronouns = {"pp1": 0, "pp2": 0, "pp3": 0}

    for word in text:
        if word in first_person:
            pronouns["pp1"] += 1
        elif word in second_person:
            pronouns["pp2"] += 1
        elif word in third_person:
            pronouns["pp3"] += 1

    total_pronouns = sum(pronouns.values())
    result = {"pp1": round(pronouns["pp1"] / total_pronouns, 2) if total_pronouns != 0 else 0,
              "pp2": round(pronouns["pp2"] / total_pronouns, 2) if total_pronouns != 0 else 0,
              "pp3": round(pronouns["pp3"] / total_pronouns, 2) if total_pronouns != 0 else 0
              }

    return result[person_key]
