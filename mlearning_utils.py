def get_pronoun_proportion(text: list):
    """
    Function that a given a text formatted as a list of words, return the proportion of pronouns of each type

    :param text: list - the list of words to be processed
    :return: dict - containing the proportion of pronouns of first, second and third person
    """

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
    result = {
        "pp1": round(pronouns["pp1"] / total_pronouns, 2) if total_pronouns != 0 else 0,
        "pp2": round(pronouns["pp2"] / total_pronouns, 2) if total_pronouns != 0 else 0,
        "pp3": round(pronouns["pp3"] / total_pronouns, 2) if total_pronouns != 0 else 0
    }

    return result
