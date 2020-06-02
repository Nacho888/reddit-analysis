import json
import os
import logging
#####
import logging_factory
#####
logger_err = logging_factory.get_module_logger("file_manager_err", logging.ERROR)
logger = logging_factory.get_module_logger("file_manager", logging.DEBUG)


def write_to_file(data: dict, save_path: str, filename: str, mode: str):
    """
    Function that writes data to a .jsonl file in the required format and returns true if successful and
    False if errored

    :param data: dict - the data to be saved as .jsonl format
    :param save_path: str - the path to the file
    :param filename: str - the name of the file
    :param mode: str - w/a to overwrite or to append
    :return: True/False - true if backup successfully, false otherwise
    """

    # Write .jsonl file backup
    try:
        with open(os.path.join(save_path, filename), mode) as outfile:
            json.dump(data, outfile)
            outfile.write('\n')
            return True
    except (OSError, IOError):
        logger_err.error("Read/Write error has occurred with file '{}'".format(filename))
        return False
    except UnicodeEncodeError:
        logger_err.error("Encoding error has occurred")
        return False


def count_lines_file(path: str, filename: str):
    """
    Function that returns the number of lines of a file given its path

    :param path: str - the path to the file
    :param filename: str - the name of the file
    :return: int - the number of lines of the file
    """

    with open(os.path.join(path, filename)) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def clear_file(save_path: str, filename: str):
    open(os.path.join(save_path, filename), "w").close()


def sort_file(save_path: str, filename: str):
    words = []
    with open(os.path.join(save_path, filename)) as file:
        for line in file:
            words.append(line)
    words.sort()

    clear_file(save_path, filename)
    with open(os.path.join(save_path, filename), "a") as file:
        for line in words:
            file.write(line)


def files_in_path(path: str):
    return os.listdir(path)


def populate_dataset(source_path, target_path, target_name, skip, size):
    count = 0
    skipped = 0
    with open(source_path, "r") as source:
        for line in source:
            if skipped != skip:
                skipped += 1
            else:
                if count < size:
                    response = write_to_file(json.loads(line), target_path, target_name, "a")
                    count += 1 if response else 0
                else:
                    break


def vector_feature_from_dataset(dataset_path, dataset_name, feature, size):
    result = []
    with open(os.path.join(dataset_path, dataset_name), "r") as dataset:
        for line in dataset:
            data = json.loads(line)
            result.append(data[feature])
    return result


def check_dataset_present(train_size):
    base = "datasets/"
    count = 0
    for directory in os.listdir(base):
        for file in os.listdir(os.path.join(base, directory)):
            if str(train_size) in file:
                if "training" in file:
                    count += 1
                if "testing" in file:
                    count += 1
    return True if count == 4 else False


check_dataset_present(10000)
