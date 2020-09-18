import os
import logging
import json
#####
import logging_factory
#####
logger_err = logging_factory.get_module_logger("file_manager_err", logging.ERROR)
logger = logging_factory.get_module_logger("file_manager", logging.DEBUG)


def count_lines_file(path: str):
    """
    Function that returns the number of lines of a file given its path

    :param path: str - the path to the file
    :return: int - the number of lines of the file
    """

    with open(path) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def clear_file(save_path: str):
    """
    Given a path to a file, clears the contents of the file

    :param save_path: str - path to the file
    """

    open(save_path, "w").close()


def sort_file(path: str, field: str):
    """
    Given a file path (.jsonl format), sorts the contents of the file (using the provided key)

    :param path: str - path to the file
    :param field: str - the key to use in the sort
    """

    logger.debug("Starting file sorting by field '{}'...".format(field))

    # File will be loaded in a list (watch out with the size)
    lines = []
    with open(path) as file:
        for line in file:
            lines.append(json.loads(line))

    # Sort
    try:
        lines = sorted(lines, key=lambda k: int(k[field]), reverse=True)
    except KeyError:
        pass

    # Clean file
    clear_file(path)
    # And rewrite with the new sorting
    with open(path, "a") as file:
        for line in lines:
            file.write(json.dumps(line))
            file.write("\n")


def files_in_path(path: str):
    """
    Returns all the file names in a given path

    :param path: str - the path to analyze
    :return: list[str] - the filenames
    """

    return os.listdir(path)


def remove_file(path: str):
    """
    Given a path to a file, tries to delete it

    :param path: str - the path to the file
    """

    try:
        os.remove(path)
    except OSError:
        logger_err.error("File cannot be removed")


def clear_path(path: str):
    """
    Given a path, removes all the contents within it

    :param path: str - the path to be cleared
    """

    for file in os.listdir(path):
        os.remove(os.path.join(path, file))


def create_subdir(base: str, sub_dir: str):
    """
    Creates a child directory into a parent directory given both paths (parent will be created if not existing)

    :param base: str - the parent directory
    :param sub_dir: str - the child directory
    """

    if not os.path.exists(base):
        os.mkdir(base)
    os.mkdir(os.path.join(base, sub_dir))


def merge_backups(file1: str, file2: str):
    """
    Function that merges two backup file into a single one, appending the contents of one to the other

    :param file1: str - the name of the first file
    :param file2: str - the name of the second file
    """

    try:
        with open(os.path.join("./backups/", file2), "r") as input_file:
            with open(os.path.join("./backups/", file1), "a") as append_file:
                for line in input_file:
                    try:
                        json.dump(line, append_file)
                        append_file.write("\n")
                    except UnicodeEncodeError:
                        logger_err.error("Encoding error has occurred")
                        return False
    except (OSError, IOError):
        logger_err.error("Error merging the files, skipping...")
        return False
    return True


def cut_datasets(path: str, size_training: int, split_prop: float):
    """
    Given a path to a folder containing the whole datasets, the size of the training and the proportion of that size
    to be dedicated to testing, creates smaller datasets based on the big ones (with the suffix "_s" in the filename)

    :param path: str - path to the folder containing the datasets
    :param size_training: int - the required size of the training datasets
    :param split_prop: float - percentages formatted as, for example: 0.75 -> to obtain, 75% training and 25% testing
    """
    for subdir in os.listdir(path):
        for file in os.listdir(os.path.join(path, subdir)):
            filename = file.split(".")[0] + "_s." + file.split(".")[1]
            with open(os.path.join(path, subdir + "\\" + file), "r") as input_file:
                is_train = "training" in file
                with open(os.path.join(path, subdir + "\\" + filename), "a") as outfile:
                    written = 0
                    for line in input_file:
                        if (written >= size_training and is_train) or \
                                (written >= size_training * (1 - split_prop) and not is_train):
                            break
                        else:
                            outfile.write(line)
                            written += 1


# cut_datasets("datasets", 10000, 0.75)
