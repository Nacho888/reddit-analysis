import json
import os
import logging
#####
import logging_factory
#####
logger_err = logging_factory.get_module_logger("file_manager_err", logging.ERROR)
logger = logging_factory.get_module_logger("file_manager", logging.DEBUG)


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


def remove_file(path: str):
    try:
        os.remove(path)
    except OSError:
        logger_err.error("File cannot be removed")


def clear_path(path: str):
    for file in os.listdir(path):
        os.remove(os.path.join(path, file))


def create_subdir(base: str, sub_dir: str):
    if not os.path.exists(base):
        os.mkdir(base)
    os.mkdir(os.path.join(base, sub_dir))


def cut_datasets(path: str, size_training: int, split_prop: float):
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
