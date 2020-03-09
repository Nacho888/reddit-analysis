import json
import os
import logging
import shutil
#####
import logging_factory
import date_utils
#####
logger_err = logging_factory.get_module_logger("file_manager_err", logging.ERROR)
logger = logging_factory.get_module_logger("file_manager", logging.DEBUG)


def write_scale_post_to_backup(data: dict, query: str, scale: str, timestamp: int):
    """
    Function that given a query and a scale, writes the data as .jsonl format, to its corresponding
    backup file and folder

    :param data: dict - the data to be saved as .jsonl format
    :param query: str - the current query being used
    :param scale: str - the current scale being used
    :param timestamp: int - timestamp when the query was performed
    :return: True/False - true if backup successfully, false otherwise

    """

    filename, directory = "", ""

    try:
        save_path = "./backups/"
        # Create, if not present, folder to store posts' backups
        if not os.path.isdir(save_path):
            os.mkdir(save_path)

        query_name_file = query.replace(" ", "-")
        query_name_file = query_name_file.strip()
        filename = "{}_{}.jsonl".format(query_name_file, timestamp)

        # One directory per scale
        directory = save_path + scale.strip() + "/"
        if not os.path.isdir(directory):
            os.mkdir(directory)
    except FileExistsError:
        # Add error to print it later
        logger_err.error("Directory '{}' already exists".format(directory))
        pass

    save_path = directory
    return write_to_file(data, save_path, filename)


def write_to_file(data: dict, save_path: str, filename: str):
    """
    Function that writes data to a .jsonl file in the required format and returns true if successful and
    False if errored

    :param data: dict - the data to be saved as .jsonl format
    :param save_path: str - the path to the file
    :param filename: str - the name of the file
    :return: True/False - true if backup successfully, false otherwise

    """
    # Write .jsonl file backup
    try:
        with open(os.path.join(save_path, filename), 'a') as outfile:
            json.dump(data, outfile)
            outfile.write('\n')
            return True
    except (OSError, IOError):
        logger_err.error("Read/Write error has occurred with file '{}'".format(filename))
        return False
    except UnicodeEncodeError:
        logger_err.error("Encoding error has occurred")
        return False


def del_backups():
    """
    Function that deletes the default backups' folders containing all the .jsonl files for each scale (but leaves other
    file in the root of the ./backups directory)

    """
    path = "./backups/"

    for root, subdirs, files in os.walk(path):
        for subdir in subdirs:
            if os.path.isdir(os.path.join(root, subdir)):
                try:
                    shutil.rmtree(os.path.join(root, subdir))
                    logger.debug("{} deleted successfully".format(os.path.join(root, subdir)))
                except FileNotFoundError:
                    logger_err.error("Error when deleting backups' folder")


def check_json(path, change_name):
    """
    Function that checks the correct format of the .jsonl files (in special date formats and not needed lines); it
    changes int timestamps to ISO8601 date format, deletes errored lines and add a field called "hour"

    :param path: str - path where the backups' folder is
    :param change_name: bool - True if you want to delete the files took as base for updating (recommended after the
    first execution where you should set it to False), False otherwise

    """
    for root, subdirs, files in os.walk(path):
        for file in files:
            if change_name:
                if "upd" not in file:
                    os.remove(os.path.join(root, file))
                else:
                    new_filename_arr = file.split("_")
                    new_filename = str(new_filename_arr[0]) + "_" + str(new_filename_arr[1]) + ".jsonl"
                    os.rename(os.path.join(root, file), os.path.join(root, new_filename))
            else:
                if "upd" not in file:
                    with open(os.path.join(root, file), 'r+') as original_file:
                        for i, line in enumerate(original_file):
                            # print("\ni: {} - line: {}".format(i, line.strip("\n")))
                            if line.startswith('{"id":'):  # Skip bad formatted lines
                                temp = json.loads(line)

                                if isinstance(temp["created_utc"], int):
                                    temp["created_utc"] = date_utils.get_iso_date_str(temp["created_utc"])
                                if isinstance(temp["retrieved_on"], int):
                                    temp["retrieved_on"] = date_utils.get_iso_date_str(temp["retrieved_on"])

                                if isinstance(temp["timestamp"], int):
                                    temp["timestamp"] = date_utils.get_iso_date_str(temp["timestamp"], "0100")
                                    # print(date_utils.get_numeric_timestamp_from_iso(temp["timestamp"]))
                                    # print("i: {} - updated_line: {}".format(i, temp))

                                temp["post_hour"] = date_utils.extract_hour_from_timestamp(temp["created_utc"])
                                print(temp["post_hour"])

                                file_name_arr = file.split(".")
                                file_name = str(file_name_arr[0]) + "_upd." + str(file_name_arr[1])
                                with open(os.path.join(root, file_name), "a") as second_file:
                                    json.dump(temp, second_file)
                                    second_file.write('\n')


# check_json("./backups/", True)
# del_backups()
