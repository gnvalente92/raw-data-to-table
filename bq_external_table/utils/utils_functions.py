import json
import os

from bq_external_table.utils import logger

log = logger.get_logger()


def load_json_config(path):
    """
    Method to load a json file into a dict object

    :param path: Path of the json file
    :return: dict object of the json file
    """
    json_file = open_json_file(path)
    log.info("Loading json config from file " + path)
    return json.loads(json_file)


def open_json_file(path):
    """
    Method to load a json file into a dict object

    :param path: Path of the json file
    :return: dict object of the json file or None if the file does not exist.
    """
    log.info("Opening jsonfile " + path)
    if os.path.isfile(path):
        with open(path) as json_file:
            return json_file.read()
    else:
        log.info("File " + path + " does not exist")
        return None
