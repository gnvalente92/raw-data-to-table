import os

from bq_external_table.utils import logger
from argparse import ArgumentParser, ArgumentTypeError

log = logger.get_logger()


def parse_arguments():
    """
    Method to parse arguments, any check need is made on the type argument, each type represents a function.
    :return: Parsed arguments
    """
    parser = ArgumentParser(description="BQ External Table Application")

    parser.add_argument("-j", "--json_config", dest="json_config", help="The JSON Config File Location",
                        type=json_config_file, required=True)

    log.info("Parsing arguments")
    return parser.parse_args()


def json_config_file(value):
    """
    Method that call the json validation method and raises an Exception if not
    :param value: The json file path that was passed as argument
    :return: Value or Exception in case of error
    """
    if is_json_file_location_valid(value):
        log.info(value + " - JSON config file path is valid")
        return value
    else:
        msg = value + " - JSON config location is not valid"
        log.error(msg)
        raise ArgumentTypeError


def is_json_file_location_valid(json_file_path):
    """
    Method that checks if the json_file_path is indeed a json file.
    :param json_file_path: The file path of the json file
    :return: True if file is a json file or False if not
    """
    log.info("Checking if json config file path - " + json_file_path + " - is valid")
    return os.path.basename(json_file_path).split(".")[-1] == "json" and os.path.exists(json_file_path)
