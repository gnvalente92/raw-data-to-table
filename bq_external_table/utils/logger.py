import logging
import logging.handlers


def setup_logger(log_location):
    """
    Method to setup the logger object that will be used across the entire program
    :param log_location: The local location of the log file
    :return: The logger object
    """
    log_debug_level = logging.INFO
    log_formatter = '%(asctime)s - %(name)s - p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s'

    log_path = log_location
    logger = get_logger()
    logger.setLevel(log_debug_level)
    handler = logging.handlers.RotatingFileHandler(log_path, maxBytes=1048576, backupCount=3)
    formatter = logging.Formatter(log_formatter)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    console_logger = logging.StreamHandler()
    console_logger.setLevel(log_debug_level)
    console_logger.setFormatter(formatter)
    logger.addHandler(console_logger)

    return logger


def get_logger():
    return logging.getLogger("BQ External Table Loader Logger")
