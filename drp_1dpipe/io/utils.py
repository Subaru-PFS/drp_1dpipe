import os.path
import logging
from logging.handlers import RotatingFileHandler


def get_auxiliary_path(dir):
    return os.path.join(os.path.dirname(__file__), 'auxdir', dir)


def get_conf_path(dir):
    return os.path.join(os.path.dirname(__file__), 'conf', dir)


def get_split_logger():
    split_logger = logging.getLogger("split")
    split_logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

    # file handler
    # TODO: change append mode --> overwrite mode
    file_handler = RotatingFileHandler('split_debug.log', 'a', 1000000, 1)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    split_logger.addHandler(file_handler)

    # stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    split_logger.addHandler(stream_handler)


def get_process_spectra_logger():
    ps_logger = logging.getLogger("process_spectra")
    ps_logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

    # file handler
    # TODO: change append mode --> overwrite mode
    file_handler = RotatingFileHandler('processSpectra_debug.log', 'a', 1000000, 1)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    ps_logger.addHandler(file_handler)

    # stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    ps_logger.addHandler(stream_handler)
