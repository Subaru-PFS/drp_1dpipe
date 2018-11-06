import os.path
import logging
from logging.handlers import RotatingFileHandler


def get_auxiliary_path(dir):
    return os.path.join(os.path.dirname(__file__), 'auxdir', dir)


def get_conf_path(dir):
    return os.path.join(os.path.dirname(__file__), 'conf', dir)


def init_logger(process_name):
    logger = logging.getLogger(process_name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

    # file handler
    file_handler = logging.FileHandler(process_name + '.log', 'w')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)


def get_args_from_file(file_name, args):
    with open(file_name, 'r') as ff:
        lines = ff.readlines()
    for line in lines:
        try:
            key, value = line.replace('\n', '').split('#')[0].split("=")
            try:
                if getattr(args, key.strip()) is None:
                    setattr(args, key.strip(), value.strip())
            except AttributeError:
                setattr(args, key.strip(), value.strip())
        except ValueError:
            pass
