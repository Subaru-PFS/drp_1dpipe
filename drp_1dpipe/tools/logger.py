import logging
from logging.handlers import RotatingFileHandler

class Logger():

    # logging.basicConfig(filename='pipeliner.log', level=logging.DEBUG,
    # format='%(asctime)s :: %(levelname)s :: %(message)s')

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

    # file handler
    file_handler = RotatingFileHandler('DEBUG.log', 'a', 1000000, 1)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)
