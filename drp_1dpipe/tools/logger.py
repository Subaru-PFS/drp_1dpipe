"""
File: drp_1dpipe/tools/logger.py

Created on: 29/10/18
Author: CeSAM
"""

import logging
from logging.handlers import RotatingFileHandler

class SplitLogger(object):

    split_logger = logging.getLogger("Split logger")
    split_logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

    # file handler
    file_handler = RotatingFileHandler('SPLIT_DEBUG.log', 'a', 1000000, 1)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    split_logger.addHandler(file_handler)

    # stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    split_logger.addHandler(stream_handler)

class ProcessSpectraLogger(object):
    # QUESTION: why is it imported despite "import SplitLogger" in split.py ?

    ps_logger = logging.getLogger("Process spectra logger")
    ps_logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

    # file handler
    file_handler = RotatingFileHandler('PROCESSSPECTRA_DEBUG.log', 'a', 1000000, 1)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    ps_logger.addHandler(file_handler)

    # stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    ps_logger.addHandler(stream_handler)
