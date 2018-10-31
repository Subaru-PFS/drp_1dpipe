"""
File: drp_1dpipe/tools/logger.py

Created on: 29/10/18
Author: CeSAM
"""

import logging
from logging.handlers import RotatingFileHandler

class SplitLogger(object):
    """Class for Split module logger"""

    def __init__(self):

        self.split_logger = logging.getLogger("Split logger")
        self.split_logger.setLevel(logging.DEBUG)
        self.formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

        # file handler
        self.file_handler = RotatingFileHandler('split_debug.log', 'a', 1000000, 1)
        self.file_handler.setLevel(logging.DEBUG)
        self.file_handler.setFormatter(self.formatter)
        self.split_logger.addHandler(self.file_handler)

        # stream handler
        self.stream_handler = logging.StreamHandler()
        self.stream_handler.setLevel(logging.DEBUG)
        self.split_logger.addHandler(self.stream_handler)

class ProcessSpectraLogger(object):
    """Class for Process spectra module logger"""

    def __init__(self):

        self.ps_logger = logging.getLogger("Process spectra logger")
        self.ps_logger.setLevel(logging.DEBUG)
        self.formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

        # file handler
        self.file_handler = RotatingFileHandler('processSpectra_debug.log', 'a', 1000000, 1)
        self.file_handler.setLevel(logging.DEBUG)
        self.file_handler.setFormatter(self.formatter)
        self.ps_logger.addHandler(self.file_handler)

        # stream handler
        self.stream_handler = logging.StreamHandler()
        self.stream_handler.setLevel(logging.DEBUG)
        self.ps_logger.addHandler(self.stream_handler)
