"""
File: drp_1dpipe/split/split.py

Created on: 24/10/18
Author: CeSAM
"""

import sys
import argparse
import logging
from drp_1dpipe.tools.logger import Logger
from logging.handlers import RotatingFileHandler


def main():

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

    # file handler
    file_handler = RotatingFileHandler('DEBUG.log', 'a', 1000000, 1)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # # stream handler
    # stream_handler = logging.StreamHandler()
    # stream_handler.setLevel(logging.DEBUG)
    # logger.addHandler(stream_handler)

    logger.info('Hello')
    logger.warning('Testing %s', 'foo')

    parser = argparse.ArgumentParser()
    parser.add_argument('--workdir', type=str, required=True,
                        help='The root working directory where the data is located')
    parser.add_argument('--logdir', type=str, required=False,
                        help='The logging directory')
    parser.add_argument('--bunch_size', type=str, required=False,
                        help='Maximum number of spectra per bunch')

    # input
    parser.add_argument('--spectra', type=str, required=False, # TODO: repasser à True
                        help='SIR combined spectra product')

    # outputs
    parser.add_argument('--spectra_sets_list', type=str, required=False, # repasser à True
                        help='List of files of bunch of astronomical objects')

    args = parser.parse_args()

    fits_list = args.spectra
    bunch_size = args.bunch_size

    # split(args, fits_list)


def split(fits_list, bunch_size):

    bunch(fits_list, bunch_size)

    args.spectra_sets_list = 1


def bunch(fits_list, bunch_size):
    result = []
    for source in iterable:
        result.append(source)
        if len(result) >= bunch_size:
            yield result
            result = []
    if result:
        yield result

# TODO: To remove
if __name__ == '__main__':
    main()
