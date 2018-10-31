"""
File: drp_1dpipe/split/split.py

Created on: 24/10/18
Author: CeSAM
"""

import sys
import argparse
import logging
from drp_1dpipe.tools.logger import SplitLogger


def main():

    splitLogObj = SplitLogger()
    logger = logging.getLogger("Split logger")
    logger.info('###')
    logger.info('### ENTERING SPLIT MAIN METHODE ###')
    logger.info('###')

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
