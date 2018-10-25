"""
File: drp_1dpipe/split/split.py

Created on: 24/10/18
Author: CeSAM
"""

import sys
import argparse
import logging

def main():

    logger = logging.getLogger()
    logger.warning('Entering "split" module "main()" method.')

    parser = argparse.ArgumentParser()

    parser.add_argument('--workdir', type=str, required=True,
                        help='The root working directory where the data is')
    parser.add_argument('--logdir', type=str, required=False,
                        help='The logging directory')
    parser.add_argument('--bunch_size', type=str, required=False,
                        help='Maximum number of spectra per bunch')

    # input
    parser.add_argument('--spectra', type=str, required=True,
                        help='Input spectra product')

    # outputs
    parser.add_argument('--spectra_sets_list', type=str, required=True,
                        help='Output bunch list')

    print("hello")

if __name__ == '__main__':
    main()
