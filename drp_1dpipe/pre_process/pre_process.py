"""
File: drp_1dpipe/pre_process/pre_process.py

Created on: 24/10/18
Author: PSF DRP1D developers
"""

import os
import json
import logging
import argparse
from tempfile import NamedTemporaryFile
from drp_1dpipe.io.utils import init_logger, get_args_from_file
from tempfile import NamedTemporaryFile

def main():
    """
    The "define_program_options" function.

    This function initializes a logger, parses all command line arguments,
    and call the main() function.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('--workdir', type=str, required=True,
                        help='The root working directory where data is located')
    parser.add_argument('--logdir', type=str, required=False,
                        help='The logging directory')
    parser.add_argument('--loglevel', type=str, required=False,
                        help='The logging level')
    parser.add_argument('--bunch_size', type=str, required=True,
                        help='Maximum number of spectra per bunch')

    # data input
    parser.add_argument('--spectra_path', type=str, required=True,
                        help='Path to spectra to process')

    # outputs
    parser.add_argument('--bunch_list', type=str, required=True,
                        help='List of files of bunch of astronomical objects')
    args = parser.parse_args()
    get_args_from_file("pre_process.conf", args)

    init_logger("pre_process", args.logdir, args.loglevel)

    # Start the main program
    run(args)


def run(args):
    """
    Prepare workdir for process_spectra.

    This function creates a json file containing a list of list of spectra.

    :param args: parsed arguments of the program.
    """
    logger = logging.getLogger("pre_process")

    spectra_path = args.spectra_path
    bunch_size = args.bunch_size

    # bunch
    bunch_list = []
    for ao_list in bunch(bunch_size, os.path.join(args.workdir, args.spectra_path)):
        f = open(NamedTemporaryFile(prefix='spectralist_', dir=args.workdir, delete=False), 'w')
        json.dump(f, ao_list)
        bunch_list.append(f.name)

    # create json containing list of bunches
    output_list = os.path.join(args.workdir, args.bunch_list)
    with open(output_list, 'w') as f:
        f.write(json.dumps(bunch_list))

def bunch(bunch_size, spectra_path):
    """
    The "bunch" function.

    Split files located "spectra_path" directory in bunches of "bunch_size" lists.

    :param bunch_size: The number of source per bunch.
    :param spectra_path: List of sources in the workdir.
    :return: a generator with the max number of sources.
    """
    logger = logging.getLogger("pre_process")

    list = []
    for source in os.listdir(spectra_path):
        list.append(source)
        if len(list) >= int(bunch_size):
            yield list
            list = []
    if list:
        yield list
