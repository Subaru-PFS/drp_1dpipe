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
from drp_1dpipe.io.utils import init_logger, get_args_from_file, normpath
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
    parser.add_argument('--bunch_size', type=str, required=False,
                        help='Maximum number of spectra per bunch')

    # data input
    parser.add_argument('--spectra_path', type=str, required=False,
                        help='Path to spectra to process')

    # outputs
    parser.add_argument('--bunch_list', type=str, required=False,
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

    spectra_path = normpath(args.workdir, args.spectra_path)
    bunch_size = args.bunch_size

    # bunch
    bunch_list = []
    for ao_list in bunch(bunch_size, spectra_path):
        f = NamedTemporaryFile(prefix='spectralist_', dir=normpath(args.workdir), delete=False,
                               mode='w')
        json.dump(ao_list, f)
        bunch_list.append(normpath(args.workdir, f.name))

    # create json containing list of bunches
    output_list = normpath(args.workdir, args.bunch_list)
    with open(output_list, 'w') as f:
        json.dump(bunch_list, f)

def bunch(bunch_size, spectra_path):
    """
    The "bunch" function.

    Split files located "spectra_path" directory in bunches of "bunch_size" lists.

    :param bunch_size: The number of source per bunch.
    :param spectra_path: List of sources in the workdir.
    :return: a generator with the max number of sources.
    """
    logger = logging.getLogger("pre_process")

    _list = []
    for source in os.listdir(spectra_path):
        _list.append(normpath(spectra_path, source))
        if len(_list) >= int(bunch_size):
            yield _list
            _list = []
    if _list:
        yield _list
