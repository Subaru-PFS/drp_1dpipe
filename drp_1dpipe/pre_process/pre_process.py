"""
File: drp_1dpipe/pre_process/pre_process.py

Created on: 24/10/18
Author: PSF DRP1D developers
"""

import os
import json
import logging
from tempfile import NamedTemporaryFile
from drp_1dpipe.io.utils import init_logger, get_args_from_file, normpath, init_argparse
from tempfile import NamedTemporaryFile


logger = logging.getLogger("pre_process")

def main():
    """
    pre_process entry point.

    Parse command line arguments, and call the run() function.
    """

    parser = init_argparse()
    parser.add_argument('--bunch_size', metavar='SIZE', help='Maximum number of spectra per bunch.')

    # data input
    parser.add_argument('--spectra_path', metavar='DIR', help='Path to spectra to process. Relative to workdir.')

    # outputs
    parser.add_argument('--bunch_list', metavar='FILE',
                        help='List of files of bunch of astronomical objects.')

    args = parser.parse_args()
    get_args_from_file("pre_process.conf", args)

    # Start the main program
    return run(args)

def run(args):
    """
    Prepare workdir for process_spectra.

    This function creates a json file containing a list of list of spectra.

    :param args: parsed arguments of the program.
    :return: 0 on success
    """

    # initialize logger
    init_logger("pre_process", args.logdir, args.loglevel)

    spectra_path = normpath(args.workdir, args.spectra_path)
    bunch_size = args.bunch_size

    # bunch
    bunch_list = []
    for ao_list in bunch(bunch_size, spectra_path):
        f = NamedTemporaryFile(prefix='spectralist_', dir=normpath(args.workdir), delete=False,
                               mode='w')
        json.dump(ao_list, f)
        bunch_list.append(f.name)

    # create json containing list of bunches
    output_list = normpath(args.workdir, args.bunch_list)
    with open(output_list, 'w') as f:
        json.dump(bunch_list, f)

    return 0

def bunch(bunch_size, spectra_path):
    """
    The "bunch" function.

    Split files located "spectra_path" directory in bunches of "bunch_size" lists.

    :param bunch_size: The number of source per bunch.
    :param spectra_path: List of sources in the workdir.
    :return: a generator with the max number of sources.
    """

    _list = []
    for source in os.listdir(spectra_path):
        _list.append(source)
        if len(_list) >= int(bunch_size):
            yield _list
            _list = []
    if _list:
        yield _list
