"""
File: drp_1dpipe/pre_process/pre_process.py

Created on: 24/10/18
Author: PSF DRP1D developers
"""

import os
import json
import logging
import argparse
from drp_1dpipe.io.utils import init_logger, get_args_from_file

def define_program_options():
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
    parser.add_argument('--templates_path', type=str, required=True,
                        help='Path to templates')

    # outputs
    parser.add_argument('--bunch_list', type=str, required=True,
                        help='List of files of bunch of astronomical objects')
    parser.add_argument('--template_list', type=str, required=True,
                        help='List of templates used in process_spectra')
    args = parser.parse_args()
    get_args_from_file("pre_process.conf", args)

    init_logger("pre_process", args.logdir, args.loglevel)

    # Start the main program
    main(args)


def main(args):
    """
    The "main" function.

    This function creates two files. A .json containing a list of
    templates, and another one containing

    :param args: parsed arguments of the program.
    """
    logger = logging.getLogger("pre_process")

    spectra_path = args.spectra_path
    bunch_size = args.bunch_size

    # bunch
    bunch_list = []
    for ao_list in bunch(bunch_size, os.path.join(args.workdir, args.spectra_path)):
        bunch_list.append(ao_list)

    # create json containing list of bunches
    output_list = os.path.join(args.workdir, args.bunch_list)
    with open(output_list, 'w') as f:
        f.write(json.dumps(bunch_list))

    # Generate the template list :
    l = []
    for file in os.listdir(os.path.join(args.workdir, args.templates_path)):
        l.append(file)
    template_list = os.path.join(args.workdir, args.template_list)
    with open(template_list, 'w') as ff:
        ff.write(json.dumps(l))

    return output_list, template_list

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
