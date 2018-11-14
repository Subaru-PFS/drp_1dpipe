"""
File: drp_1dpipe/scheduler/scheduler.py

Created on: 01/11/18
Author: CeSAM
"""

import json
import uuid
import argparse
import subprocess
from drp_1dpipe.io.utils import init_logger, get_args_from_file, normpath
from drp_1dpipe.scheduler import pbs, local


def main():
    """
    The "define_program_options" function.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('--workdir', type=str, required=True,
                        help='The root working directory where data is located')
    parser.add_argument('--logdir', type=str, required=False,
                        help='The logging directory')
    parser.add_argument('--loglevel', type=str, required=False,
                        help='The logging level. CRITICAL, ERROR, WARNING, INFO or DEBUG')
    parser.add_argument('--scheduler', type=str, required=False,
                        help='The scheduler to use. Whether "local" or "pbs".')
    parser.add_argument('--pre_commands', type=str, required=False,
                        help='Commands to run before before process_spectra.')

    args = parser.parse_args()
    get_args_from_file("scheduler.conf", args)

    # Initialize logger
    init_logger("scheduler", args.logdir, args.loglevel)

    run(args)

def run(args):

    if args.scheduler.lower() == 'pbs':
        scheduler = pbs
    elif args.scheduler.lower() == 'local':
        scheduler = local
    else:
        raise "Unknown scheduler {}".format(args.scheduler)

    bunch_list = normpath(args.workdir, '{}.json'.format(uuid.uuid4().hex))

    # prepare workdir
    scheduler.single('pre_process', args={'workdir': normpath(args.workdir),
                                          'logdir': normpath(args.logdir),
                                          'pre_commands': args.pre_commands,
                                          'bunch_list': bunch_list})

    # process spectra
    scheduler.parallel('process_spectra', bunch_list, 'spectra_listfile',
                       args={'workdir': normpath(args.workdir),
                             'logdir': normpath(args.logdir),
                             'pre_commands': args.pre_commands})
