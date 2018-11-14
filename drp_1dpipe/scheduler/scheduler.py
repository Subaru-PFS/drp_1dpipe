"""
File: drp_1dpipe/scheduler/scheduler.py

Created on: 01/11/18
Author: CeSAM
"""

import json
import uuid
import argparse
import subprocess
from drp_1dpipe.io.utils import init_logger, get_args_from_file, normpath, init_argparse
from drp_1dpipe.scheduler import pbs, local


def main():
    """Pipeline entry point.

    Initialize a logger, parse command line arguments, and call the run() function.
    """

    parser = init_argparse()

    parser.add_argument('--scheduler', metavar='SCHEDULER',
                        help='The scheduler to use. Whether "local" or "pbs".')
    parser.add_argument('--pre_commands', metavar='COMMAND',
                        help='Commands to run before before process_spectra.')
    parser.add_argument('--spectra_path', metavar='DIR',
                        help='Base path where to find spectra. Relative to workdir.')

    args = parser.parse_args()
    get_args_from_file("scheduler.conf", args)

    return run(args)

def run(args):
    """Run the 1D Data Reduction Pipeline.

    :return: 0 on success
    """

    # initialize logger
    init_logger("scheduler", args.logdir, args.loglevel)

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
                                          'spectra_path': normpath(args.spectra_path)
                                          'bunch_list': bunch_list})

    # process spectra
    scheduler.parallel('process_spectra', bunch_list, 'spectra_listfile', 'output_dir',
                       args={'workdir': normpath(args.workdir),
                             'logdir': normpath(args.logdir),
                             'pre_commands': args.pre_commands,
                             'output_dir': normpath(args.workdir, 'output-')})

    # merge results
    scheduler.single('merge_results', args={'workdir': normpath(args.workdir),
                                            'logdir': normpath(args.logdir),
                                            'spectra_path': normpath(args.spectra_path)
                                            'result_dirs': normpath(args.workdir, 'output-*')})

    return 0
