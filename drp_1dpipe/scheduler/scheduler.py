"""
File: drp_1dpipe/scheduler/scheduler.py

Created on: 01/11/18
Author: CeSAM
"""

import json
import uuid
import argparse
import subprocess
from drp_1dpipe.io.utils import init_logger, get_args_from_file
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

    args = parser.parse_args()
    get_args_from_file("scheduler.conf", args)

    # Initialize logger
    init_logger("scheduler", args.logdir, args.loglevel)

    run(args)

def run(args):

    if args.scheduler.lower() == 'pbs':
        parallel = pbs.parallel
    elif args.scheduler.lower() == 'local':
        parallel = local.parallel
    else:
        raise "Unknown scheduler {}".format(args.scheduler)

    bunch_list = '{}.json'.format(uuid.uuid4().hex)

    # prepare workdir
    result = subprocess.run(['pre_process', '--workdir={}'.format(args.workdir),
                             '--logdir={}'.format(args.logdir),
                             '--bunch_list={}'.format(bunch_list)])
    assert result.returncode == 0

    # process spectra
    parallel('process_spectra', bunch_list, 'spectra_listfile',
             args={'workdir': args.workdir,
                   'logdir': args.logdir})
