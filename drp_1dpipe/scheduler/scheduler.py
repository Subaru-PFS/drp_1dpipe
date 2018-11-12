"""
File: drp_1dpipe/scheduler/scheduler.py

Created on: 01/11/18
Author: CeSAM
"""

import json
import uuid
from drp_1dpipe import pre_process, process_spectra
from drp_1dpipe import pbs, local


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
    pre_process.main({'workdir': args.workdir,
                      'logdir': args.logdir,
                      'bunch_list': bunch_list})

    # process spectra
    parallel('process_spectra', bunch_list, 'spectra_listfile',
             args={'workdir': args.workdir,
                   'logdir': args.logdir})
