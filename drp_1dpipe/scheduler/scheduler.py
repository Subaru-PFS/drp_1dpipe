"""
File: drp_1dpipe/scheduler/scheduler.py

Created on: 01/11/18
Author: CeSAM
"""

import uuid
import logging
from drp_1dpipe import VERSION
from drp_1dpipe.io.utils import (init_logger, init_environ, get_args_from_file,
                                 normpath, init_argparse, get_auxiliary_path,
                                 TemporaryFilesSet, save_config_file,
                                 AbspathAction)
from drp_1dpipe.scheduler.runner import list_runners, get_runner
from drp_1dpipe.scheduler.notifier import init_notifier
from drp_1dpipe.scheduler import local, pbs, slurm  # noqa: F401
import argparse
import traceback


logger = logging.getLogger("scheduler")


def main():
    """Pipeline entry point.

    Initialize a logger, parse command line arguments, and call the run()
    function.
    """

    parser = init_argparse()

    defaults = {'scheduler': 'local',
                'pre-commands': '',
                'spectra-dir': 'spectra',
                'bunch-size': 8,
                'notification-url': '',
                'parameters-file': get_auxiliary_path("parameters.json"),
                'linemeas-parameters-file': get_auxiliary_path("linemeas-parameters.json"),
                'lineflux': 'on',
                'concurrency': 1,
                'output-dir':'output'}
    defaults.update(get_args_from_file('drp_1dpipe.conf'))

    parser.add_argument('--scheduler',
                        default=defaults['scheduler'],
                        choices=list_runners(),
                        help='The scheduler to use.')
    parser.add_argument('--pre-commands', metavar='COMMAND',
                        default=defaults['pre-commands'],
                        help='Commands to run before before process_spectra.')
    parser.add_argument('--spectra-dir', metavar='DIR',
                        default=defaults['spectra-dir'],
                        help='Base path where to find spectra. '
                        'Relative to workdir.')
    parser.add_argument('--bunch-size', '-n', metavar='SIZE',
                        default=defaults['bunch-size'],
                        help='Maximum number of spectra per bunch.')
    parser.add_argument('--notification-url', metavar='URL',
                        default=defaults['notification-url'],
                        #help='Notification URL.',
                        help=argparse.SUPPRESS)
    parser.add_argument('--lineflux', choices=['on', 'off', 'only'],
                        default=defaults['lineflux'],
                        help='Whether to do line flux measurements.'
                        '"on" to do redshift and line flux calculations, '
                        '"off" to disable line flux, '
                        '"only" to skip the redshift part.')
    parser.add_argument('--parameters-file', metavar='FILE',
                        default=defaults['parameters-file'],
                        help='Parameters file. Relative to workdir.')
    parser.add_argument('--linemeas-parameters-file', metavar='FILE',
                        default=defaults['linemeas-parameters-file'],
                        help='Parameters file used for line flux measurement. '
                        'Relative to workdir.')
    parser.add_argument('--concurrency', '-j', type=int,
                        default=defaults['concurrency'],
                        help='Concurrency level for parallel run.'
                        ' -1 means infinity.')
    parser.add_argument('--output-dir', '-o', metavar='DIR',
                        default=defaults['output-dir'],
                        help='Output directory.')
    parser.add_argument('--config', '-c', metavar='FILE', action=AbspathAction,
                        default=None,
                        help='Configuration file giving all these command line arguments')

    args = parser.parse_args()
    if args.config:
        vars(args).update(get_args_from_file(args.config))
        args = parser.parse_args(namespace=args)
        args.loglevel=int(args.loglevel)
        args.concurrency=int(args.concurrency)
        args.bunch_size=int(args.bunch_size)
    save_config_file(args)

    return run(args)


def run(args):
    """Run the 1D Data Reduction Pipeline.

    :return: 0 on success
    """

    # initialize logger
    init_logger('scheduler', args.logdir, args.loglevel)

    # Launch banner
    print("Running drp_1dpipe {}".format(VERSION))

    # set workdir environment
    init_environ(args.workdir)

    runner_class = get_runner(args.scheduler)
    if not runner_class:
        raise f'Unknown runner {args.scheduler}'

    notif = init_notifier(args.notification_url)

    bunch_list = normpath(args.workdir,
                          'list_{}.json'.format(uuid.uuid4().hex))

    notif.update('root', 'RUNNING')
    notif.update('pre_process', 'RUNNING')

    with TemporaryFilesSet(keep_tempfiles=args.loglevel <= logging.INFO) as tmpcontext:

        runner = runner_class(args, tmpcontext)

        # prepare workdir
        try:
            runner.single('pre_process',
                        args={'workdir': normpath(args.workdir),
                                'logdir': normpath(args.logdir),
                                'loglevel': args.loglevel,
                                'bunch-size': args.bunch_size,
                                'pre-commands': args.pre_commands,
                                'spectra-dir': normpath(args.spectra_dir),
                                'bunch-list': bunch_list})
        except Exception as e:
            traceback.print_exc()
            notif.update('root', 'ERROR')
        else:
            notif.update('pre_process', 'SUCCESS')
            tmpcontext.add_files(bunch_list)

        # process spectra
        try:
            runner.parallel('process_spectra', bunch_list,
                            'spectra-listfile', ['output-dir','logdir'],
                            args={'workdir': normpath(args.workdir),
                                  'logdir': normpath(args.logdir),
                                  'loglevel': args.loglevel,
                                  'lineflux': args.lineflux,
                                  'spectra-dir': normpath(args.spectra_dir),
                                  'pre-commands': args.pre_commands,
                                  'parameters-file': args.parameters_file,
                                  'linemeas-parameters-file': args.linemeas_parameters_file,
                                  'notifier': notif,
                                  'output-dir': args.output_dir})
        except Exception as e:
            traceback.print_exc()
            notif.update('root', 'ERROR')
        else:
            notif.update('root', 'SUCCESS')

    return 0


if __name__ == '__main__':
    main()