"""
File: drp_1dpipe/scheduler/scheduler.py

Created on: 01/11/18
Author: CeSAM
"""

import uuid
import logging
from drp_1dpipe.io.utils import init_logger, get_args_from_file, normpath, \
    init_argparse
from drp_1dpipe.scheduler import pbs, local
from drp_1dpipe.scheduler.notifier import Notifier, DummyNotifier

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
                'lineflux': 'on'}
    defaults.update(get_args_from_file('drp_1dpipe.conf'))

    parser.add_argument('--scheduler', metavar='SCHEDULER',
                        default=defaults['scheduler'],
                        help='The scheduler to use. Either "local" or "pbs".')
    parser.add_argument('--pre-commands', metavar='COMMAND',
                        default=defaults['pre-commands'],
                        help='Commands to run before before process_spectra.')
    parser.add_argument('--spectra-dir', metavar='DIR',
                        default=defaults['spectra-dir'],
                        help='Base path where to find spectra. '
                        'Relative to workdir.')
    parser.add_argument('--bunch-size', metavar='SIZE',
                        default=defaults['bunch-size'],
                        help='Maximum number of spectra per bunch.')
    parser.add_argument('--notification-url', metavar='URL',
                        default=defaults['notification-url'],
                        help='Notification URL.')
    parser.add_argument('--lineflux', choices=['on', 'off', 'only'],
                        default=defaults['lineflux'],
                        help='Whether to do line flux measurements.'
                        '"on" to do redshift and line flux calculations, '
                        '"off" to disable line flux, '
                        '"only" to skip the redshift part.')

    args = parser.parse_args()

    return run(args)


def run(args):
    """Run the 1D Data Reduction Pipeline.

    :return: 0 on success
    """

    # initialize logger
    init_logger('scheduler', args.logdir, args.loglevel)

    if args.scheduler.lower() == 'pbs':
        scheduler = pbs
    elif args.scheduler.lower() == 'local':
        scheduler = local
    else:
        raise 'Unknown scheduler {}'.format(args.scheduler)

    if args.notification_url:
        try:
            notif = Notifier(args.notification_url,
                             name='pfs-{}'.format(uuid.uuid4()),
                             nodes={
                                 'root': {'type': 'SERIAL',
                                          'children': ['pre_process',
                                                       'process_spectra']},
                                 'pre_process': {'name': 'pre_process',
                                                 'type': 'TASK'},
                                 'process_spectra': {'name': 'process_spectra',
                                                     'type': 'PARALLEL'}
                             })
        except Exception as e:
            logger.log(logging.INFO, "Can't initialize notifier. "
                       "Using DummyNotifier. {}".format(e))
            notif = DummyNotifier()
    else:
        notif = DummyNotifier()

    bunch_list = normpath(args.workdir,
                          'list_{}.json'.format(uuid.uuid4().hex))

    notif.update('root', 'RUNNING')
    notif.update('pre_process', 'RUNNING')

    # prepare workdir
    scheduler.single('pre_process',
                     args={'workdir': normpath(args.workdir),
                           'logdir': normpath(args.logdir),
                           'loglevel': args.loglevel,
                           'bunch-size': args.bunch_size,
                           'pre-commands': args.pre_commands,
                           'spectra-dir': normpath(args.spectra_dir),
                           'bunch-list': bunch_list})

    notif.update('pre_process', 'SUCCESS')

    # process spectra
    try:
        scheduler.parallel('process_spectra', bunch_list,
                           'spectra-listfile', 'output-dir',
                           args={'workdir': normpath(args.workdir),
                                 'logdir': normpath(args.logdir),
                                 'loglevel': args.loglevel,
                                 'lineflux': args.lineflux,
                                 'spectra-dir': normpath(args.spectra_dir),
                                 'pre-commands': args.pre_commands,
                                 'notifier': notif,
                                 'output-dir': 'output-'})
    except Exception as e:
        logger.log(logging.ERROR, 'Error in process_spectra:', e)
        notif.update('root', 'ERROR')
    else:
        notif.update('root', 'SUCCESS')

    return 0
