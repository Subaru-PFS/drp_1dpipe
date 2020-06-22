"""
File: drp_1dpipe/scheduler/scheduler.py

Created on: 01/11/18
Author: CeSAM
"""

import uuid
import logging
import os
import argparse
import traceback
import json
from datetime import datetime

from drp_1dpipe import VERSION
from drp_1dpipe.core.argparser import define_global_program_options, AbspathAction
from drp_1dpipe.core.logger import init_logger
from drp_1dpipe.core.utils import ( init_environ, get_args_from_file,
                                    normpath, get_auxiliary_path, get_conf_path,
                                    TemporaryFilesSet, config_update, config_save )
from drp_1dpipe.core.engine.runner import get_runner, list_runners
from drp_1dpipe.core.engine import local #, pbs, slurm
from drp_1dpipe.core.notifier import init_notifier
from drp_1dpipe.scheduler.config import config_defaults


# logger = logging.getLogger("scheduler")


def define_specific_program_options():
    """Define specific program options.
    
    Return
    ------
    :obj:`ArgumentParser`
        An ArgumentParser object
    """

    parser = argparse.ArgumentParser(
        prog='drp_1dpipe',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument('--scheduler',
                        choices=list_runners(),
                        help='The scheduler to use.')
    parser.add_argument('--venv', metavar='COMMAND', action=AbspathAction,
                        help='Virtual environment path to load before running batch job')
    parser.add_argument('--concurrency', '-j', type=int,
                        help='Concurrency level for local parallel run. -1 means maximum.')
    parser.add_argument('--spectra_dir', metavar='DIR', action=AbspathAction,
                        help='Base path where to find spectra. '
                        'Relative to workdir.')
    parser.add_argument('--bunch_size', '-n', metavar='SIZE',
                        help='Maximum number of spectra per bunch.')
    parser.add_argument('--notification_url', metavar='URL',
                        help=argparse.SUPPRESS)
    parser.add_argument('--lineflux', choices=['on', 'off', 'only'],
                        help='Whether to do line flux measurements.'
                        '"on" to do redshift and line flux calculations, '
                        '"off" to disable line flux, '
                        '"only" to skip the redshift part.')
    parser.add_argument('--parameters_file', '-p', metavar='FILE', action=AbspathAction,
                        help='Parameters file. Relative to workdir.')
    parser.add_argument('--linemeas_parameters_file', metavar='FILE', action=AbspathAction,
                        help='Parameters file used for line flux measurement. '
                        'Relative to workdir.')
    parser.add_argument('--output_dir', '-o', metavar='DIR', action=AbspathAction,
                        help='Output directory.')

    return parser


def auto_dir(config):
    """Generate output and logdir directory from workdir, spectra_dir and timestamp

    Parameters
    ----------
    config : :obj:`config`
        Configuration object
    """
    if config.output_dir.strip() == '@AUTO@':
        dirname = "_".join(['drp1d', os.path.basename(config.spectra_dir), datetime.now().strftime("%Y%m%dT%H%M%SZ")])
        config.output_dir = os.path.join(config.workdir, dirname)
    if config.logdir.strip() == '@AUTO@':
        config.logdir = os.path.join(config.output_dir, 'log')


def map_process_spectra_entries(json_bunch_list, output_dir, logdir):
    """Prepare arguments list for process spectra command

    Parameters
    ----------
    json_bunch_file : str
        Path to JSON file of bunch list
    output_dir : str
        Path to output directory
    logdir : str
        Path to log directory
    
    Return
    ------
    list, list, list
        Packed list for bunch list, output directory list, logdir list
    """
    with open(json_bunch_list, 'r') as f:
            bunch_list = json.load(f)

    output_list = []
    logdir_list = []
    for i, arg_value in enumerate(bunch_list):
        output_list.append(os.path.join(output_dir, 'B{}'.format(str(i))))
        logdir_list.append(os.path.join(logdir, 'B{}'.format(str(i))))
    return bunch_list, output_list, logdir_list


def reduce_process_spectra_output(json_bunch_list, output_dir, json_reduce):
    """Prepare arguments for merge result command

    Parameters
    ----------
    json_bunch_list : str
        Path to JSON file of bunch list
    output_dir : str
        Path to output directory
    """
    with open(json_bunch_list, 'r') as f:
            bunch_list = json.load(f)

    bunch_dir_list = []
    for i, arg_value in enumerate(bunch_list):
        bunch_dir_list.append(os.path.join(output_dir, 'B{}'.format(str(i))))
    
    # create json containing list of product
    with open(json_reduce, 'w') as f:
        json.dump(bunch_dir_list, f)


def list_aux_data(json_bunch_list, output_dir):
    """List all aux data directories

    Parameters
    ----------
    json_reduce : str
        Path to JSON file of bunch directory list 
    """
    with open(json_bunch_list, 'r') as f:
        bunch_list = json.load(f)

    aux_data_list = []
    for i, arg_value in enumerate(bunch_list):
        with open(arg_value, 'r') as f:
            file_list = json.load(f)
        for filename in file_list:
            aux_data_list.append(os.path.join(output_dir, 'B{}'.format(str(i)), os.path.splitext(filename)[0]))
    
    return aux_data_list


def main_method(config):
    """Run the 1D Data Reduction Pipeline.

    Returns
    -------
    int
        0 on success
    """

    # initialize logger
    logger = init_logger('scheduler', config.logdir, config.log_level)
    start_message = "Running drp_1dpipe {}".format(VERSION)
    logger.info(start_message)

    # Launch banner
    print(start_message)

    # set workdir environment
    init_environ(config.workdir)

    runner_class = get_runner(config.scheduler)
    # if not runner_class:
    #     error_message = "Unknown runner {}".format(config.scheduler)
    #     logger.error(error_message)
    #     raise error_message

    notifier = init_notifier(config.notification_url)

    json_bunch_list = normpath(config.output_dir, 'bunchlist.json')

    notifier.update('root', 'RUNNING')
    notifier.update('pre_process', 'RUNNING')

    with TemporaryFilesSet(keep_tempfiles=config.log_level <= logging.DEBUG) as tmpcontext:

        runner = runner_class(config, tmpcontext)

        # prepare workdir
        try:
            runner.single('pre_process',
                        args={'workdir': normpath(config.workdir),
                                'logdir': normpath(config.logdir),
                                'bunch_size': config.bunch_size,
                                'spectra_dir': normpath(config.spectra_dir),
                                'bunch_list': json_bunch_list,
                                'output_dir': normpath(config.output_dir)
                                })
        except Exception as e:
            traceback.print_exc()
            notifier.update('pre_process', 'ERROR')
            return 1
        else:
            notifier.update('pre_process', 'SUCCESS')
            # tmpcontext.add_files(json_bunch_list)

        # process spectra
        bunch_list, output_list, logdir_list = map_process_spectra_entries(
            json_bunch_list, config.output_dir, config.logdir)
        try:
            # runner.parallel('process_spectra', bunch_list,
            #                 'spectra-listfile', ['output-dir','logdir'],
            runner.parallel('process_spectra',
                            parallel_args={
                                'spectra_listfile': bunch_list,
                                'output_dir': output_list,
                                'logdir': logdir_list
                            },
                            args={
                                'workdir': normpath(config.workdir),
                                'lineflux': config.lineflux,
                                'spectra_dir': normpath(config.spectra_dir),
                                'parameters_file': config.parameters_file,
                                'linemeas_parameters_file': config.linemeas_parameters_file,
                            })
        except Exception as e:
            traceback.print_exc()
            notifier.update('root', 'ERROR')
        else:
            notifier.update('root', 'SUCCESS')
        
        json_reduce = normpath(config.output_dir, 'reduce.json')
        reduce_process_spectra_output(json_bunch_list, config.output_dir, json_reduce)
        try:
            runner.single('merge_results',
                          args={'workdir': normpath(config.workdir),
                                'logdir': normpath(config.logdir),
                                'output_dir': normpath(config.output_dir),
                                'bunch_listfile': json_reduce})
        except Exception as e:
            traceback.print_exc()
            notifier.update('merge_results', 'ERROR')
            return 1
        else:
            notifier.update('merge_results', 'SUCCESS')

        aux_data_list = list_aux_data(json_bunch_list, config.output_dir)
        for aux_dir in aux_data_list:
            tmpcontext.add_dirs(aux_dir)


    return 0


def main():
    """Pipeline entry point

    Return
    ------
    int
        Exit code of the main method
    """
    parser = define_specific_program_options()
    define_global_program_options(parser)
    args = parser.parse_args()
    config = config_update(
        config_defaults,
        args=vars(args),
        install_conf_path=get_conf_path("drp_1dpipe.json"),
        environ_var='DRP_1DPIPE_STARTUP'
        )
    auto_dir(config)
    config_save(config, "config.json")
    return main_method(config)


if __name__ == '__main__':
    main()