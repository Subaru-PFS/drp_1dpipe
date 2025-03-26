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
from drp_1dpipe.core.argparser import define_global_program_options, AbspathAction, ShowParametersAction
from drp_1dpipe.core.logger import init_logger
from drp_1dpipe.core.utils import ( get_args_from_file,
                                    normpath, get_auxiliary_path, get_conf_path,
                                    TemporaryFilesSet, config_update, config_save )
from drp_1dpipe.core.workers import get_worker,list_workers


from drp_1dpipe.scheduler.config import config_defaults
from drp_1dpipe.pre_process.pre_process import pre_process
from drp_1dpipe.process_spectra.process_spectra import main_no_parse
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
                        choices=list_workers(),
                        help='The scheduler to use.')
    parser.add_argument('--venv', metavar='DIR', action=AbspathAction,
                        help='Virtual environment path to load before running batch job')
    parser.add_argument('--concurrency', '-j', type=int,
                        help='Concurrency level for local parallel run. -1 means maximum.')
    parser.add_argument('--spectra_dir', metavar='DIR', action=AbspathAction,
                        help='Base path where to find pfsObjects. '
                        'Relative to workdir.')
    parser.add_argument('--coadd_file', metavar='FILE', action=AbspathAction,
                        help='Base path where to find pfsCoadd file '
                        'Relative to workdir.')
    parser.add_argument('--bunch_size', '-n', metavar='SIZE',
                        help='Maximum number of spectra per bunch.')
    parser.add_argument('--notification_url', metavar='URL',
                        help=argparse.SUPPRESS)
    parser.add_argument('--parameters_file', '-p', metavar='FILE', action=AbspathAction,
                        help='Parameters file. Relative to workdir.')
    parser.add_argument('--output_dir', '-o', metavar='DIR', action=AbspathAction,
                        help='Output directory.')
    parser.add_argument('--get_default_parameters',  action=ShowParametersAction,
                        choices=["galaxy+star+qso", "galaxy", "star"], default="galaxy+star+qso",
                        const="galaxy+star+qso",
                        help='Show example parameters ',nargs='?')
    parser.add_argument('--debug', action='store_true',
                        help="Debug mode, no worker")

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


    # prepare workdir
    try:
        nb_bunches=pre_process(config)
    except Exception as e:
        traceback.print_exc()
        raise e

    worker = get_worker(config.scheduler)(config)
    
    # process spectra

    try:
        if config.debug:
            main_no_parse(
                          args={
                                 'workdir': normpath(config.workdir),
                                 'spectra_dir': normpath(config.spectra_dir),
                                 'parameters_file': config.parameters_file,
                'spectra_listfile': os.path.join(config.output_dir,'spectralist_B0.json'),
                'output_dir': os.path.join(config.output_dir,'B0'),
                'logdir': os.path.join(config.output_dir,'log','B0'),
                             })
        else:
            for i in range(nb_bunches):
                worker.run(['process_spectra',i])
    except Exception as e:
        traceback.print_exc()

    worker.wait_all()

    worker.run(['merge_results'])
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
    if args.get_default_parameters is not None:
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
