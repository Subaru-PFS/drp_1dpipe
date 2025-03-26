import os
import logging
import argparse
import shutil
import json
import sys

from drp_1dpipe import VERSION
from drp_1dpipe.core.logger import init_logger
from drp_1dpipe.core.argparser import define_global_program_options, AbspathAction
from drp_1dpipe.core.utils import get_conf_path, config_update, config_save
from drp_1dpipe.merge_results.config import config_defaults

from astropy.io import fits

import glob
import pandas as pd
logger = logging.getLogger("mergs_results")


def define_specific_program_options():
    """Define specific program options.
    
    Return
    ------
    :obj:`ArgumentParser`
        An ArgumentParser object
    """
    parser = argparse.ArgumentParser(
        prog='merge_results',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument('--bunch_listfile', metavar='FILE',
                        help='List of bunch.')
    parser.add_argument('--output_dir', '-o', metavar='DIR', action=AbspathAction,
                        help='Output directory.')

    return parser


def concat_summury_files():
    pass

def main_method(config):
    """main_method

    Parameters
    ----------
    config : :obj:`Config`
        Configuration object

    Returns
    -------
    int
        0 on success
    """    

    # initialize logger
    logger = init_logger("merge_results", config.logdir, config.log_level)
    start_message = "Running merge_results {}".format(VERSION)
    logger.info(start_message)
    
    data_dir = os.path.join(config.output_dir, 'data')
    os.makedirs(data_dir, exist_ok=True)
    nb_bunches = len(glob.glob(os.path.join(config.output_dir,f'spectralist_B*.json')))
    redshifts_dfs = []
    for bunch_id in range(nb_bunches):
        bunch_dir = os.path.join(config.output_dir,f'B{bunch_id}')
        bunch_data_dir = os.path.join(bunch_dir,"data")
        if not os.path.exists(bunch_data_dir):
            raise FileNotFoundError("Bunch data directory not found : {}".format(bunch_data_dir))
        to_merge = os.listdir(bunch_data_dir)
        for pfs_candidate in to_merge:
            shutil.move(
                os.path.join(bunch_data_dir, pfs_candidate),
                os.path.join(data_dir, pfs_candidate))
        ps_path = os.path.join(config.output_dir,f"process_spectra_{bunch_id}.sh")
        if os.path.isfile(ps_path):
            os.remove(ps_path)
        ps_path = os.path.join(config.output_dir,f"spectralist_B{bunch_id}.json")
        if os.path.isfile(ps_path):
            os.remove(ps_path)
        try:
            shutil.rmtree(os.path.join(config.output_dir,f"B{bunch_id}"))
        except Exception as e:
            print(f'could not clean bunch dir : {e}',file=sys.stderr)
    
    return 0


def main():
    """Merge results entry point

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
        install_conf_path=get_conf_path("merge_results.json")
        )
    return main_method(config)


if __name__ == '__main__':
    main()
