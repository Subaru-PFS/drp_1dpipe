import os
import logging
from collections import namedtuple
import argparse
import shutil
import json

from drp_1dpipe import VERSION
from drp_1dpipe.core.config import ConfigJson
from drp_1dpipe.core.logger import init_logger
from drp_1dpipe.core.argparser import define_global_program_options, AbspathAction
from drp_1dpipe.core.utils import normpath, get_conf_path, config_update, config_save
from drp_1dpipe.merge_results.config import config_defaults
from drp_1dpipe.process_spectra.results import SpectrumResults, RedshiftSummary, StellarSummary, QsoSummary

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

    if not os.path.exists(config.bunch_listfile):
        raise FileNotFoundError("Bunch list file not found : {}".format(config.bunch_listfile))
    
    with open(config.bunch_listfile, "r") as ff :
        bunch_list = json.load(ff)
    
    data_dir = os.path.join(config.output_dir, 'data')
    os.makedirs(data_dir, exist_ok=True)
    galaxy_summary_list = []
    stellar_summary_list = []
    qso_summary_list = []
    for bunch in bunch_list:
        if not os.path.exists(bunch):
            raise FileNotFoundError("Bunch directory not found : {}".format(bunch))
        bunch_data_dir = os.path.join(bunch, "data")
        if not os.path.exists(bunch_data_dir):
            raise FileNotFoundError("Bunch data directory not found : {}".format(bunch_data_dir))
        to_merge = os.listdir(bunch_data_dir)
        for pfs_candidate in to_merge:
            shutil.move(
                os.path.join(bunch_data_dir, pfs_candidate),
                os.path.join(data_dir, pfs_candidate))

        try:
            amazed_results = RedshiftSummary(output_dir=bunch)
            amazed_results.read()
            galaxy_summary_list.extend(amazed_results.summary)
        except FileNotFoundError:
            raise FileNotFoundError("Redshift summary file not found in {}".format(bunch))

        try:
            amazed_results = StellarSummary(output_dir=bunch)
            amazed_results.read()
            stellar_summary_list.extend(amazed_results.summary)
        except:
            pass

        try:
            amazed_results = QsoSummary(output_dir=bunch)
            amazed_results.read()
            qso_summary_list.extend(amazed_results.summary)
        except:
            pass

    gsr = RedshiftSummary(output_dir=config.output_dir)
    gsr.summary = galaxy_summary_list
    gsr.write()
    ssr = StellarSummary(output_dir=config.output_dir)
    ssr.summary = stellar_summary_list
    ssr.write()
    qsr = QsoSummary(output_dir=config.output_dir)
    qsr.summary = qso_summary_list
    qsr.write()
    
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
    config_save(config, "merge_results_config.json")
    return main_method(config)


if __name__ == '__main__':
    main()