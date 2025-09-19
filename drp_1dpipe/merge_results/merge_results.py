import os
import logging
import argparse
import shutil
import json
import sys

from drp_1dpipe.core.logger import init_logger
from drp_1dpipe.core.argparser import define_global_program_options, AbspathAction
from drp_1dpipe.core.utils import get_conf_path, config_update, config_save
from drp_1dpipe.merge_results.config import config_defaults
from drp_1dpipe.merge_results.pfsOutputAnalyzer import PfsOutputAnalyzer
from pylibamazed.Parameters import Parameters

from astropy.io import fits

import glob
import pandas as pd
logger = logging.getLogger("merge_results")


def define_write_analysis_options():
    """Define specific program options.
    
    Return
    ------
    :obj:`ArgumentParser`
        An ArgumentParser object
    """
    parser = argparse.ArgumentParser(
        prog='write_analysis',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument('--output_dir', '-o', metavar='DIR', action=AbspathAction,
                        help='Output directory.')
    parser.add_argument('--report_line_snr_threshold',type=float,
                        help='snr threshold use to define correctness of a line measurement in report.json')
   
    return parser

def write_report_cli():
    parser = define_write_analysis_options()
    define_global_program_options(parser)
    args = parser.parse_args()
    config = config_update(
        config_defaults,
        args=vars(args),
        install_conf_path=get_conf_path("merge_results.json")
        )
    write_analysis(config)

def define_make_diff_options():
    """Define specific program options.
    
    Return
    ------
    :obj:`ArgumentParser`
        An ArgumentParser object
    """
    parser = argparse.ArgumentParser(
        prog='write_analysis',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument('--output_dir', '-o', metavar='DIR', action=AbspathAction,
                        help='Output directory.')
    parser.add_argument('--ref_output_dir', '-r', metavar='DIR', action=AbspathAction,
                        help='Reference Output directory.')

    parser.add_argument('--report_line_snr_threshold',type=float,
                        help='snr threshold use to define correctness of a line measurement in report.json')
    parser.add_argument('--report_line_rdiff_threshold',type=float,
                        help='Relative difference threshold use to define correct line measurement in report.json')
   
    return parser

def make_diff_cli():
    parser = define_make_diff_options()
    define_global_program_options(parser)
    args = parser.parse_args()
    config = config_update(
        config_defaults,
        args=vars(args),
        install_conf_path=get_conf_path("merge_results.json")
        )
    make_diff(config)
    
def make_diff(config):
    with open(os.path.join(config.output_dir,"parameters.json")) as f:
        params = Parameters(json.load(f))

    po = PfsOutputAnalyzer(config.output_dir, None, params)
    opo = PfsOutputAnalyzer(config.ref_output_dir, None, params)
    zdiff = po.diff_redshifts(opo)
    for o in ["galaxy","qso"]:
        if len(zdiff[o]):
            print(f'\033[91m✖\033[00m Differences on {o} redshifts:')
            print(zdiff[o].to_string())
        else:
            print(f'\033[92m✔\033[00m No difference on {o} redshifts.')
    po.diff_lines(opo, config.report_line_snr_threshold, config.report_line_rdiff_threshold)
    
    
def write_analysis(config):
    with open(os.path.join(config.output_dir,"parameters.json")) as f:
        params = Parameters(json.load(f))

    
    po = PfsOutputAnalyzer(config.output_dir, None, params)
    po.load_results_summary()
    rs = po.redshifts
    reliable_threshold = 0.9
    report = dict()
    report["ObjectCount"] = len(rs)
    report["Count"] = dict()
    report["Fraction"] = dict()
    report["ZError"] = dict()
    report["ZErrorFraction"] = dict()
    report["LError"] = dict()
    report["LErrorFraction"] = dict()
    for o in ["galaxy","qso","star"]:
        if o in rs["classification.Type"].unique():
            report["Count"][o] = int(rs.groupby("classification.Type").count().at[o,"SpectrumID"])
            report["Fraction"][o] = report["Count"][o]*100/report["ObjectCount"]
        else:
            report["Count"][o] = 0
            report["Fraction"][o] = 0
        report["ZError"][o] = int((rs[f'error.{o}.redshiftSolver.code'] != "SUCCESS" ).values.sum())
        report["ZErrorFraction"][o] = report["ZError"][o]*100/len(rs)
        if o != "star":
            report["LError"][o] = int((rs[f'error.{o}.lineMeasSolver.code'] != "SUCCESS" ).values.sum()) 
            report["LErrorFraction"][o] = report["LError"][o]*100/len(rs)
    report["ReliableFraction"] = len(rs[rs.RedshiftProba > reliable_threshold])*100/len(rs)
    report["ReducedLeastSquare"] = dict()
    for o in ["galaxy","qso","star"]:
        report["ReducedLeastSquare"][o] = dict()
        ls = rs[f'{o}.ReducedLeastSquare'] 
        report["ReducedLeastSquare"][o]["mean"] = float(ls.mean())
        report["ReducedLeastSquare"][o]["min"] = float(ls.min())
        report["ReducedLeastSquare"][o]["max"] = float(ls.max())
        report["ReducedLeastSquare"][o]["std"] = float(ls.std())
        report["ReducedLeastSquare"][o]["median"] = float(ls.median())

    report["ReducedLeastSquare"]["classified"] = dict()
    ls = rs['ReducedLeastSquare'] 
    report["ReducedLeastSquare"]["classified"]["mean"] = float(ls.mean())
    report["ReducedLeastSquare"]["classified"]["min"] = float(ls.min())
    report["ReducedLeastSquare"]["classified"]["max"] = float(ls.max())
    report["ReducedLeastSquare"]["classified"]["std"] = float(ls.std())
    report["ReducedLeastSquare"]["classified"]["median"] = float(ls.median())

        
    report["LinesGlobal"] = po.get_global_lines_infos(config.report_line_snr_threshold) 
    with open(os.path.join(config.output_dir, "report.json"),'w') as f:
        json.dump(report,f,indent=4)


    
def merge_results(config):
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
    start_message = "Running merge_results"
    logger.info(start_message)
    
    data_dir = os.path.join(config.output_dir, 'data')
    os.makedirs(data_dir, exist_ok=True)
    nb_bunches = len(glob.glob(os.path.join(config.output_dir,f'spectralist_B*.json')))

    for bunch_id in range(nb_bunches):
        bunch_dir = os.path.join(config.output_dir,f'B{bunch_id}')
        ps_path = os.path.join(config.output_dir,f"process_spectra_{bunch_id}.sh")
        if os.path.isfile(ps_path):
            os.remove(ps_path)
        ps_path = os.path.join(config.output_dir,f"spectralist_B{bunch_id}.json")
        if os.path.isfile(ps_path):
            os.remove(ps_path)
    try:
        write_analysis(config)
    except Exception as e:
        logger.error(f"failed to write report : {e}")
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
    return merge_results(config)


if __name__ == '__main__':
    main()
