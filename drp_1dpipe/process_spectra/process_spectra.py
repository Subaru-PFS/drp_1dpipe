import os
import json
import logging
import time
import argparse
import shutil
import traceback

from pylibamazed.CalibrationLibrary import CalibrationLibrary
from pylibamazed.ResultStoreOutput import ResultStoreOutput
from pylibamazed.ProcessFlow import ProcessFlow
from pylibamazed.Parameters import Parameters

from drp_1dpipe.core.logger import init_logger
from drp_1dpipe.core.argparser import define_global_program_options, AbspathAction
from drp_1dpipe.core.utils import normpath, get_conf_path, config_update, config_save
from drp_1dpipe.process_spectra.config import config_defaults

from drp_1dpipe.core.utils import normpath, TemporaryFilesSet
from drp_1dpipe.core.get_default_summary import get_default_summary_columns

from drp_1dpipe.io.PFSReader import PFSReader
from drp_1dpipe.io.PFSExternalStorage import PFSExternalStorage

from drp_1dpipe.io.redshiftCoCandidates import RedshiftCoCandidates
from drp_1dpipe.process_spectra.parameters import default_parameters

from pylibamazed.redshift import (CLog,
                                  CLogFileHandler,
                                  get_version)
import pandas as pd
import collections.abc
from flufl.lock import Lock
from datetime import timedelta,datetime
import resource


zlog = CLog.GetInstance()


def update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = update(d.get(k, {}), v)
        else:
            d[k] = v
    return d


logger = logging.getLogger("process_spectra")

_map_loglevel = {logging.CRITICAL: CLog.nLevel_Critical,
                 logging.ERROR: CLog.nLevel_Error,
                 logging.WARNING: CLog.nLevel_Warning,
                 logging.INFO: CLog.nLevel_Info,
                 logging.DEBUG: CLog.nLevel_Debug,
                 logging.NOTSET: CLog.nLevel_None}


def define_specific_program_options():
    """Define specific program options.
    
    Return
    ------
    :obj:`ArgumentParser`
        An ArgumentParser object
    """
    parser = argparse.ArgumentParser(
        prog='process_spectra',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )

    parser.add_argument('--spectra_dir', metavar='DIR', action=AbspathAction,
                        help='Path to spectra directory. '
                        'Relative to workdir.')
    parser.add_argument('--spectra_listfile', metavar='FILE',
                        help='JSON file holding a list of files of '
                        'astronomical objects.')
    parser.add_argument('--calibration_dir', metavar='DIR', action=AbspathAction,
                        help='Specify directory in which calibration files are'
                        ' stored. Relative to workdir.')
    parser.add_argument('--parameters_file', metavar='FILE',
                        help='Parameters file. Relative to workdir.')
    parser.add_argument('--process_method',
                        help='Process method to use. Whether DUMMY or AMAZED.')
    parser.add_argument('--output_dir', metavar='DIR', action=AbspathAction,
                        help='Directory where all generated files are going to'
                        ' be stored. Relative to workdir.')
    parser.add_argument('--continue', action='store_true', dest='continue_',
                        help='Continue a previous processing.')

    return parser


def _output_path(args, *path):
    return normpath(args.workdir, args.output_dir, *path)


def _process_spectrum(output_dir, spectrum, process_flow, user_param, storage) :
    try:
        zlog.LogInfo(f"Processing spectrum {spectrum.source_id}")
        output = process_flow.run(spectrum) 
    except Exception as e:
        logger.log(logging.ERROR,"Could not process spectrum: {}".format(e))
        return 0
    try:
        rc = RedshiftCoCandidates(output, storage, logger, process_flow.calibration_library)

        l = Lock(os.path.join(output_dir,"coZcand.lock"))
        l.lifetime = timedelta(hours=2)
        tstart = datetime.now()
        debut_user_time = resource.getrusage(resource.RUSAGE_SELF).ru_utime
        debut_system_time = resource.getrusage(resource.RUSAGE_SELF).ru_stime

        l.lock()
        
        end_user_time = resource.getrusage(resource.RUSAGE_SELF).ru_utime
        end_system_time = resource.getrusage(resource.RUSAGE_SELF).ru_stime
        tend = datetime.now()
        memory_used = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.log(logging.INFO, f"{spectrum.source_id}|waiting|{end_user_time - debut_user_time}|{end_system_time - debut_system_time}|{tend-tstart}|{memory_used}")

        tstart = datetime.now()
        debut_user_time = resource.getrusage(resource.RUSAGE_SELF).ru_utime
        debut_system_time = resource.getrusage(resource.RUSAGE_SELF).ru_stime
        rc.write_fits(output_dir)
        end_user_time = resource.getrusage(resource.RUSAGE_SELF).ru_utime
        end_system_time = resource.getrusage(resource.RUSAGE_SELF).ru_stime
        tend = datetime.now()
        memory_used = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        logger.log(logging.INFO, f"{spectrum.source_id}|writing|{end_user_time - debut_user_time}|{end_system_time - debut_system_time}|{tend-tstart}|{memory_used}")
        l.unlock()
    except Exception as e:
        logger.log(logging.ERROR,"Failed to write fits result for spectrum "
                   "{} : {}".format(spectrum.source_id, e))
        l.unlock()
        return 0
    return output

def _setup_pass(config):

    parameters_file = None
    if config.parameters_file:
        parameters_file = normpath(config.parameters_file)
    params = default_parameters.copy()
    user_params = None
    if parameters_file:
        try:
            # override default parameters with those found in parameters_file
            with open(parameters_file, 'r') as f:
                user_params = json.load(f)
                params = update(params, user_params )
        except Exception as e:
            logger.log(logging.INFO,
                       f'unable to read parameter file : {e}, using defaults')
            raise

    # setup calibration dir
    if not os.path.exists(config.calibration_dir):
        raise FileNotFoundError(f"Calibration directory does not exist: "
                                f"{config.calibration_dir}")
    params['calibrationDir'] = config.calibration_dir

    process_flow = ProcessFlow(vars(config), Parameters(params))

    return process_flow, user_params


def amazed(config):
    """Run the full-featured amazed client

    Parameters
    ----------
    config : :obj:`Config`
        Configuration object
    """
    logFileHandler = CLogFileHandler(os.path.join(config.logdir,
                                                        'amazed.log'))
    logFileHandler.SetLevelMask(_map_loglevel[config.log_level])
    #
    # Set up param and linecatalog for redshift pass
    #
    process_flow, user_parameters = _setup_pass(config)

    with open(normpath(config.workdir, config.spectra_listfile), 'r') as f:
        spectra_list = json.load(f)
        
    if type(spectra_list)==dict and 'coadd_file' in spectra_list.keys():
        config.coadd_file = spectra_list['coadd_file']
        spectra_list = spectra_list['objIdList']
    
        
    outdir = normpath(config.workdir, config.output_dir)
    os.makedirs(outdir, exist_ok=True)

    data_dir = os.path.join(outdir, 'data')
    os.makedirs(data_dir, exist_ok=True)
    summary_columns = get_default_summary_columns()
    lines_ids = process_flow.calibration_library.get_lines_ids(summary_columns)

    products = []

    for i, object_id in enumerate(spectra_list):
        try:
            spectrum_id = object_id
            storage = PFSExternalStorage(config, spectrum_id)
            reader = PFSReader(process_flow.calibration_library.parameters,
                               process_flow.calibration_library,
                               spectrum_id)
            resource = storage.read()
            reader.load_all(resource)
            spectrum = reader.get_spectrum()
            storage.close(resource)
        except Exception as e:
            logger.log(logging.ERROR, f"Could not read spectrum at {spectrum_path} with id {spectrum_id} : {e}")
            continue
        
        output = _process_spectrum(data_dir, spectrum,process_flow, user_parameters, storage)
        
    logger.log(logging.INFO, "Bunch terminated")
    with TemporaryFilesSet(keep_tempfiles=config.log_level <= logging.INFO) as tmpcontext:

        # save amazed version and parameters file to output dir
        version_file = _output_path(config, 'version.json')
        with open(version_file, 'w') as f:
            json.dump({'amazed-version': get_version()}, f)
        parameters_file = os.path.join(normpath(config.workdir, config.output_dir),
                                       'parameters.json')
        with open(parameters_file,'w') as f:
            json.dump(process_flow.parameters.parameters, f)
        tmpcontext.add_files(parameters_file)


def dummy(config):
    """A dummy client, for pipeline testing purpose.

    Parameters
    ----------
    config : :obj:`Config`
        Configuration object
    """
    logger.info("running dummy client {}".format(config))
    time.sleep(3)
    logger.info("done")


def main_method(config):
    """main method for processing spectra.

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
    init_logger("process_spectra", config.logdir, config.log_level)
    start_message = "Running process_spectra"
    logger.info(start_message)


    if config.process_method.lower() == 'amazed':
        amazed(config)
    elif config.process_method.lower() == 'dummy':
        dummy(config)
    else:
        raise "Unknown process_method {}".format(config.process_method)

    return 0


def main():
    """Process Spectra entry point.

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
        install_conf_path=get_conf_path('process_spectra.json')
        )
    return main_method(config)

def main_no_parse(args):
    config = config_update(
        config_defaults,
        args=args,
        install_conf_path=get_conf_path('process_spectra.json')
        )
    return main_method(config)

if __name__ == '__main__':
    main()
