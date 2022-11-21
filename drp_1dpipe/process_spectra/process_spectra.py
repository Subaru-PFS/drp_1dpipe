import os
import json
import logging
import time
import argparse
import shutil
import traceback

from pylibamazed.CalibrationLibrary import CalibrationLibrary
from pylibamazed.ResultStoreOutput import ResultStoreOutput
from pylibamazed.Context import Context

from drp_1dpipe import VERSION
from drp_1dpipe.core.logger import init_logger
from drp_1dpipe.core.argparser import define_global_program_options, AbspathAction
from drp_1dpipe.core.utils import normpath, get_conf_path, config_update, config_save
from drp_1dpipe.process_spectra.config import config_defaults

from drp_1dpipe.core.utils import normpath, TemporaryFilesSet
from drp_1dpipe.io.reader import PfsObjectReader
from drp_1dpipe.io.redshiftCandidates import RedshiftCandidates
from drp_1dpipe.process_spectra.parameters import default_parameters

from pylibamazed.redshift import (CLog,
                                  CLogFileHandler,
                                  get_version)

import collections.abc

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


def _process_spectrum(output_dir, reader, context, user_param) :

    try:
        reader.load_all(None)
    except Exception as e:
        raise Exception(f"Failed to prepare reader for {reader.source_id} : {e}")


    output = context.run(reader)
    
    try:
        rc = RedshiftCandidates(output, reader, logger, user_param, context.calibration_library)
        logger.log(logging.INFO, "write fits")

        rc.write_fits(output_dir)
    except Exception as e:
        raise Exception("Failed to write fits result for spectrum "
                          "{} : {}".format(reader.source_id, e))


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

    context = Context(vars(config), params)

    return context, user_params


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
    context, user_parameters = _setup_pass(config)

    with open(normpath(config.workdir, config.spectra_listfile), 'r') as f:
        spectra_list = json.load(f)

    outdir = normpath(config.workdir, config.output_dir)
    os.makedirs(outdir, exist_ok=True)

    data_dir = os.path.join(outdir, 'data')
    os.makedirs(data_dir, exist_ok=True)

    products = []
    for i, spectrum_path in enumerate(spectra_list):
        spectrum = normpath(config.workdir, config.spectra_dir, spectrum_path["fits"])
        reader = PfsObjectReader(spectrum, context.calibration_library)
        nb_valid_points = reader.get_nb_valid_points()
        if nb_valid_points < 3000:
            logger.log(logging.WARNING,
                       "Invalid spectrum, only " + str(nb_valid_points) + " valid points, not processed")
            to_process = False
        else:
            to_process = True
        proc_id, ext = os.path.splitext(spectrum_path["fits"])
        spc_out_dir = os.path.join(outdir, proc_id )
        if to_process:
            # first step : compute redshift
            to_process = True
            if os.path.exists(spc_out_dir):
                if config.continue_:
                    to_process = False
                else:
                    shutil.rmtree(spc_out_dir)
            if to_process:
                try:
                    _process_spectrum(data_dir, reader,context, user_parameters)
                except Exception as e:
                    logger.log(logging.ERROR,"Could not process spectrum: {}".format(e))

    with TemporaryFilesSet(keep_tempfiles=config.log_level <= logging.INFO) as tmpcontext:

        # save amazed version and parameters file to output dir
        version_file = _output_path(config, 'version.json')
        with open(version_file, 'w') as f:
            json.dump({'amazed-version': get_version()}, f)
        parameters_file = os.path.join(normpath(config.workdir, config.output_dir),
                                       'parameters.json')
        with open(parameters_file,'w') as f:
            json.dump(context.parameters.parameters, f)
        tmpcontext.add_files(parameters_file)

        # write list of created products
        with open(os.path.join(config.output_dir, "output.json"), 'w') as ff:
            json.dump(products, ff)


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
    start_message = "Running process_spectra {}".format(VERSION)
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
    config_save(config, "process_spectra_config.json")
    return main_method(config)


if __name__ == '__main__':
    main()
