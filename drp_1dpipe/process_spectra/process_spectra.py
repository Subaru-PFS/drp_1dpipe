import os
import json
import logging
import time
import argparse
import shutil
import traceback

from pylibamazed.ResultStoreOutput import ResultStoreOutput

from drp_1dpipe import VERSION
from drp_1dpipe.core.logger import init_logger
from drp_1dpipe.core.argparser import define_global_program_options, AbspathAction
from drp_1dpipe.core.utils import normpath, get_conf_path, config_update, config_save
from drp_1dpipe.process_spectra.config import config_defaults

from drp_1dpipe.core.utils import init_environ, normpath, TemporaryFilesSet
from drp_1dpipe.io.reader import read_spectrum, get_nb_valid_points
from drp_1dpipe.io.catalog import DirectoryTemplateCatalog, FitsTemplateCatalog
from drp_1dpipe.io.redshiftCandidates import RedshiftCandidates
from drp_1dpipe.io.lsf import CreateLSF
from drp_1dpipe.process_spectra.parameters import default_parameters
from pylibamazed.redshift import (CProcessFlowContext, CProcessFlow, CLog,
                                   CLogFileHandler, CRayCatalog,
                                  GlobalException, ParameterException,
                                  CTemplateCatalog, get_version)
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


def _process_spectrum(output_dir, spectrum_path, template_catalog,
                      gal_line_catalog, qso_line_catalog, param, user_param):

    try:
        spectrum = read_spectrum(spectrum_path)
    except Exception as e:
        traceback.print_exc()
        raise Exception("Spectrum loading error: {}".format(e))

    # proc_id = os.path.join(spectrum.GetName(), str(index))
    proc_id, ext = os.path.splitext(spectrum.GetName())

    try:
        ctx = CProcessFlowContext()
        parameter_store = ctx.LoadParameterStore(json.dumps(param))
    except GlobalException as e:
        raise Exception("Can't build parameter Store : {}".format(e.what()))
    except ParameterException as e:
        raise Exception("Can't build parameter Store : {}".format(e.what()))
    except Exception as e:
        raise Exception("Can't build parameter Store : {}".format(e))

    try:
        lsf = CreateLSF(param["LSF"]["LSFType"], parameter_store, param["calibrationDir"])
        spectrum.SetLSF(lsf)
    except GlobalException as e:
        raise Exception("Can't create LSF : {}".format(e.what()))
    except ParameterException as e:
        raise Exception("Can't create LSF : {}".format(e.what()))
    except Exception as e:
        raise Exception("Can't create LSF : {}".format(e))

    try:
        ctx.Init(spectrum,
                 template_catalog,
                 gal_line_catalog,
                 qso_line_catalog)
    except Exception as e:
        raise Exception("ProcessFlow init error : {}".format(e))

    pflow = CProcessFlow()
    try:
        pflow.Process(ctx)
    except Exception as e:
        raise Exception("Processing error : {}".format(e))

    try:
        output = ResultStoreOutput(None, ctx.GetResultStore(), param)
        param["enablelinemeassolve"] = "yes"
        param["enablegalaxysolve"] = "no"
        param["enablestellarsolve"] = "no"
        param["enableqsosolve"] = "no"
        param["linemeas"]["redshiftref"] = output.get_attribute("galaxy","model_parameters","Redshift",0)
        param["linemeas"]["linemeassolve"]["linemodel"]["velocityabsorption"] = output.get_attribute("galaxy",
                                                                                                     "model_parameters",
                                                                                                     "VelocityAbsorption",
                                                                                                     0)
        param["linemeas"]["linemeassolve"]["linemodel"]["velocityemission"] = output.get_attribute("galaxy",
                                                                                                   "model_parameters",
                                                                                                   "VelocityEmission",
                                                                                                   0)
        ctx.LoadParameterStore(json.dumps(param))
        pflow.Process(ctx)
    except Exception as e:
        raise Exception("Line Measurement Processing error : {}".format(e))

    try:
        param["enablegalaxysolve"] = "yes"
        param["enablelinemeassolve"] = "no" # temporary trick, waiting for correct API in 0.26

        output = ResultStoreOutput(None, ctx.GetResultStore(), param)
        output.object_results["linemeas"] = dict() # temporary trick, waiting for correct API in 0.26
        output.object_dataframes["linemeas"] = dict() # temporary trick, waiting for correct API in 0.26
        output.operator_results["linemeas"] = dict() # temporary trick, waiting for correct API in 0.26
        output.load_object_level("linemeas") # temporary trick, waiting for correct API in 0.26

        rc = RedshiftCandidates(output, spectrum_path, logger, user_param)
        rc.load_line_catalog(normpath(param['calibrationDir'],
                                      param["linemeas"]["linemeassolve"]["linemodel"]["linecatalog"]))
        logger.log(logging.INFO, "write fits")
        rc.write_fits(output_dir)
    except Exception as e:
        raise Exception("Failed to write fits result for spectrum "
                      "{} : {}".format(proc_id, e))


def load_line_catalog(calibration_dir, objectType, linemodel_params):
    if "linecatalog" not in linemodel_params:
        raise Exception("Incomplete parameter file, linemodelsolve.linemodel.linecatalog entry mandatory")
    line_catalog_file = linemodel_params["linecatalog"]
    line_catalog = CRayCatalog()
    line_catalog.Load(normpath(calibration_dir, line_catalog_file))
    line_catalog.ConvertVacuumToAir()

    return line_catalog


def _setup_pass(calibration_dir, parameters_file):

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
    if not os.path.exists(calibration_dir):
        raise FileNotFoundError(f"Calibration directory does not exist: "
                                f"{calibration_dir}")
    params['calibrationDir'] = calibration_dir

    # load line catalog
    if "linemodelsolve" in params["galaxy"]:
        linemodel_params = params["galaxy"]["linemodelsolve"]["linemodel"]
        gal_line_catalog = load_line_catalog(calibration_dir, "galaxy", linemodel_params)
    else:
        gal_line_catalog = CRayCatalog()

    if params["enableqsosolve"] == "yes" and "linemodelsolve" in params["qso"]:
        linemodel_params = params["qso"]["linemodelsolve"]["linemodel"]
        qso_line_catalog = load_line_catalog(calibration_dir, "QSO", linemodel_params)
    else:
        qso_line_catalog = CRayCatalog()

    medianRemovalMethod = params['templateCatalog']['continuumRemoval']['method']
    medianKernelWidth = float(params["templateCatalog"]["continuumRemoval"]["medianKernelWidth"])
    nscales = float(params["templateCatalog"]["continuumRemoval"]["decompScales"])
    dfBinPath = params["templateCatalog"]["continuumRemoval"]["binPath"]

    # Load galaxy templates
    if "template_dir" not in params["galaxy"]:
        raise Exception("Incomplete parameter file, template_dir entry mandatory")
    _template_dir = normpath(calibration_dir,  params["galaxy"]["template_dir"])

    if os.path.isfile(_template_dir):
        # template_dir is actually a file: load templates from FITS
        template_catalog = FitsTemplateCatalog(medianRemovalMethod,
                                               medianKernelWidth,
                                               nscales, dfBinPath)
    else:
        # template_dir a directory: load templates from calibration dirs
        template_catalog = DirectoryTemplateCatalog(medianRemovalMethod,
                                                    medianKernelWidth,
                                                    nscales, dfBinPath)
    logger.log(logging.INFO, "Loading %s" % _template_dir)

    try:
        template_catalog.Load(_template_dir)
    except Exception as e:
        logger.log(logging.CRITICAL, "Can't load template : {}".format(e))
        raise

    if params["enablestellarsolve"] == "yes":
        tdir = normpath(calibration_dir, params["star"]["template_dir"])
        template_catalog.Load(tdir)
    # Read QSO template catalog
    if params["enableqsosolve"] == "yes":
        tdir = normpath(calibration_dir, params["qso"]["template_dir"])
        template_catalog.Load(tdir)

    return params, gal_line_catalog, qso_line_catalog, template_catalog, user_params


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
    parameters_file = None
    if config.parameters_file:
        parameters_file = normpath(config.parameters_file)

    param, gal_line_catalog, qsol_line_catalog, template_catalog, user_param = _setup_pass(normpath(config.calibration_dir),
                                                                               parameters_file)
    with open(normpath(config.workdir, config.spectra_listfile), 'r') as f:
        spectra_list = json.load(f)

    outdir = normpath(config.workdir, config.output_dir)
    os.makedirs(outdir, exist_ok=True)

    data_dir = os.path.join(outdir, 'data')
    os.makedirs(data_dir, exist_ok=True)

    products = []
    for i, spectrum_path in enumerate(spectra_list):
        spectrum = normpath(config.workdir, config.spectra_dir, spectrum_path["fits"])
        nb_valid_points = get_nb_valid_points(spectrum)
        if nb_valid_points < 3000:
            logger.log(logging.WARNING,
                       "Invalid spectrum, only " + str(nb_valid_points) + " valid points, not processed")
            to_process = False
        else:
            to_process = True
        proc_id, ext = os.path.splitext(spectrum_path["fits"])
        spc_out_dir = os.path.join(outdir, proc_id )
        processed = False
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
                    _process_spectrum(data_dir, spectrum, template_catalog,
                                      gal_line_catalog,qsol_line_catalog, param, user_param)
                    processed = True
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
            json.dump(param,f)
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

    # set workdir environment
    init_environ(config.workdir)

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
