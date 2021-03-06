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
from drp_1dpipe.io.redshiftCandidates import RedshiftCandidates
from drp_1dpipe.process_spectra.parameters import default_parameters
from pylibamazed.redshift import (CProcessFlowContext, CProcessFlow, CLog,
                                  CParameterStore, CClassifierStore,
                                  CLogFileHandler, CRayCatalog,
                                  CTemplateCatalog, get_version)
import collections.abc


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
    parser.add_argument('--linemeas_parameters_file', metavar='FILE',
                        help='Parameters file used for line flux measurement. '
                        'Relative to workdir.')
    parser.add_argument('--lineflux', choices=['on', 'off', 'only'],
                        help='Whether to do line flux measurements.'
                        '"on" to do redshift and line flux calculations, '
                        '"off" to disable, '
                        '"only" to skip the redshift part.')
    parser.add_argument('--continue', action='store_true', dest='continue_',
                        help='Continue a previous processing.')
    parser.add_argument('--stellar', choices=['on', 'off', 'only'],
                        help='Whether to provide stellar results'
                        '"on" provide stellar results'
                        '"off" do not provide stellar results'
                        '"only" provide only stellar results')

    return parser


def _output_path(args, *path):
    return normpath(args.workdir, args.output_dir, *path)


def _process_spectrum(output_dir, index, spectrum_path, template_catalog,
                      line_catalog, param, classif, save_results,param_dict):

    try:
        spectrum = read_spectrum(spectrum_path)
    except Exception as e:
        traceback.print_exc()
        raise Exception("Spectrum loading error: {}".format(e))

    # proc_id = os.path.join(spectrum.GetName(), str(index))
    proc_id, ext = os.path.splitext(spectrum.GetName())

    try:
        ctx = CProcessFlowContext()
        ctx.Init(spectrum,
                 proc_id,
                 template_catalog,
                 line_catalog,
                 param,
                 classif)
    except Exception as e:
        raise Exception("ProcessFlow init error : {}".format(e))

    pflow = CProcessFlow()
    try:
        pflow.Process(ctx)
    except Exception as e:
        raise Exception("Processing error : {}".format(e))

    try:
        output = ResultStoreOutput(None, ctx.GetResultStore(), param_dict)
        rc = RedshiftCandidates(output, spectrum_path)
        rc.write_fits(output_dir)
    except Exception as e:
        raise Exception("Failed to write fits result for spectrum "
                      "{} : {}".format(proc_id, e))
#        traceback.print_exc()


def _setup_pass(calibration_dir, default_parameters_file, parameters_file):

    # setup parameter store
    param = CParameterStore()
    _params = default_parameters.copy()
    try:
        # override default parameters with those found in parameters_file
        with open(default_parameters_file, 'r') as f:
            _params = update(_params, json.load(f))
    except Exception as e:
        logger.log(logging.ERROR,
                   f'unable to read default parameter file : {e}')
        raise
    if parameters_file:
        try:
            # override default parameters with those found in parameters_file
            with open(parameters_file, 'r') as f:
                _params = update(_params, json.load(f))
        except Exception as e:
            logger.log(logging.INFO,
                       f'unable to read parameter file : {e}, using defaults')
            raise
    param.FromString(json.dumps(_params))

    # setup calibration dir
    if not os.path.exists(calibration_dir):
        raise FileNotFoundError(f"Calibration directory does not exist: "
                                f"{calibration_dir}")
    param.Set_String('calibrationDir', calibration_dir)

    # load line catalog
    line_catalog = CRayCatalog()
    line_catalog_file = normpath(os.path.join(calibration_dir, _params["linecatalog"]))
    if not os.path.exists(line_catalog_file):
        raise FileNotFoundError(f"Line catalog file not found: "
                                f"{line_catalog_file}")
    logger.log(logging.INFO, "Loading %s" % line_catalog_file)
    line_catalog.Load(line_catalog_file)
    line_catalog.ConvertVacuumToAir()

    medianRemovalMethod = param.Get_String('templateCatalog.continuumRemoval.'
                                           'method', 'IrregularSamplingMedian')
    opt_medianKernelWidth = param.Get_Float64('templateCatalog.'
                                              'continuumRemoval.'
                                              'medianKernelWidth')
    opt_nscales = param.Get_Float64('templateCatalog.continuumRemoval.'
                                    'decompScales',
                                    8.0)
    dfBinPath = param.Get_String('templateCatalog.continuumRemoval.binPath',
                                 'absolute_path_to_df_binaries_here')
    template_catalog = CTemplateCatalog(medianRemovalMethod,
                                        opt_medianKernelWidth,
                                        opt_nscales, dfBinPath)
    template_catalog_path = normpath(os.path.join(calibration_dir, param.Get_String('template_dir')))
    logger.log(logging.INFO, "Loading %s" % template_catalog_path)

    try:
        template_catalog.Load(template_catalog_path)
    except Exception as e:
        logger.log(logging.CRITICAL, "Can't load template : {}".format(e))
        raise
    if _params["enablestellarsolve"] == "yes" or _params["enableqsosolve"] == "yes":
        qs_config = {}
        path_file = normpath(calibration_dir, "calibration-config.txt")
        if not os.path.exists(path_file):
            raise FileNotFoundError("Template file not found : {}".format(path_file))
        else:
            with open(path_file) as ff:
                for line in ff:
                    if not line.startswith('#'):
                        k, v = line.strip().split("=")
                        qs_config[k] = v
        # Read Star template catalog
        if _params["enablestellarsolve"] == "yes":
            tdir = normpath(calibration_dir, qs_config["star-templates-dir"])
            template_catalog.Load(tdir)
        # Read QSO template catalog
        if _params["enableqsosolve"] == "yes":
            tdir = normpath(calibration_dir, qs_config["qso-templates-dir"])
            template_catalog.Load(tdir)
    return param,_params, line_catalog,template_catalog


def amazed(config):
    """Run the full-featured amazed client

    Parameters
    ----------
    config : :obj:`Config`
        Configuration object
    """

    zlog = CLog()
    logFileHandler = CLogFileHandler(zlog, os.path.join(config.logdir,
                                                        'amazed.log'))
    logFileHandler.SetLevelMask(_map_loglevel[config.log_level])

    #
    # Set up param and linecatalog for redshift pass
    #
    parameters_file = None
    if config.parameters_file:
        parameters_file = normpath(config.parameters_file)

    param, param_dict, line_catalog, template_catalog = _setup_pass(normpath(config.calibration_dir),
                                                  normpath(config.default_parameters_file),
                                                  parameters_file)


    #
    # Set up param and linecatalog for line measurement pass
    #
    linemeas_parameters_file = None
    if config.linemeas_parameters_file:
        linemeas_parameters_file = normpath(config.linemeas_parameters_file)

    linemeas_param, linemeas_params_dict, linemeas_line_catalog, linemeas_template_catalog= \
        _setup_pass(normpath(config.calibration_dir),
                    normpath(config.default_linemeas_parameters_file),
                    linemeas_parameters_file)

    classif = CClassifierStore()

    with open(normpath(config.workdir, config.spectra_listfile), 'r') as f:
        spectra_list = json.load(f)

    outdir = normpath(config.workdir, config.output_dir)
    os.makedirs(outdir, exist_ok=True)

    data_dir = os.path.join(outdir, 'data')
    os.makedirs(data_dir, exist_ok=True)

    outdir_linemeas = None
    if config.lineflux in ['only', 'on']:
        outdir_linemeas = '-'.join([outdir, 'lf'])
        os.makedirs(outdir_linemeas, exist_ok=True)

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
        if config.lineflux != 'only' and to_process:
            # first step : compute redshift
            to_process = True

            if os.path.exists(spc_out_dir):
                if config.continue_:
                    to_process = False
                else:
                    shutil.rmtree(spc_out_dir)
            if to_process:
                try:
                    _process_spectrum(data_dir, i, spectrum, template_catalog,
                                      line_catalog, param, classif, 'all',param_dict)
                    processed = True
                except Exception as e:
                    logger.log(logging.ERROR,"Could not process spectrum: {}".format(e))
        if config.lineflux in ['only', 'on'] and processed:
            # second step : compute line fluxes
            try:
                to_process_lin = True
                spc_out_lin_dir = os.path.join(outdir_linemeas, proc_id)
                if os.path.exists(spc_out_lin_dir):
                    if config.continue_:
                        to_process_lin = False
                    else:
                        shutil.rmtree(spc_out_lin_dir)
                if to_process_lin:
                    linemeas_param.Set_String('linemeascatalog',
                                        os.path.join(outdir, 'redshift.csv'))
                    _process_spectrum(outdir_linemeas, i, spectrum,
                                              template_catalog,
                                              linemeas_line_catalog, linemeas_param,
                                              classif, 'linemeas',param_dict)
            except Exception as e:
                logger.log(logging.CRITICAL, "Could not process linemeas: {}".format(e))
                spc_out_lin_dir = None
        else:
            spc_out_lin_dir = None
#        products.append(result.write(data_dir))

    with TemporaryFilesSet(keep_tempfiles=config.log_level <= logging.INFO) as tmpcontext:

        # save amazed version and parameters file to output dir
        version_file = _output_path(config, 'version.json')
        with open(version_file, 'w') as f:
            json.dump({'amazed-version': get_version()}, f)
        parameters_file = os.path.join(normpath(config.workdir, config.output_dir),
                                       'parameters.json')
        param.Save(parameters_file)
        tmpcontext.add_files(parameters_file)

        # create output products
        # results = AmazedResults(_output_path(config), normpath(config.workdir,
        #                                                      config.spectra_dir),
        #                         config.lineflux in ['only', 'on'],
        #                         tmpcontext=tmpcontext)
        # products = results.write()

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
