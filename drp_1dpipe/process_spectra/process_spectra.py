"""
File: drp_1dpipe/process_spectra/process_spectra.py

Created on: 01/11/18
Author: CeSAM
"""
import os.path
import json
import logging
import time
from drp_1dpipe.io.utils import init_logger, get_args_from_file, normpath, \
    init_argparse, get_auxiliary_path
from drp_1dpipe.io.reader import read_spectrum
from pyamazed.redshift import CProcessFlowContext, \
    CProcessFlow, CLog, CParameterStore, CClassifierStore, \
    CLogFileHandler, CRayCatalog, CTemplateCatalog, get_version
from drp_1dpipe.process_spectra.results import AmazedResults


MULTIPROC = False

if MULTIPROC:
    import concurrent.futures

logger = logging.getLogger("process_spectra")


def _output_path(args, *path):
    return normpath(args.workdir, args.output_dir, *path)


def main():
    """
    process_spectra entry point.

    Parse command line arguments, and call the run() function.
    """

    parser = init_argparse()
    parser.add_argument('--spectra_path', metavar='DIR',
                        help='Path where spectra are stored. '
                        'Relative to workdir.')
    parser.add_argument('--spectra_listfile', metavar='FILE',
                        help='JSON file holding a list of files of '
                        'astronomical objects.')
    parser.add_argument('--calibration_dir', metavar='DIR',
                        help='Specify directory in which calibration files are'
                        ' stored. Relative to workdir.')
    parser.add_argument('--parameters_file', metavar='FILE',
                        default=get_auxiliary_path("parameters.json"),
                        help='Parameters file. Relative to workdir.')
    parser.add_argument('--template_dir', metavar='DIR',
                        help='Specify directory in which input templates files'
                        'are stored.')
    parser.add_argument('--linecatalog', metavar='FILE',
                        help='Path to the rest lines catalog file.')
    parser.add_argument('--zclassifier_dir', metavar='DIR',
                        help='Specify directory in which zClassifier files are'
                        ' stored.')
    parser.add_argument('--process_method',
                        help='Process method to use. Whether Dummy or Amazed.')
    parser.add_argument('--output_dir', metavar='DIR',
                        help='Directory where all generated files are going to'
                        ' be stored. Relative to workdir.')
    parser.add_argument('--linemeas_parameters_file', metavar='FILE',
                        default=get_auxiliary_path("linemeas-parameters.json"),
                        help='Parameters file used for line measurement. '
                        'Relative to workdir.')
    parser.add_argument('--linemeas_linecatalog', metavar='FILE',
                        help='Path to the rest lines catalog file used for '
                        'line measurement.')
    args = parser.parse_args()
    get_args_from_file("process_spectra.conf", args)

    # Start the main program
    return run(args)


def _process_spectrum(output_dir, index, spectrum_path, template_catalog,
                      line_catalog, param, classif, save_results):
    try:
        spectrum = read_spectrum(spectrum_path)
    except Exception as e:
        logger.log(logging.ERROR, "Can't load spectrum : {}".format(e))
        return

    proc_id = '{}-{}'.format(spectrum.GetName(), index)

    try:
        ctx = CProcessFlowContext()
        ctx.Init(spectrum,
                 proc_id,
                 template_catalog,
                 line_catalog,
                 param,
                 classif)
    except Exception as e:
        logger.log(logging.ERROR, "Can't init process flow : {}".format(e))

    pflow = CProcessFlow()
    try:
        pflow.Process(ctx)
    except Exception as e:
        logger.log(logging.ERROR, "Can't process : {}".format(e))

    if save_results == 'all':
        ctx.GetDataStore().SaveRedshiftResult(output_dir)
        ctx.GetDataStore().SaveAllResults(os.path.join(output_dir, proc_id),
                                          'all')
    elif save_results == 'linemeas':
        ctx.GetDataStore().SaveAllResults(os.path.join(output_dir, proc_id),
                                          'linemeas')
    else:
        raise Exception("Unhandled save_results {}".format(save_results))


_map_loglevel = {'CRITICAL': CLog.nLevel_Critical,
                 'FATAL': CLog.nLevel_Critical,
                 'ERROR': CLog.nLevel_Error,
                 'WARNING': CLog.nLevel_Warning,
                 'WARN': CLog.nLevel_Warning,
                 'INFO': CLog.nLevel_Info,
                 'DEBUG': CLog.nLevel_Debug,
                 'NOTSET': CLog.nLevel_None}


def _setup_pass(calibration_dir, parameters_file, line_catalog_file):

    # setup parameter store
    param = CParameterStore()
    if not os.path.exists(parameters_file):
        raise FileNotFoundError(f"Parameter file not found: {parameters_file}")
    try:
        param.Load(parameters_file)
    except Exception as e:
        print("Unable to read parameter file :", e)

    # setup calibration dir
    if not os.path.exists(calibration_dir):
        raise FileNotFoundError(f"Calibration directory does not exist: "
                                f"{calibration_dir}")
    param.Set_String('calibrationDir', calibration_dir)

    # load line catalog
    line_catalog = CRayCatalog()
    if not os.path.exists(line_catalog_file):
        raise FileNotFoundError(f"Line catalog file not found: "
                                f"{line_catalog_file}")
    logger.log(logging.INFO, "Loading %s" % line_catalog_file)
    line_catalog.Load(line_catalog_file)
    line_catalog.ConvertVacuumToAir()

    return param, line_catalog


def amazed(args):
    """Run the full-featured amazed client"""

    zlog = CLog()
    logFileHandler = CLogFileHandler(zlog, os.path.join(args.logdir,
                                                        'amazed.log'))
    logFileHandler.SetLevelMask(_map_loglevel[args.loglevel.upper()])

    #
    # Set up param and linecatalog for redshift pass
    #
    param, line_catalog = _setup_pass(normpath(args.calibration_dir),
                                      normpath(args.parameters_file),
                                      normpath(args.linecatalog))
    medianRemovalMethod = param.Get_String("continuumRemoval.method",
                                           "IrregularSamplingMedian")
    opt_medianKernelWidth = param.Get_Float64('continuumRemoval.'
                                              'medianKernelWidth',
                                              75.0)
    opt_nscales = param.Get_Float64("continuumRemoval.decompScales",
                                    8.0)
    dfBinPath = param.Get_String("continuumRemoval.binPath",
                                 "absolute_path_to_df_binaries_here")

    #
    # Set up param and linecatalog for line measurement pass
    #
    linemeas_param, linemeas_line_catalog = \
        _setup_pass(normpath(args.calibration_dir),
                    normpath(args.linemeas_parameters_file),
                    normpath(args.linemeas_linecatalog))

    classif = CClassifierStore()

    if args.zclassifier_dir:
        zclassifier_dir = normpath(args.workdir, args.zclassifier_dir)
        if not os.path.exists(zclassifier_dir):
            raise FileNotFoundError(f"zclassifier directory does not exist: "
                                    f"{zclassifier_dir}")
        classif.Load(zclassifier_dir)

    with open(normpath(args.workdir, args.spectra_listfile), 'r') as f:
        spectra_list = json.load(f)

    template_catalog = CTemplateCatalog(medianRemovalMethod,
                                        opt_medianKernelWidth,
                                        opt_nscales, dfBinPath)
    logger.log(logging.INFO, "Loading %s" % args.template_dir)

    try:
        template_catalog.Load(normpath(args.template_dir))
    except Exception as e:
        logger.log(logging.CRITICAL, "Can't load template : {}".format(e))
        raise

    for i, spectrum_path in enumerate(spectra_list):
        outdir = normpath(args.workdir, args.output_dir)
        spectrum = normpath(args.workdir, args.spectra_path, spectrum_path)
        if MULTIPROC:
            futures = []
            with concurrent.futures.ProcessPoolExecutor(max_workers=4) as ex:
                futures.append(ex.submit(_process_spectrum,
                                         i, args, spectrum_path,
                                         template_catalog,
                                         line_catalog, param, classif))
        else:
            # first pass : compute redshift
            _process_spectrum(outdir, i, spectrum, template_catalog,
                              line_catalog, param, classif, 'all')

            # second pass : compute line fluxes
            linemeas_param.Set_String('linemeascatalog',
                                      os.path.join(outdir, 'redshift.csv'))
            _process_spectrum('-'.join([outdir, 'lf']), i, spectrum,
                              template_catalog,
                              linemeas_line_catalog, linemeas_param, classif,
                              'linemeas')

    if MULTIPROC:
        concurrent.futures.wait(futures)

    # save cpf-redshift version in output dir
    with open(_output_path(args, 'version.json'), 'w') as f:
        json.dump({'cpf-redshift-version': get_version()}, f)

    # create output products
    results = AmazedResults(_output_path(args), normpath(args.workdir,
                                                         args.spectra_path))
    results.write()


def dummy(args):
    """A dummy client, for pipeline testing purpose."""
    logger.log(logging.INFO, "running dummy client {}".format(args))
    time.sleep(10)
    print("done")


def run(args):

    # initialize logger
    init_logger("process_spectra", args.logdir, args.loglevel)

    if args.process_method.lower() == 'amazed':
        amazed(args)
    elif args.process_method.lower() == 'dummy':
        dummy(args)
    else:
        raise "Unknown process_method {}".format(args.process_method)

    return 0
