"""
File: drp_1dpipe/process_spectra/process_spectra.py

Created on: 01/11/18
Author: CeSAM
"""

import os.path
import json
import logging
from drp_1dpipe.io.reader import read_spectrum
from redshift import *

logger = logging.getLogger("process_spectra")

def define_program_options():
    """
    The "define_program_options" function.
    """

    parser = argparse.ArgumentParser()
    # mandatory arguments
    parser.add_argument('--workdir', type=str, required=True,
                        help='The root working directory where data is located')
    parser.add_argument('--logdir', type=str, required=False,
                        help='The logging directory')
    parser.add_argument('--loglevel', type=str, required=False,
                        help='The logging level')

    # specific arguments
    parser.add_argument('--spectra_listfile', metavar='FILE', type=str,
                        help='JSON file holding a list of files of astronomical objects')
    parser.add_argument('--calibration_dir', metavar='DIR', type=str,
                        help='Specify directory in which calibration files are stored')
    parser.add_argument('--parameters_file', metavar='FILE', type=str,
                        help='Parameters file. Relative to calibration_dir')
    parser.add_argument('--template_dir', metavar='DIR', type=str,
                        help='Specify directory in which input templates files are stored. Relative to calibration_dir')
    parser.add_argument('--linecatalog', metavar='FILE', type=str,
                        help='Path to the rest lines catalog file. Relative to calibration_dir')
    parser.add_argument('--zclassifier_dir', metavar='DIR', type=str,
                        help='Specify directory in which zClassifier files are stored. Relative to calibration_dir')
    parser.add_argument('--process_method', type=str, required=True,
                        help='Process method to use. Whether Dummy or Amazed')
    parser.add_argument('--output_dir', metavar='DIR', type=str,
                        help='Directory where all generated files are going to be stored')
    args = parser.parse_args()
    get_args_from_file("process_spectra.conf", args)

    # Initialize logger
    init_logger("process_spectra", args.logdir, args.loglevel)

    # Start the main program
    main(args)

def _calibration_path(args, *path):
    return os.path.normpath(os.path.expanduser(os.path.join(args.workdir,
                                                            args.calibration_dir,
                                                            *path)))

def _spectrum_path(config, *path):
    return os.path.normpath(os.path.expanduser(os.path.join(args.workdir,
                                                            args.spectrum_dir,
                                                            *path)))
def _output_path(config, *path):
    return os.path.normpath(os.path.expanduser(os.path.join(args.workdir,
                                                            args.output_dir,
                                                            *path)))

def _process_spectrum(index, args, spectrum_path, template_catalog, line_catalog, param, classif):
    try:
        spectrum = read_spectrum(_spectrum_path(args, spectrum_path))
    except Exception as e:
        logger.log(logging.CRITICAL, "Can't load spectrum : {}".format(e))
        continue

    proc_id = '{}-{}'.format(spectrum.GetName(), i)

    range = TFloat64Range(3800, 12600)
    done, mean, std = spectrum.GetMeanAndStdFluxInRange(range)
    logger.log(logging.DEBUG, 'Spectrum stats: {}, {}, {}'.format(done, mean, std))

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
        continue

    pflow=CProcessFlow()
    try:
        pflow.Process(ctx)
    except Exception as e:
        logger.log(logging.ERROR, "Can't process : {}".format(e))
        continue

    ctx.GetDataStore().SaveRedshiftResult(args.output_folder)
    ctx.GetDataStore().SaveAllResults(output_path(args, proc_id), 'all')

def amazed(args):
    """Run the full-featured amazed client"""

    param = CParameterStore()
    param.Load(os.path.normpath(os.path.expanduser(args.parameters_file)))
    update_paramstore(param, args)
    opt_saveIntermediateResults = param.Get_String('SaveIntermediateResults', 'all')

    classif = CClassifierStore()

    if args.zclassifier_dir:
        classif.Load(_calibration_path(args, args.zclassifier_dir))

    with open(os.path.normpath(os.path.expanduser(args.spectra_listfile)), 'r') as f:
        spectra_list = json.load(f)

    retcode, medianRemovalMethod = param.Get_String("continuumRemoval.method",
                                                    "IrregularSamplingMedian")
    assert retcode

    retcode, opt_medianKernelWidth = param.Get_Float64("continuumRemoval.medianKernelWidth", 75.0)
    assert retcode

    retcode, opt_nscales = param.Get_Float64("continuumRemoval.decompScales", 8.0)
    assert retcode

    retcode, dfBinPath = param.Get_String("continuumRemoval.binPath",
                                          "absolute_path_to_df_binaries_here")
    assert retcode

    template_catalog = CTemplateCatalog(medianRemovalMethod, opt_medianKernelWidth,
                                        opt_nscales, dfBinPath)
    logger.log(logging.INFO, "Loading %s" % args.template_dir)

    try:
        template_catalog.Load(_calibration_path(args, args.template_dir))
    except Exception as e:
        logger.log(logging.CRITICAL, "Can't load template : {}".format(e))
        raise

    line_catalog = CRayCatalog()
    logger.log(logging.INFO, "Loading %s" % args.linecatalog)
    line_catalog.Load(_calibration_path(args, args.linecatalog))

    for i, spectrum_path in enumerate(spectra_list):
        _process_spectrum(i, args, spectrum_path, template_catalog, line_catalog, param, classif)

    # save cpf-redshift version in output dir
    with open(output_path(args, 'version.json'), 'w') as f:
        json.dump({'cpf-redshift-version': get_version()}, f)

def dummy(args):
    """A dummy client, for pipeline testing purpose"""
    pass

def main(args):
    if args.process_method.lower() == 'amazed':
        amazed(args)
    elif args.process_method.lower() == 'dummy':
        dummy(args)
    else:
        raise "Unknown process_method {}".format(args.process_method)
