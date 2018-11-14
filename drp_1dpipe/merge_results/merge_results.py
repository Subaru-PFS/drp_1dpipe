"""
File: drp_1dpipe/merge_results/merge_results.py

Created on: 01/11/18
Author: CeSAM
"""

import os.path
import glob
import argparse
from collections import namedtuple
from drp_1dpipe.io.utils import init_logger, get_args_from_file, normpath, init_argparse


logger = logging.getLogger("process_spectra")

def main():
    """
    merge_results entry point.

    Parse command line arguments, and call the run() function.
    """

    parser = init_argparse()
    parser.add_argument('--spectra_path', metavar='DIR',
                        help='Base path where to find spectra. Relative to workdir')
    parser.add_argument('--result_dirs', metavar='DIR',
                        help='Shell glob pattern of directories where to find result files. Relative to workdir')

    args = parser.parse_args()
    get_args_from_file("merge_results.conf", args)

    return run(args)

RedshiftResult = namedtuple('RedshiftResult', ['spectrum', 'processingid', 'redshift', 'merit',
                                               'template', 'method', 'deltaz', 'reliability',
                                               'snrha',
                                               #'lfha',  # TODO: missing ?
                                               'type_'])
def _list_redshifts(lines):
    results = []
    for l in lines:
        if not l or l.startswith('#'):
            continue
        result = RedshiftResult(*l.split())
        results.append(result)
    return results

def _update_pfsobject(spectra_path, redshift):
    with open(os.path.join(spectra_path, redshift.spectrum)) as f:
        print("Updating {} with redshift {}".format(f.name, redshift.redshift))

def run(args):

    # initialize logger
    init_logger("merge_results", args.logdir, args.loglevel)

    redshifts = []

    for res_dir in glob.glob(normpath(args.workdir, args.result_dirs)):
        with open(os.path.join(res_dir, 'redshift.csv'), 'r') as f:
            redshifts.extend(_list_redshifts([l.strip() for l in f.readlines()]))

    for r in redshifts:
        _update_pfsobject(normpath(args.workdir, args.spectra_path), r)

    return 0
