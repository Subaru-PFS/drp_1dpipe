"""
File: drp_1dpipe/tests/test_utils.py

Created on: 31/10/18
Author: PSF DRP1D developers
"""

import pytest
import os
from collections import namedtuple
from tempfile import TemporaryDirectory
from drp_1dpipe.merge_results.merge_results import run
#from .utils import generate_fake_fits


redshift_header = "#Spectrum\tProcessingID\tRedshift\tMerit\tTemplate" \
                  "\tMethod\tDeltaz\tReliability\tsnrHa\tlfHa\tType\n"

redshift_template = "file.fits\tprocessing_id-{proc_id}\t{redshift}\t{merit}\t{template}" \
                    "\tChisquareLogSolve_0.00\t-1\tC6\t-1\tG\n"

def test_run():

    # generate fake result directories
    pass
    # TODO: rewrite complete test after including the merge feature

    # workdir =  TemporaryDirectory(prefix='pytest_')
    #
    #
    # args = namedtuple('Args', ['workdir', 'logdir', 'loglevel', 'result_dirs', 'spectra_path'])(
    #     workdir=workdir.name, logdir='logdir', loglevel='debug', spectra_path='spectra',
    #     result_dirs='output-*')
    #
    # os.makedirs(os.path.join(args.workdir, args.spectra_path), exist_ok=True)
    # generate_fake_fits(fileName=os.path.join(args.workdir, args.spectra_path, 'file.fits'))
    #
    # dirs = [TemporaryDirectory(prefix='output-', dir=args.workdir) for i in range(4)]
    # for i, d in enumerate(dirs):
    #     f = open(os.path.join(args.workdir, d.name, 'redshift.csv'), 'w')
    #     f.write(redshift_header)
    #     f.write(redshift_template.format(proc_id=1, redshift=0.234*i, merit=-0.011*i, template="foo"))
    #     f.write(redshift_template.format(proc_id=2, redshift=0.321*i, merit=-0.022*i, template="bar"))

    # TODO: actually test something
    # run(args)
