"""
File: drp_1dpipe/tests/test_pre_process.py

Created on: 25/10/18
Author: PSF DRP1D developers
"""

import os
import math
import pytest
import logging
import tempfile
from drp_1dpipe.io.utils import init_logger
from drp_1dpipe.pre_process.pre_process import run, bunch

spectra_path = "/tmp/pfsssss/"


class FakeArgs(object):
    def __init__(self):
        """
        The "test_main" function.

        This function test the "bunch" function of "pre_process.py" module.
        """
        self.workdir = tempfile.mkdtemp(suffix=None, prefix=None, dir=None)
        self.logdir = tempfile.mkdtemp(suffix=None, prefix=None, dir=None)
        self.loglevel = 10
        self.bunch_size = 4
        self.spectra_path = tempfile.mkdtemp(suffix=None, prefix=None, dir=None)
        self.bunch_list = tempfile.NamedTemporaryFile(dir=spectra_path)


def test_run():
    """
    The "test_main" function.

    This function test the "bunch" function of "pre_process.py" module.
    """
    args = FakeArgs()


    result = run(args)
    assert result is not None


def test_bunch():
    """
    The "test_bunch" function.

    This function test the "bunch" function of "pre_process.py" module.
    """

    d = {}
    bunch_size = 4
    spectra_path = tempfile.mkdtemp(suffix=None, prefix=None, dir=None)

    for f in range(0, 9):
        d["{0}".format(f)] = tempfile.NamedTemporaryFile(dir=spectra_path)

    res = [b for b in bunch(bunch_size, spectra_path)]

    assert res is not None
    assert len(res) == math.ceil(len(os.listdir(spectra_path))/bunch_size)
