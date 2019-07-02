"""
File: drp_1dpipe/tests/test_pre_process.py

Created on: 25/10/18
Author: PSF DRP1D developers
"""

import os
import json
import pytest
import collections
from tempfile import TemporaryDirectory
from tempfile import NamedTemporaryFile
from drp_1dpipe.io.utils import normpath
from drp_1dpipe.pre_process.pre_process import run, bunch


def compare(a, b):
    """
    Allows to compare unordered collection.

    This function is used by test_run() to assert that two lists are equal.

    :param a: First list
    :param b: Second list
    :return: Return True if equality exists.
    """
    return collections.Counter(a) == collections.Counter(b)

class FakeArgs(object):
    def __init__(self, workdir):
        """
        The "test_main" function.

        This function test the "bunch" function of "pre_process.py" module.
        """
        self.workdir = workdir
        self.logdir = workdir
        self.spectra_dir = os.path.join(workdir, 'spectra')
        os.mkdir(self.spectra_dir)
        self.bunch_list = os.path.join(workdir, 'bunch_list.json')
        self.loglevel = 'debug'
        self.bunch_size = '4'


def test_run():
    """
    The "test_run" function.

    This function test the "run" function of "pre_process.py" module.
    """

    workdir = TemporaryDirectory()
    args = FakeArgs(workdir.name)

    fits_file = []
    for i in range(9):
        with open(normpath(args.spectra_dir, '{}.fits'.format(i)), 'w') as f:
            fits_file.append(os.path.basename(f.name))

    result_run = run(args)

    assert os.path.isfile(os.path.join(args.workdir, args.bunch_list))

    with open(os.path.join(args.workdir, args.bunch_list), 'r') as f:
        data = json.load(f)
        total = []
        for e in data:
            with open(e, 'r') as ff:
                datal = json.load(ff)
                total.append(datal)

    assert len(total) == 3
    assert len(total[0]) == 4
    assert len(total[1]) == 4
    assert len(total[2]) == 1

    flat_list = [item for sublist in total for item in sublist]
    assert compare(flat_list, fits_file)


def test_bunch():
    """
    The "test_bunch" function.

    This function test the "bunch" function of "pre_process.py" module.
    """

    d = {}
    bunch_size = 4
    _spectra_path = TemporaryDirectory()
    spectra_path = _spectra_path.name

    for f in range(9):
        d["{0}".format(f)] = NamedTemporaryFile(dir=spectra_path)

    res = [b for b in bunch(bunch_size, spectra_path)]

    assert res is not None
    assert len(res) == 3
    assert len(res[0]) == 4
    assert len(res[1]) == 4
    assert len(res[2]) == 1
