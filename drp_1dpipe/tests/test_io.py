"""
File: drp_1dpipe/tests/test_utils.py

Created on: 31/10/18
Author: PSF DRP1D developers
"""

import numpy as np
import pytest
import os
from tempfile import NamedTemporaryFile
from drp_1dpipe.io.reader import read_spectrum
from pfs.datamodel.pfsObject import PfsObject

NROW = 11640
NCOARSE = 10


def generate_fake_fits(fileName=None):
    obj = PfsObject(tract=99999, patch="0,0", objId=0, catId=0, visits=[], pfsConfigIds=[],
                    nVisit=None)
    obj.lam = np.array(range(NROW))/NROW*(1260-380)+380
    obj.flux = np.random.rand(NROW, 1)
    obj.fluxTbl.lam = np.array(range(NROW))/NROW*(1260-380)+380
    obj.fluxTbl.flux = np.random.rand(NROW)
    obj.fluxTbl.fluxVariance = np.random.rand(NROW)
    obj.fluxTbl.mask = np.zeros(NROW)
    obj.mask = np.zeros(NROW)
    obj.sky = np.zeros(NROW)
    obj.covar = np.random.rand(NROW, 3)
    obj.covar2 = np.random.rand(NCOARSE, NCOARSE)
    obj.visits = []
    obj.pfsConfigIds = []
    obj.write(fileName=fileName)

def test_reader():
    """
    This function test features concerning pfsObject reader.
    """
    filename = NamedTemporaryFile()
    generate_fake_fits(fileName=filename.name)
    spectrum = read_spectrum(filename.name)
    assert spectrum.GetSpectralAxis().GetSamplesCount() == NROW
    assert spectrum.GetFluxAxis().GetSamplesCount() == NROW
    filename.close()
