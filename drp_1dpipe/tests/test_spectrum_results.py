import pytest
import os
import json
import tempfile
import numpy as np


from pfs.datamodel.drp import PfsObject
from pfs.datamodel.masks import MaskHelper
from pfs.datamodel.target import Target
from pfs.datamodel.observations import Observations

def create_pfsobject(wavelength, directory):
    target = Target(catId=0, tract=1, patch='2,2', objId=3, ra=0.0, dec=0.0)
    observations = Observations(np.array([0]),
        np.array(['n']),
        np.array([0]),
        np.array([0]),
        np.array([0]),
        np.array([(0.0,0.0)]),
        np.array([(0.0,0.0)]))

    obj = PfsObject(target=target,
                observations=observations,
                wavelength=wavelength,
                flux=np.ones(2),
                mask=np.zeros(2),
                sky=np.zeros(2),
                covar=np.array([np.ones(2), np.zeros(2), np.zeros(2)]),
                covar2=np.zeros((2, 2)),
                flags=MaskHelper())
    obj.write(dirName=directory)
    return obj.filenameFormat%obj.getIdentity()

def test_spectrum_results():
    pass

def test_candidates():
    pass

def test_zpdf():
    pass

def test_lambda_ranges():
    pass

def test_lines():
    pass

def test_classification():
    pass

def test_models():
    pass

def test_redshift():
    pass

def test_parse_pfsObjectName():
    name = 'pfsObject-999-96321-P,P-0000000000001234-745-0x0000000000000045.fits'
 #   c, t, p, o, v, h = SpectrumResults._parse_pfsObject_name(name)
#    assert c == 999
#    assert t == 96321
#    assert p == "P,P"
#    assert o == 4660
#    assert v == 745
#    assert h == 69
