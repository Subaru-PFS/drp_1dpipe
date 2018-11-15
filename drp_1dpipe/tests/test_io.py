"""
File: drp_1dpipe/tests/test_utils.py

Created on: 31/10/18
Author: PSF DRP1D developers
"""

import pytest
from tempfile import NamedTemporaryFile
from drp_1dpipe.io.reader import read_spectrum
from .utils import generate_fake_fits, NROW


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
