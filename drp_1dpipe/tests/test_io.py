import pytest
from tempfile import NamedTemporaryFile
from tempfile import TemporaryDirectory
import numpy as np

from drp_1dpipe.io.reader import read_spectrum
#from .utils import generate_fake_fits, NROW


def test_reader():
    """
    This function test features concerning pfsObject reader.
    """
    # TODO: datamodel is using deprecated pyfits module. Rewrite the complete
    # test after datamodel update.
    pass
    # filename = NamedTemporaryFile()
    # generate_fake_fits(fileName=filename.name)
    # spectrum = read_spectrum(filename.name)
    # assert spectrum.GetSpectralAxis().GetSamplesCount() == NROW
    # assert spectrum.GetFluxAxis().GetSamplesCount() == NROW
    # filename.close()


def test_writer():
    pass
