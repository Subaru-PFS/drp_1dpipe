import pytest
from tempfile import NamedTemporaryFile
from tempfile import TemporaryDirectory
import numpy as np

from drp_1dpipe.io.reader import read_spectrum
from drp_1dpipe.io.writer import write_candidates
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
    fd = TemporaryDirectory()
    fname = write_candidates(fd.name, 0, 1, '1,1', 2, 3, 4, [], [], [], [], np.array([]), [], '')
    assert fname == 'pfsZcandidates-000-00001-1,1-0000000000000002-003-0x0000000000000004.fits'