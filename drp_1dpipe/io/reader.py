import os.path
from astropy.io import fits
from pyamazed.redshift import (CSpectrumSpectralAxis,
                               CSpectrumFluxAxis_withError,
                               CSpectrum)
import numpy as np


def read_spectrum(path):
    """
    Read a pfsObject FITS file and build a CSpectrum out of it

    :param path: FITS file name
    :rtype: CSpectrum
    """

    with fits.open(path) as f:
        fluxtbl = f['FLUXTBL'].data
        error = np.sqrt(f['COVAR'].data[:,0])
        spectralaxis = CSpectrumSpectralAxis(fluxtbl['wavelength'] * 10)
        signal = CSpectrumFluxAxis_withError(fluxtbl['flux'], error)
        spectrum = CSpectrum(spectralaxis, signal)
        spectrum.SetName(os.path.basename(path))
    return spectrum
