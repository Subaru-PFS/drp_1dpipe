import fitsio
import os.path
from pyamazed.redshift import CSpectrumSpectralAxis, CSpectrumFluxAxis_withError, \
    CSpectrum
import numpy as np


def read_spectrum(path):
    """
    Read a pfsObject FITS file and build a CSpectrum out of it

    :param path: FITS file name
    :rtype: CSpectrum
    """
    fits = fitsio.FITS(path)
    data = fits['FLUXTBL']
    error = np.sqrt(fits['COVAR'][:,0].flatten())
    spectralaxis = CSpectrumSpectralAxis(data['wavelength'][:]*10)
    signal = CSpectrumFluxAxis_withError(data['flux'].read() * 1, error)
    spectrum = CSpectrum(spectralaxis, signal)
    spectrum.SetName(os.path.basename(path))
    return spectrum
