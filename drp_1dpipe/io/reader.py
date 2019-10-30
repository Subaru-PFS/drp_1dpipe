import os.path
from pfs.datamodel.drp import PfsObject
from pylibamazed.redshift import (CSpectrumSpectralAxis,
                                  CSpectrumFluxAxis_withError,
                                  CSpectrum)
import numpy as np


def read_spectrum(path):
    """
    Read a pfsObject FITS file and build a CSpectrum out of it

    :param path: FITS file name
    :rtype: CSpectrum
    """

    obj = PfsObject.readFits(path)
    wavelength = obj.wavelength
    flux = obj.flux
    error = np.sqrt(obj.covar[0][0:])
    spectralaxis = CSpectrumSpectralAxis(wavelength * 10)
    signal = CSpectrumFluxAxis_withError(flux, error)
    spectrum = CSpectrum(spectralaxis, signal)
    spectrum.SetName(os.path.basename(path))
    return spectrum
