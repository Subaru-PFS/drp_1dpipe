import fitsio
import os.path
from pyamazed.redshift import *
import numpy as np

def read_spectrum(path):
    """
    Read a pfsObject FITS file and build a CSpectrum out of it

    :param path: FITS file name
    :rtype: CSpectrum
    """
    fits = fitsio.FITS(path)
    obj_id = fits[0].read_header()['PFSVHASH']
    data = fits['FLUXTBL']
    spectralaxis = CSpectrumSpectralAxis(data['lambda'][:]*10)
    signal = CSpectrumFluxAxis(data['flux'][:], np.sqrt(data['fluxVariance'][:]))
    spectrum = CSpectrum(spectralaxis, signal)
    spectrum.SetName(os.path.basename(path))
    return spectrum
