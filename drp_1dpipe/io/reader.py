import os.path
from pfs.datamodel.drp import PfsObject
from pylibamazed.redshift import (CSpectrumSpectralAxis,
                                  CSpectrumFluxAxis_withError,
                                  CSpectrum)
from astropy.io import fits
import numpy as np


def read_spectrum(path):
    """
    Read a pfsObject FITS file and build a CSpectrum out of it

    :param path: FITS file name
    :rtype: CSpectrum
    """

    obj = PfsObject.readFits(path)
    mask = obj.mask
    valid = np.where(mask == 0, True, False)
    wavelength = np.array(np.extract(valid, obj.wavelength), dtype=np.float32)
    flux = np.array(np.extract(valid, obj.flux), dtype=np.float32)
    error = np.array(np.extract(valid, np.sqrt(obj.covar[0][0:])), dtype=np.float32)

    # convert from nJy
    flux = np.multiply(1/wavelength**2, flux)*2.99792458/10**14
    error = np.multiply(1/wavelength**2, error)*2.99792458/10**14

    spectralaxis = CSpectrumSpectralAxis(wavelength * 10.0, "")
    signal = CSpectrumFluxAxis_withError(flux, error)
    spectrum = CSpectrum(spectralaxis, signal)
    spectrum.SetName(os.path.basename(path))
    return spectrum


def get_nb_valid_points(path):
    """
    Read a pfsObject FITS file and tells if it is valid

    :param path: FITS file name
    :rtype: CSpectrum
    """

    obj = PfsObject.readFits(path)
    mask = obj.mask
    valid = np.where(mask == 0, True, False)
    return np.sum(valid)


def get_datamodel_version(path):
    """
        Read a pfsObject FITS file and tells if it is valid

        :param path: FITS file name
        :rtype: CSpectrum
    """
    f = fits.open(path)
    return f[1].header["DAMD_VER"]