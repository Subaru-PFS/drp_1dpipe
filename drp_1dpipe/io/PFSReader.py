from pylibamazed.AbstractSpectrumReader import AbstractSpectrumReader
from astropy.io import fits
import numpy as np
import sys


class PFSReader(AbstractSpectrumReader):

    def __init__(self, observation_id, parameters, calibration_library, source_id):
        AbstractSpectrumReader.__init__(self,
                                        observation_id,
                                        parameters,
                                        calibration_library,
                                        source_id)

    def load_wave(self, pfs_object, obs_id=""):
        """
        Load the spectral axis in self.wave , units are in Angstrom by default
        :param pfs_object: pfs_object of the resource where the wave can be found
        """
        mask = pfs_object.mask
        valid = np.where(mask == 0, True, False)
        wavelength = np.array(np.extract(valid, pfs_object.wavelength), dtype=np.float32)
        try:
            self.waves.append(wavelength*10, obs_id)
        except Exception as e:
            raise Exception("Could not load wave : {e}")
        
    def load_flux(self, pfs_object, obs_id=""):
        """
        Load the spectral axis in self.flux , units are in erg.cm-2 by default
        :param pfs_object: pfs_object of the resource where the wave can be found
        """
        mask = pfs_object.mask
        valid = np.where(mask == 0, True, False)
        flux = np.array(np.extract(valid, pfs_object.flux), dtype=np.float32)
        flux = np.multiply(1 / self.waves.get(obs_id) ** 2, flux) * 2.99792458 / 10 ** 14
        try:
            self.fluxes.append(flux, obs_id)
        except Exception as e:
            raise Exception("Could not load flux : {e}")

    def load_error(self, pfs_object, obs_id=""):
        mask = pfs_object.mask
        valid = np.where(mask == 0, True, False)
        error = np.array(np.extract(valid, np.sqrt(pfs_object.covar[0][0:])), dtype=np.float32)
        error = np.multiply(1 / self.waves.get(obs_id) ** 2, error) * 2.99792458 / 10 ** 14
        try:
            self.errors.append(error, obs_id)
        except Exception as e:
            raise Exception("Could not load error : {e}")

    def load_lsf(self, pfs_object,obs_id=""):
        if hasattr(pfs_object,"lsf"):
            lsf = np.ndarray((1,),dtype=np.dtype([("width",'<f8')]))
            lsf["width"] = pfs_object.lsf
            self.lsf_type = "gaussianConstantWidth"
            self.lsf_data.append(lsf,obs_id)
            
    def load_photometry(self, pfs_object):
        pass

    def get_nb_valid_points(self,pfs_object):
        """
        Read a pfsObject FITS file and tells if it is valid

        :param path: FITS file name
        :rtype: CSpectrum
        """

        mask = pfs_object.mask
        valid = np.where(mask == 0, True, False)
        return np.sum(valid)
