from pylibamazed.AbstractSpectrumReader import AbstractSpectrumReader, register_reader
from pylibamazed.Container import Container

from astropy.io import fits
import numpy as np
import sys


class PFSReader(AbstractSpectrumReader):

    def __init__(self, parameters, calibration_library, source_id):
        AbstractSpectrumReader.__init__(self,
                                        parameters,
                                        calibration_library,
                                        source_id)

    def load_wave(self, pfs_object, obs_id=""):
        """
        Load the spectral axis in self.wave , units are in Angstrom by default
        :param pfs_object: pfs_object of the resource where the wave can be found
        """
        try:
            self.waves.append(pfs_object.wavelength*10, obs_id)
        except Exception as e:
            raise Exception("Could not load wave : {e}")
        
    def load_flux(self, pfs_object, obs_id=""):
        """
        Load the spectral axis in self.flux , units are in erg.cm-2 by default
        :param pfs_object: pfs_object of the resource where the wave can be found
        """
        try:
            flux = np.array(pfs_object.flux, dtype=np.float32)
            flux = np.multiply(1 / self.waves.get(obs_id) ** 2, flux) * 2.99792458 / 10 ** 14
            self.fluxes.append(flux, obs_id)
        except Exception as e:
            raise Exception("Could not load flux : {e}")

    def load_error(self, pfs_object, obs_id=""):
        try:
            error = np.array(np.sqrt(pfs_object.covar[0][0:]), dtype=np.float32)
            error = np.multiply(1 / self.waves.get(obs_id) ** 2, error) * 2.99792458 / 10 ** 14
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

    def load_others(self, pfs_object, obs_id: str = "") -> None:
        self.others["mask"] = Container(**{obs_id: pfs_object.mask})
        
    def get_nb_valid_points(self,pfs_object):
        """
        Read a pfsObject FITS file and tells if it is valid

        :param path: FITS file name
        :rtype: CSpectrum
        """

        mask = pfs_object.mask
        valid = np.where(mask == 0, True, False)
        return np.sum(valid)

register_reader("pfs",PFSReader)
