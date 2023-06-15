import os.path
from pfs.datamodel.drp import PfsObject
from pylibamazed.AbstractSpectrumReader import AbstractSpectrumReader
from astropy.io import fits
import numpy as np


class PfsObjectReader(AbstractSpectrumReader):

    def __init__(self, spectrum_path, calibration_library):
        self.pfs_object = PfsObject.readFits(spectrum_path)
        proc_id, ext = os.path.splitext(spectrum_path.split(os.pathsep)[-1])
        catId, tract, patch, objId, nVisit, pfsVisitHash = self._parse_pfsObject_name(
                os.path.basename(spectrum_path))
        self.pfs_object_id = {
            "catId": catId,
            "tract": tract,
            "patch": patch,
            "objId": objId,
            "nVisit": nVisit,
            "pfsVisitHash": pfsVisitHash
        }

        f = fits.open(spectrum_path)
        self.wl_infos = {"CRPIX1": f[1].header["CRPIX1"],
                         "CRVAL1": f[1].header["CRVAL1"],
                         "CDELT1": f[1].header["CDELT1"]}

        self.damd_version = f[1].header["DAMD_VER"]
        AbstractSpectrumReader.__init__(self,
                                        proc_id,
                                        calibration_library.parameters,
                                        calibration_library,
                                        proc_id)

    def load_wave(self, hdul, obs_id=""):
        """
        Load the spectral axis in self.wave , units are in Angstrom by default
        :param hdul: hdul of the resource where the wave can be found
        """
        mask = self.pfs_object.mask
        valid = np.where(mask == 0, True, False)
        wavelength = np.array(np.extract(valid, self.pfs_object.wavelength), dtype=np.float32)
        try:
            self.waves.append(wavelength*10, obs_id)
        except Exception as e:
            raise Exception("Could not load wave : {e}")
        
    def load_flux(self, hdul, obs_id=""):
        """
        Load the spectral axis in self.flux , units are in erg.cm-2 by default
        :param hdul: hdul of the resource where the wave can be found
        """
        mask = self.pfs_object.mask
        valid = np.where(mask == 0, True, False)
        flux = np.array(np.extract(valid, self.pfs_object.flux), dtype=np.float32)
        flux = np.multiply(1 / self.waves.get(obs_id) ** 2, flux) * 2.99792458 / 10 ** 14
        try:
            self.fluxes.append(flux, obs_id)
        except Exception as e:
            raise Exception("Could not load flux : {e}")

    def load_error(self, hdul, obs_id=""):
        mask = self.pfs_object.mask
        valid = np.where(mask == 0, True, False)
        error = np.array(np.extract(valid, np.sqrt(self.pfs_object.covar[0][0:])), dtype=np.float32)
        error = np.multiply(1 / self.waves.get(obs_id) ** 2, error) * 2.99792458 / 10 ** 14
        try:
            self.errors.append(error, obs_id)
        except Exception as e:
            raise Exception("Could not load error : {e}")

    def load_lsf(self, hdul):
        pass

    def load_photometry(self, hdul):
        pass

    def get_nb_valid_points(self):
        """
        Read a pfsObject FITS file and tells if it is valid

        :param path: FITS file name
        :rtype: CSpectrum
        """

        mask = self.pfs_object.mask
        valid = np.where(mask == 0, True, False)
        return np.sum(valid)

    def _parse_pfsObject_name(self, name):
        """Parse a pfsObject file name.

        Template is : pfsObject-%05d-%05d-%s-%016x-%03d-0x%016x.fits
        pfsObject-%(catId)05d-%(tract)05d-%(patch)s-%(objId)016x-%(nVisit % 1000)03d-0x%(pfsVisitHash)016x.fits
        """
        basename = os.path.splitext(name)[0]
        head, catId, tract, patch, objId, nvisit, pfsVisitHash = basename.split('-')
        assert head == 'pfsObject'
        return (int(catId), int(tract), patch, int(objId, 16), int(nvisit), int(pfsVisitHash, 16))




