from astropy.io import fits
from pfs.datamodel.drp import PfsObject,PfsCoadd
from pylibamazed.AbstractExternalStorage import AbstractExternalStorage
import os
import glob

import sys


class PFSExternalStorage(AbstractExternalStorage):
    """Opener for FITS files that should be treated with an PFSSpectrumReader."""

    def __init__(self, config, spectrum_id):
        self.config = config

        self.spectrum_id = spectrum_id
        self.spectrum_infos = dict()
        self.global_infos = dict()

    # for LAM client compatibility    
    def set_spectrum_id(self, spectrum_id): 
        self.spectrum_id = spectrum_id.ProcessingID

    def _get_pfsObject_from_dir(self):
        try:
            spectra_dir = self.config.spectra_dir
        except:
            spectra_dir = self.config.spectrum_dir # for LAM client compatibility
        spectrum_path = glob.glob(os.path.join(spectra_dir,"*","*","*",f'pfsObject-{self.spectrum_id}.fits'))
        if len(spectrum_path) != 1:
            raise Exception(f"{self.spectrum_id} cannot be found in {spectra_dir}")
        spectrum_path = spectrum_path[0]

        return PfsObject.readFits(spectrum_path)

    def _get_pfsObject_from_coadd(self):
        coadd = PfsCoadd.readFits(self.config.coadd_file)
        return coadd.get(self.spectrum_id)
        
    def read(
        self,
        obs_id: str = "",
    ) -> PfsObject:
        """
        Read a spectrum file and return its data.

        :param obs_id: id of the observation
        :type obs_id: str

        :return: HDUList
        """

        if self.config.coadd_file:
            pfs_object = self._get_pfsObject_from_coadd()           
        else:
            pfs_object = self._get_pfsObject_from_dir()
        self.pfs_object_id = pfs_object.getIdentity()
        self.astronomical_source_id = f'{self.pfs_object_id["catId"]:05}-{self.pfs_object_id["tract"]:05}-{self.pfs_object_id["patch"]}-{self.pfs_object_id["objId"]:016x}'
        self.mask = pfs_object.mask
        self.full_wavelength= pfs_object.wavelength

        self.global_infos["VERSION_drp_stella"] = ""
        self.global_infos["damd_version"] = ""
        try:
            self.global_infos["VERSION_drp_stella"] = pfs_object.metadata["VERSION_DRP_STELLA"]
            self.global_infos["damd_version"] = pfs_object.metadata["VERSION_DATAMODEL"]
        except:
            print("could not retrieve 2D and or damd version",file =sys.stderr)
        
        arms = list(set(pfs_object.observations.arm))
        self.spectrum_infos["arms"] = "".join(sorted(arms)) 
        self.spectrum_infos["nVisit"] = pfs_object.nVisit
        self.spectrum_infos["fiberFlux"] = pfs_object.target.fiberFlux
        self.spectrum_infos["RA"]=pfs_object.target.ra
        self.spectrum_infos["DEC"]=pfs_object.target.dec
        self.spectrum_infos["designIds"]=list(pfs_object.observations.pfsDesignId)
        self.spectrum_infos["visits"]=list(pfs_object.observations.visit)
        self.spectrum_infos["fiberId"] = list(pfs_object.observations.fiberId)
        return pfs_object

    def close(self, resource: PfsObject):
        pass
        
    #  to be used as context manager
    def __enter__(self):
        obs_id, kwargs = self._read_param
        self.resource = self.read(obs_id, **kwargs)
        return self.resource

    def __call__(self, obs_id="", **kwargs):
        # store read parameters
        self._read_param = (obs_id, kwargs)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._read_param = None
        self.close(self.resource)
        self.resource = None
        return False
