from astropy.io import fits
from pfs.datamodel.drp import PfsObject,PfsCoadd
from pylibamazed.AbstractExternalStorage import AbstractExternalStorage, register_storage
import os
import glob

import sys


class PFSExternalStorage(AbstractExternalStorage):
    """Opener for FITS files that should be treated with an PFSSpectrumReader."""


    # for LAM client compatibility    
    def set_spectrum_id(self, spectrum_id): 
        self.spectrum_id = int((spectrum_id.ProcessingID).split('-')[-1],16)
        self.config.coadd_file = os.path.join(self.config.spectrum_dir,spectrum_id.Path)

    def _get_pfsObject_from_coadd(self):
        try:
            coadd = PfsCoadd.readFits(self.config.coadd_file)
            return coadd.get(self.spectrum_id)
        except:
            calibrated = PfsCalibrated.readFits(self.config.coadd_file)
            return calibrated.get(self.spectrum_id)
        
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

        pfs_object = self._get_pfsObject_from_coadd()           
        
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
        self.spectrum_infos["targetType"]=pfs_object.target.targetType

        return pfs_object

    def close(self, resource: PfsObject):
        pass
        

register_storage("pfs",PFSExternalStorage)
