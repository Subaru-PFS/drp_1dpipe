from astropy.io import fits
from pfs.datamodel.drp import PfsObject
import os
import glob

import sys

class PFSExternalStorage:
    """Opener for FITS files that should be treated with an AmazedSpectrumReader."""

    def __init__(self, config, spectrum_id):
        self.config = config

        self.spectrum_id = spectrum_id
        self.spectrum_infos = dict()
        self.global_infos = dict()

    # for LAM client compatibility    
    def set_spectrum_id(self, spectrum_id): 
        self.spectrum_id = spectrum_id.ProcessingID
        
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
        try:
            spectra_dir = self.config.spectra_dir
        except:
            spectra_dir = self.config.spectrum_dir # for LAM client compatibility
        spectrum_path = glob.glob(os.path.join(spectra_dir,"*","*","*",f'pfsObject-{self.spectrum_id}.fits'))
        if len(spectrum_path) != 1:
            raise Exception(f"{self.spectrum_id} cannot be found in {spectra_dir}")
        spectrum_path = spectrum_path[0]
#        spectrum_path = f'{self.config.spectrum_dir}/{catId:05}/{tract:05}/{patch}/pfsObject-{self.spectrum_id.ProcessingID}.fits'
        pfs_object = PfsObject.readFits(spectrum_path)
        self.pfs_object_id = pfs_object.getIdentity()
        self.astronomical_source_id = f'{self.pfs_object_id["catId"]:05}-{self.pfs_object_id["tract"]:05}-{self.pfs_object_id["patch"]}-{self.pfs_object_id["objId"]:016x}'
        self.mask = pfs_object.mask
        self.full_wavelength= pfs_object.wavelength

        try:
            with fits.open(spectrum_path) as f:
                try:
                    pfs_object.lsf = f[1].header["LSF"] * 0.8 # this is only for pfsObjects processed at LAM, which have integrated lsf pickle files content
                except:
                    print("Could not retrieve lsf",file = sys.stderr)
                    pass
                self.global_infos["VERSION_drp_stella"] =f[1].header["VERSION_drp_stella"]
                self.global_infos["damd_version"] = f[1].header["DAMD_VER"]
                self.spectrum_infos["fiberId"] = f[7].data["fiberId"][0]
                self.spectrum_infos["wl_infos"] = {"CRPIX1": f[1].header["CRPIX1"],
                                                   "CRVAL1": f[1].header["CRVAL1"],
                                                   "CDELT1": f[1].header["CDELT1"]}
                arms = (
                    str(set(f[7].data["arm"]))
                    .replace("'", "")
                    .replace("{", "")
                    .replace(",", "")
                    .replace("}", "")
                    .replace(" ", "")
                )
                self.spectrum_infos["arms"] = "".join(sorted(arms)) 
                self.spectrum_infos["nVisit"] = pfs_object.nVisit
                self.spectrum_infos["fiberFlux"] = pfs_object.target.fiberFlux
                self.spectrum_infos["RA"]=pfs_object.target.ra
                self.spectrum_infos["DEC"]=pfs_object.target.dec
                self.spectrum_infos["designIds"]=list(f[7].data['pfsDesignId'])
                self.spectrum_infos["visits"]=list(f[7].data['visit'])
        except Exception as e:
            print("Could not retrieve additional infos : {e} ",file = sys.stderr)
            pass
                
        return pfs_object

    def close(self, resource: PfsObject):
        pass
        
