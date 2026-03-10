from pfs.datamodel.drp import PfsObject, PfsCoadd
from pylibamazed.AbstractExternalStorage import AbstractExternalStorage, register_storage


class PFSExternalStorage(AbstractExternalStorage):
    """Opener for FITS files that should be treated with an PFSSpectrumReader."""

    def __init__(self, config):
        super().__init__(config)
        if not config.reader == "pfs":
            raise Exception(f"Cannot initialize PFSExternalStorage for a {config.reader} reader.")
        self.read_flag = False
       
    def read(
        self,
        spectrum_id: str,
        path: str,
        obs_id: str = "",
    ) -> PfsObject:
        """
        Read a PFS spectrum file and return its data.

        Parameters
        ----------

        spectrun_id: str
            id of the source
        path: str
            path or anything else neded to acquire the resource
        obs_id: str
            id of the observation
        
        Return
        ------
        PfsObject
            PFS spectrum object
        """
        if not self.read_flag:
            self.coadd = PfsCoadd.readFits(path)
            self.read_flag = True
        
        pfs_object = self.coadd.get(spectrum_id)
        pfs_object_id = pfs_object.getIdentity()
        self.spectrum_infos["pfs_object_id"] = pfs_object_id
        self.spectrum_infos["astronomical_source_id"] = f'{pfs_object_id["catId"]:05}-{pfs_object_id["tract"]:05}-{pfs_object_id["patch"]}-{pfs_object_id["objId"]:016x}'
        self.spectrum_infos["mask"] = pfs_object.mask
        self.spectrum_infos["full_wavelength"]= pfs_object.wavelength

        self.global_infos["VERSION_drp_stella"] = pfs_object.metadata["VERSION_DRP_STELLA"]
        self.global_infos["damd_version"] = pfs_object.metadata["VERSION_DATAMODEL"]
        
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
        """
        Close resource.

        Parameters
        ----------

        resource: PfsObject
            External resource.
        """
        pass
        

register_storage("pfs", PFSExternalStorage)
