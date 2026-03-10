from pylibamazed.AbstractSpectrumReader import AbstractSpectrumReader, register_reader
from pylibamazed.Container import Container

from pfs.datamodel.drp import PfsObject, PfsCoadd

import numpy as np

class PFSReader(AbstractSpectrumReader):

    def __init__(self, parameters, calibration_library, source_id):
        AbstractSpectrumReader.__init__(self,
                                        parameters,
                                        calibration_library,
                                        source_id)

    def load_wave(self, pfs_object: PfsCoadd, obs_id: str = "") -> None:
        """
        Load the wave length data in the self.waves Amazed container.
        
        Amazed container expects wave length unit in in Angstrom.
        Unit have to be convert from nm (in pfs_object) to Angtrom (in Amazed container).

        Parameters
        ----------

        pfs_object: PfsCoadd
            Data resource (unit are in nm)
        obs_id: str
            Identifiant of the observation
        """
        try:
            self.waves.append(pfs_object.wavelength*10, obs_id)
        except Exception as e:
            raise Exception(f"Could not load wave : {e}")
        
    def load_flux(self, pfs_object: PfsCoadd, obs_id: str = "") -> None:
        """
        Load the flux data in the self.flux Amazed container.
         
        Amazed container expects fluxes unit in in erg.cm-2.s-1.Angstrom-1
        Unit have to be convert from nJy (in pfs_object) to erg.cm-2.s-1.Angstrom-1 (in Amazed container).

        Parameters
        ----------

        pfs_object: PfsCoadd
            Data resource (unit are in nJy)
        obs_id: str
            Identifiant of the observation
        """
        try:
            flux = np.array(pfs_object.flux, dtype=np.float32)
            flux = np.multiply(1 / self.waves.get(obs_id) ** 2, flux) * 2.99792458 / 10 ** 14
            self.fluxes.append(flux, obs_id)
        except Exception as e:
            raise Exception(f"Could not load flux : {e}")

    def load_error(self, pfs_object: PfsCoadd, obs_id: str = "") -> None:
        """
        Load the flux error data in the self.errors Amazed container.
         
        Amazed container expects flux errors unit in in erg.cm-2.s-1.Angstrom-1
        Unit have to be convert from nJy (in pfs_object) to erg.cm-2.s-1.Angstrom-1 (in Amazed container).

        Parameters
        ----------

        pfs_object: PfsCoadd
            Data resource (unit are in nJy)
        obs_id: str
            Identifiant of the observation
        """
        try:
            error = np.array(np.sqrt(pfs_object.covar[0][0:]), dtype=np.float32)
            error = np.multiply(1 / self.waves.get(obs_id) ** 2, error) * 2.99792458 / 10 ** 14
            self.errors.append(error, obs_id)
        except Exception as e:
            raise Exception(f"Could not load error : {e}")

    def load_lsf(self, pfs_object: PfsCoadd, obs_id: str = "") -> None:
        """
        Load the LSF data in the Amazed container.
         
        Amazed container expects a lsf_type and a related lsf_data.

        Currently, LSF doesn't exist in the PFS product. It should be done in the future.
        This is a place holder for LSF in PFS product.

        If the data exists in PFS product, expected LSF is :
            - The LSF type is "gaussianConstantWidth"
            - The related width is get from resource
        
        Amazed container expects LSF width unit in in Angstrom.

        Parameters
        ----------

        pfs_object: PfsCoadd
            Data resource
        obs_id: str
            Identifiant of the observation
        """
        if hasattr(pfs_object,"lsf"):
            self.lsf_type = "gaussianConstantWidth"
            lsf = np.ndarray((1,),dtype=np.dtype([("width",'<f8')]))
            lsf["width"] = pfs_object.lsf
            self.lsf_data.append(lsf,obs_id)
            
    def load_others(self, pfs_object: PfsCoadd, obs_id: str = "") -> None:
        """
        Load other data in the Amazed container.
         
        Parameters
        ----------

        pfs_object: PfsCoadd
            Data resource (unit are in nJy)
        obs_id: str
            Identifiant of the observation
        """
        self.others["mask"] = Container(**{obs_id: pfs_object.mask})
        
    def get_nb_valid_points(self, pfs_object: PfsCoadd) -> int :
        """
        Get the number of valid pixel of the spectrum.

        Parameters
        ----------

        pfs_object: PfsCoadd
            Data resource (unit are in nJy)

        Return
        ------
        int:
            Number of valid pixel of the spectrum
        """

        mask = pfs_object.mask
        valid = np.where(mask == 0, True, False)
        return np.sum(valid)


register_reader("pfs", PFSReader)
