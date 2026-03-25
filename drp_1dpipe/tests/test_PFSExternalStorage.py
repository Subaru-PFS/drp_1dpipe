import pytest
from collections import namedtuple
import numpy as np

from drp_1dpipe.process_spectra.config import config_defaults
from drp_1dpipe.core.utils import  config_update
from drp_1dpipe.io.PFSExternalStorage import PFSExternalStorage

# Define local test content
class FackPfsObject():
    mask=wavelength=[]
    metadata={"VERSION_DRP_STELLA":"1.0", "VERSION_DATAMODEL":"2.0"}
    nVisit="nvisit"
    target = namedtuple("Target", "fiberFlux ra dec targetType")
    target.fiberFlux="fiberFlux"
    target.ra="ra"
    target.dec="dec"
    target.targetType="targetType"
    observations = namedtuple("Observation", "arm pfsDesignId visit fiberId")
    observations.arm="arm"
    observations.pfsDesignId="Observation"
    observations.visit="visit"
    observations.fiberId="fiberId"
    wavelength=np.arange(1, 3, dtype=np.float64)
    flux=np.ones(2, dtype=np.float64)
    covar=np.ones([2,2], dtype=np.float64)
    mask=np.array(2, dtype=np.int16)
    def getIdentity(self):
        return {"catId":1, "tract":2, "patch":"PP", "objId":12}
class MyClass():
    def get(self, sid):
        return FackPfsObject()


def test_PFSExternalStorage(mocker):
    """
    Check the behavior of PFSExternalStorage
    """

    Config = namedtuple("Config", "reader")


    # Test : instance test
    # --------------------

    # Basic instance test
    PFSExternalStorage(Config(reader="pfs"))

    # Instance test raises an exception with bad reader name
    with pytest.raises(Exception):
        PFSExternalStorage(Config(reader="foo"))
    
    # Instance test with default config
    storage = PFSExternalStorage(config_update(config_defaults))


    # Test : read method
    # ------------------

    # Mock PFS product read method
    mocker.patch("pfs.datamodel.drp.PfsCoadd.readFits").return_value = MyClass()

    # Read method
    storage.read("0","my_file.fits","obs_id")
    assert storage.global_infos["damd_version"] == "2.0"
    assert storage.spectrum_infos["DEC"] == "dec"
    assert storage.spectrum_infos["astronomical_source_id"] == "00001-00002-PP-000000000000000c"


    # Test : PFS product should be read once
    # --------------------------------------

    # Mock PFS product read method
    mock_read_pfs_product = mocker.patch("pfs.datamodel.drp.PfsCoadd.readFits")
    mock_read_pfs_product.return_value = MyClass()

    # Define new storage
    new_storage = PFSExternalStorage(config_update(config_defaults))
    new_storage.read("0","my_file.fits","obs_id")
    mock_read_pfs_product.assert_called_once()
    new_storage.read("0","my_file.fits","obs_id")
    mock_read_pfs_product.assert_called_once()