from collections import namedtuple

from drp_1dpipe.io.PFSDataProvider import PFSDataProvider

from drp_1dpipe.tests.test_PFSExternalStorage import MyClass

class MyParam:
    def get_multiobs_method(self):
        return False
    def get_lsf_type(self):
        pass
    def get_lsf(self):
        pass

def test_PFSDataProvider(mocker):

    Config = namedtuple("Config", "reader coadd_file")

    # Test : instance test
    # --------------------

    # Basic instance test
    dp = PFSDataProvider(Config(reader="pfs", coadd_file=None), MyParam(), None)

    # Test : get_spectrum method test
    # --------------------------------
    
    # Mock PFS product get method
    mocker.patch("pfs.datamodel.drp.PfsCoadd.readFits").return_value = MyClass()

    spectrum = dp.get_spectrum('4')
    assert spectrum.source_id == '4'
