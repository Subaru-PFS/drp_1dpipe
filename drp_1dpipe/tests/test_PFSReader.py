import pytest
from collections import namedtuple

from drp_1dpipe.process_spectra.config import config_defaults
from drp_1dpipe.core.utils import  config_update
from drp_1dpipe.io.PFSReader import PFSReader

def test_PFSReader():

    # Test : instance test
    # --------------------

    # Basic instance test
    reader = PFSReader(None, None, None)


