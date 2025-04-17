import pytest
import os
import json
import collections
import tempfile
import types
import glob

from drp_1dpipe.core.utils import normpath, config_update
from drp_1dpipe.core.config import Config

from drp_1dpipe.pre_process.pre_process import main_method
from drp_1dpipe.pre_process.config import config_defaults


def test_config_update__none():
    cf0 = config_defaults.copy()
    for k in cf0.keys():
        cf0[k] = None
    cf0['loglevel'] = 'DEBUG'
    obj = config_update(cf0)
    with pytest.raises(AttributeError):
        cfg = obj.config


def test_main_method():
    """
    The "test_run" function.

    This function test the "run" function of "pre_process.py" module.
    """

    wd = tempfile.TemporaryDirectory()
    sd = tempfile.TemporaryDirectory()
    config = Config(config_defaults)
    config.workdir = wd.name
    config.logdir = wd.name
    config.spectra_dir = sd.name
    config.output_dir = wd.name
    config.bunch_list = os.path.join(wd.name, config.bunch_list)
    config.coadd_file = ""

    list_file = []
    for i in range(9):
        with open(normpath(config.spectra_dir, '{}.file'.format(i)), 'w') as ff:
            list_file.append(os.path.basename(ff.name))
#    result_run = main_method(config)
    # assert result_run == 0

    # total = []
    # for e in glob.glob(os.path.join(config.output_dir,"spectralist_B*")):
    #     with open(e, 'r') as ff:
    #         datal = json.load(ff)
    #         total.append(datal)
    


