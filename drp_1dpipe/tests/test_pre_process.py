import pytest
import os
import json
import collections
import tempfile
import types

from drp_1dpipe.core.utils import normpath, config_update
from drp_1dpipe.core.config import Config

from drp_1dpipe.pre_process.pre_process import bunch, main_method
from drp_1dpipe.pre_process.config import config_defaults


def test_config_update__none():
    cf0 = config_defaults.copy()
    for k in cf0.keys():
        cf0[k] = None
    cf0['loglevel'] = 'DEBUG'
    obj = config_update(cf0)
    with pytest.raises(AttributeError):
        cfg = obj.config


def test_bunch():
    d = {}
    bunch_size = 4
    _spectra_path = tempfile.TemporaryDirectory()
    spectra_path = _spectra_path.name
    for f in range(9):
        d["{0}".format(f)] = tempfile.NamedTemporaryFile(dir=spectra_path)
    gen = bunch(bunch_size, spectra_path)
    assert isinstance(gen, types.GeneratorType)
    res = [b for b in gen]
    assert res is not None
    assert len(res) == 3
    assert len(res[0]) == 4
    assert len(res[1]) == 4
    assert len(res[2]) == 1


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

    list_file = []
    for i in range(9):
        with open(normpath(config.spectra_dir, '{}.file'.format(i)), 'w') as ff:
            list_file.append(os.path.basename(ff.name))
    result_run = main_method(config)
    assert result_run == 0

    json_bunch_list = os.path.join(config.bunch_list)
    assert os.path.exists(json_bunch_list)

    with open(json_bunch_list, "r") as ff:
        lines = ff.readlines()
    assert len(lines) == 1

    with open(json_bunch_list, 'r') as ff:
        data = json.load(ff)
    assert len(data) == 2
    assert os.path.basename(data[0]) == "spectralist_B0.json"
    assert os.path.basename(data[1]) == "spectralist_B1.json"

    total = []
    for e in data:
        with open(e, 'r') as ff:
            datal = json.load(ff)
            total.append(datal)
    assert len(total) == 2
    assert len(total[0]) == 8
    assert len(total[1]) == 1


