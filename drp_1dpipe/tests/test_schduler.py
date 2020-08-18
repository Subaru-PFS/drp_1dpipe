import pytest
import os
import tempfile
import json

from drp_1dpipe.core.config import Config
from drp_1dpipe.core.utils import config_update
from drp_1dpipe.scheduler.scheduler import map_process_spectra_entries, reduce_process_spectra_output, auto_dir, main_method, list_aux_data
from drp_1dpipe.scheduler.config import config_defaults


def test_config_update_none():
    cf0 = config_defaults.copy()
    for k in cf0.keys():
        cf0[k] = None
    cf0['loglevel'] = 'DEBUG'
    obj = config_update(cf0)
    with pytest.raises(AttributeError):
        cfg = obj.config


def test_map_process_spectra_entries():
    sjbl = '["list0.json","list1.json"]'
    jbl = tempfile.NamedTemporaryFile()
    with open(jbl.name, 'w') as ff:
        ff.write(sjbl)
    bl,ol,ll = map_process_spectra_entries(jbl.name,"output","log")
    assert len(bl) == len(ol) == len(ll)
    assert bl[0] == "list0.json"
    assert bl[1] == "list1.json"
    assert ol[0] == "output/B0"
    assert ol[1] == "output/B1"
    assert ll[0] == "log/B0"
    assert ll[1] == "log/B1"


def test_reduce_process_spectra_output():
    sjbl = '["list0.json","list1.json"]'
    jbl = tempfile.NamedTemporaryFile()
    with open(jbl.name, 'w') as ff:
        ff.write(sjbl)
    jpl = tempfile.NamedTemporaryFile()
    reduce_process_spectra_output(jbl.name, "output", jpl.name)
    with open(jpl.name, 'r') as ff:
        pl = json.load(ff)
    assert len(pl) == 2
    assert pl[0] == "output/B0"
    assert pl[1] == "output/B1"


def test_list_aux_data():
    bd = tempfile.TemporaryDirectory()
    bl = [os.path.join(bd.name, "list0.json"), os.path.join(bd.name, "list1.json")]
    b1dl = '["file1.fits", "file2.fits"]'
    b2dl = '["file3.fits", "file4.fits"]'
    jbl = tempfile.NamedTemporaryFile()
    with open(jbl.name, 'w') as ff:
        json.dump(bl, ff)
    with open(os.path.join(bd.name,"list0.json"), 'w') as ff:
        ff.write(b1dl)
    with open(os.path.join(bd.name,"list1.json"), 'w') as ff:
        ff.write(b2dl)
    ll = list_aux_data(jbl.name, "output")
    assert ll[0] == "output/B0/file1"
    assert ll[1] == "output/B0/file2"
    assert ll[2] == "output/B1/file3"
    assert ll[3] == "output/B1/file4"
    

def test_auto_dir():
    cf0 = config_defaults.copy()
    config = config_update(cf0)
    config.logdir = '.'
    auto_dir(config)
    assert config.logdir == '.'
    assert 'drp1d_spectra_' in config.output_dir
    config.output_dir = 'test'
    config.logdir = '@AUTO@'
    auto_dir(config)
    assert config.output_dir == 'test'
    assert config.logdir == 'test/log'
    config.output_dir = '@AUTO@'
    config.logdir = '@AUTO@'
    auto_dir(config)
    assert 'drp1d_spectra_' in config.output_dir


def test_main_method():
    wd = tempfile.TemporaryDirectory()
    ld = tempfile.TemporaryDirectory()
    od = tempfile.TemporaryDirectory()
    config = Config(config_defaults)
    config.workdir = wd.name
    config.logdir = ld.name
    config.output_dir = od.name
    with pytest.raises(FileNotFoundError):
        main_method(config)
    logpath = os.path.join(ld.name, "scheduler.log")
    assert os.path.exists(logpath)
