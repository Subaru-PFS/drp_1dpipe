import pytest
import os
import json
import collections
import tempfile
import types
import glob

from drp_1dpipe.core.utils import normpath, config_update
from drp_1dpipe.core.config import Config

from drp_1dpipe.merge_results.merge_results import concat_summury_files, main_method
from drp_1dpipe.merge_results.config import config_defaults


def test_config_update_none():
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
    config = Config(config_defaults)
    config.workdir = wd.name
    config.output_dir = wd.name
    
    with pytest.raises(FileNotFoundError):
        main_method(config)
    
    bd = os.path.join(config.output_dir, "B0")
    config.bunch_listfile = os.path.join(bd, 'reduce.json')
    bdd = os.path.join(bd, 'data')
    os.makedirs(bdd, exist_ok=True)

    list_file = []
    for i in range(2):
        with open(normpath(bdd, '{}.file'.format(i)), 'w') as ff:
            ff.write("/n")
            list_file.append(ff.name)

    with open(config.bunch_listfile, "w") as ff:
        json.dump([bd], ff)
    
    rstr = "#com\nstr1	str2	1.0	2.0 str3    str4	3.0	str5	4.0	5.0	6.0	7.0	str6"
    rname = os.path.join(bd, "redshift.csv")
    with open(rname, "w") as ff:
        ff.write(rstr)

    result_run = main_method(config)
    assert result_run == 0

    data_dir = os.path.join(config.output_dir, 'data')
    assert os.path.exists(data_dir)

    dl = os.listdir(data_dir)
    assert len(dl) == 2
    assert "0.file" in dl
    assert "1.file" in dl
