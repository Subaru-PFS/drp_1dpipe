import pytest
import os.path
import tempfile

from drp_1dpipe.core.config import ConfigJson


def test_config_json():
    """
    Check the behavior of ConfigJson class
    """
    cf0 = {"str":"str0.0", "int":1, "float":1.1, "str_arr":["str0.0","str1.0"]}
    config = ConfigJson(cf0)
    assert config.str == "str0.0"
    assert config.int == 1
    assert config.float == 1.1
    assert config.str_arr[0] == "str0.0"
    assert config.str_arr[1] == "str1.0"
    cfl = tempfile.NamedTemporaryFile()
    json_str = '{"str":"str0.1", "int":2, "float":1.2, "str_arr":["str0.1","str1.1"]}'
    with open(cfl.name, 'w') as ff:
        ff.write(json_str)
    config.load(cfl.name)
    assert config.str == "str0.1"
    assert config.int == 2
    assert config.float == 1.2
    assert config.str_arr[0] == "str0.1"
    assert config.str_arr[1] == "str1.1"
    cfs = tempfile.NamedTemporaryFile()
    config.save(cfs.name, indent=None)
    with open(cfs.name, "r") as ff:
        line = ff.readline()
    assert line.replace(" ","") == json_str.replace(" ","")
    cf1 = {"str":"str0.2"}
    config.update(cf1)
    assert config.str == "str0.2"