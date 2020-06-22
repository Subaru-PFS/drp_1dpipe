"""
File: drp_1dpipe/tests/test_utils.py

Created on: 31/10/18
Author: PSF DRP1D developers
"""

import pytest
import os.path
import tempfile
import threading
import time
from drp_1dpipe.core.utils import get_args_from_file, convert_dl_to_ld
from drp_1dpipe.core.utils import get_auxiliary_path, get_conf_path, normpath, wait_semaphores
from drp_1dpipe.core.utils import config_update, config_save
from drp_1dpipe.core.utils import UnconsistencyArgument


def test_auxdir():
    """
    The "test_auxdir" function.

    This function tests features concerning auxiliary directory.
    """
    assert os.path.exists(get_auxiliary_path("."))
    assert not(os.path.exists(get_auxiliary_path("foo.txt")))


def test_confdir():
    """
    The "test_confdir" function.

    This function tests features concerning configuration directory.
    """
    assert os.path.exists(get_conf_path("."))
    assert not(os.path.exists(get_conf_path("foo.txt")))


def test_args_from_file():
    """
    The "test_args_from_file" function.

    This function tests feature of retrieving argument value from
    configuration file
    """
    fp1 = tempfile.NamedTemporaryFile()
    conf_file = fp1.name
    with open(conf_file, 'w') as cf:
        cf.write('arg1 = 4\n')
        cf.write('arg2 = foo2 foo2\n')
        cf.write('arg3 = foo3 # test\n')
        cf.write('arg4 = # foo4\n')
        cf.write('arg5 # = foo5\n')
        cf.write('#arg6 = foo6')
        cf.write('arg7 arg7 = foo7\n')

    args = {'arg1':'2'}

    args.update(get_args_from_file(conf_file))
    assert args["arg1"] == "4"
    assert args["arg2"] == "foo2 foo2"
    assert args["arg3"] == "foo3"
    assert args["arg4"] == ""
    with pytest.raises(KeyError):
        args["arg5"]
    with pytest.raises(KeyError):
        args["arg6"]
    with pytest.raises(KeyError):
        args["arg7"]
    fp1.close()


def test_normpath():
    assert normpath('~/foo//bar/baz/~') == os.path.expanduser('~/foo/bar/baz/~')
    assert normpath('~/foo/.././bar/./baz/') == os.path.expanduser('~/bar/baz')
    assert normpath('////foo/baz////') == os.path.expanduser('/foo/baz')


def _create_semaphores(semaphores):
    """Create all files in semaphores, one every 4 seconds"""
    for f in semaphores:
        fd = open(f, 'w')
        fd.write('foo')
        fd.close()
        time.sleep(4)

def test_convert_dl_to_ld():
    dl = {"l1":[1,2],"l2":[3,4]}
    ld = convert_dl_to_ld(dl)
    assert ld[0]["l1"] == 1
    assert ld[0]["l2"] == 3
    assert ld[1]["l1"] == 2
    assert ld[1]["l2"] == 4
    dl = {"l1":[1,2],"l2":[3]}
    with pytest.raises(UnconsistencyArgument):
        convert_dl_to_ld(dl)


def test_config_update():
    cf0 = {"config":"","a1":"v1.0","a2":"v2.0","a3":"v3.0","a4":"v4.0"}
    # Test on install conf path
    cf1 = '{"a1":"v1.1"}'
    cfl1 = tempfile.NamedTemporaryFile()
    with open(cfl1.name, 'w') as ff:
        ff.write(cf1)
    obj = config_update(cf0, install_conf_path=cfl1.name)
    assert obj.a1 == "v1.1"
    assert obj.a2 == "v2.0"
    assert obj.a3 == "v3.0"
    assert obj.a4 == "v4.0"
    # Test on args
    args = {"a2":"v2.1"}
    obj = config_update(cf0, args=args)
    assert obj.a1 == "v1.0"
    assert obj.a2 == "v2.1"
    assert obj.a3 == "v3.0"
    assert obj.a4 == "v4.0"
    # Test on config args
    cf_args = '{"a3":"v3.1"}'
    cfl2 = tempfile.NamedTemporaryFile()
    with open(cfl2.name, 'w') as ff:
        ff.write(cf_args)
    argsc={"config":cfl2.name}
    obj = config_update(cf0, args=argsc)
    assert obj.a1 == "v1.0"
    assert obj.a2 == "v2.0"
    assert obj.a3 == "v3.1"
    assert obj.a4 == "v4.0"
    # Test on environment variable
    json_str = '{"a4":"v4.1"}'
    cfl3 = tempfile.NamedTemporaryFile()
    with open(cfl3.name, 'w') as ff:
        ff.write(json_str)
    os.environ['TEST_STARTUP'] = cfl3.name
    obj = config_update(cf0, environ_var='TEST_STARTUP')
    assert obj.a1 == "v1.0"
    assert obj.a2 == "v2.0"
    assert obj.a3 == "v3.0"
    assert obj.a4 == "v4.1"
    # Whole test
    args.update(argsc)
    obj = config_update(cf0, args=args, install_conf_path=cfl1.name, environ_var='TEST_STARTUP')
    assert obj.a1 == "v1.1"
    assert obj.a2 == "v2.1"
    assert obj.a3 == "v3.1"
    assert obj.a4 == "v4.1"
    del os.environ['TEST_STARTUP']
    # Test on log level
    cf0 = {"config":"", "loglevel": "no"}
    with pytest.raises(KeyError):
        obj = config_update(cf0)
    


def test_config_save():
    fd = tempfile.TemporaryDirectory()
    cf0 = {"k":"v","output_dir":fd.name}
    cf = config_update(cf0)
    config_save(cf, 'config.json', indent=None)
    with open(os.path.join(fd.name, 'config.json')) as ff:
        line = ff.readline()
    assert line.replace(" ","") == "{"+'"k":"v","output_dir":"{}"'.format(fd.name)+"}"


# def test_wait_semaphores():
#
#     # wait a never created file
#     _error = []
#     try:
#         wait_semaphores(['/file/that/should/not/exist'], 3)
#     except TimeoutError as e:
#         _error = e.args[0]
#     except:
#         raise
#
#     assert _error == ['/file/that/should/not/exist']
#
#     # create files before waiting
#     semaphores = [tempfile.NamedTemporaryFile(prefix='pytest_') for i in range(5)]
#     wait_semaphores([s.name for s in semaphores], 10, 5)
#
#     # create files after waiting
#     with tempfile.TemporaryDirectory(prefix='pytest_') as tmpdir:
#         semaphores = [os.path.join(tmpdir, str(i)) for i in range(6)]
#         t = threading.Thread(target=_create_semaphores, args=(semaphores,))
#         t.start()
#         wait_semaphores(semaphores, 50, 5)
#         t.join(timeout=2)
