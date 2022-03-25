import os
import shutil
import copy
import logging
import time
import datetime

from drp_1dpipe.core.config import ConfigJson

_loglevels = {
    'ERROR': logging.ERROR,
    'WARNING': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
}

class UnconsistencyArgument(Exception):
    """Exception raised by unconsistency argument checking

    Parameters
    ----------
    msg : str
        Human readable string describing the exception.
    code : :obj:`int`, optional
        Numeric error code.

    Attributes
    ----------
    msg : str
        Human readable string describing the exception.
    """    
    def __init__(self, msg):
        self.msg = msg


def get_auxiliary_path(file_name):
    """Get the full path of file in auxiliary directory.

    :param file_name: Name of the file.
    :return: Full path of auxiliary directory.

    :Example:

    get_auxiliary_path("my_data.dat") # -> /python/package/path/my_data.dat
    """
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), 'auxdir', file_name)


def get_conf_path(file_name):
    """Get the full path of file in configuration directory.

    :param file_name: Name of the file.
    :return: Full path of configuration file.

    :Example:

    get_conf_path("my_conf.conf") # -> /python/package/path/my_conf.conf
    """
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), 'conf', file_name)



def get_args_from_file(file_name):
    """Get arguments value from configuration file.

    :param file_name: name of the configuration file

    Get arguments value from configuration file. Value has to be formatted as
    ``option = string``. To comment use ``#``.

    Return a key, value pairs as a dictionnary.
    """
    args = {}
    with open(get_conf_path(file_name), 'r') as ff:
        lines = ff.readlines()
    for line in lines:
        try:
            key, value = line.replace('\n', '').split('#')[0].split("=")
        except ValueError:
            continue
        args[key.strip()] = value.strip()
    return args


def normpath(*args):
    return os.path.normpath(os.path.expanduser(os.path.expandvars(os.path.join(*args))))


def wait_semaphores(semaphores, timeout=4.354e17, tick=60):
    """Wait all files are created.

    :param semaphores: List of files to watch for creation.
    :param timeout: Maximun wait time, in seconds.
    """
    start = time.time()
    # we have to copy the semaphore list as some other thread may use it
    _semaphores = copy.copy(semaphores)
    while _semaphores:
        if time.time() - start > timeout:
            raise TimeoutError(_semaphores)
        if os.path.exists(_semaphores[0]):
            del _semaphores[0]
            continue
        time.sleep(tick)


def convert_dl_to_ld(args):
    """Convert dict of list to list of dict

    Parameters
    ----------
    args : :obj:`dict
        Dictionnary of lists.

    Returns
    -------
    :obj:list
        List of dictionnaries.
    """
    llist = None
    for k, v in args.items():
        if llist is not None:
            if len(v)!=llist:
                raise UnconsistencyArgument("Lists must have the same size")
        else:
            llist = len(v)
    return [dict(zip(args,t)) for t in zip(*args.values())]


class TemporaryFilesSet:
    """A context manager to handle a set of temporary files"""

    def __init__(self, keep_tempfiles=False):
        self._tempfiles = set()
        self._tempdirs = set()
        self.keep_tempfiles = keep_tempfiles

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cleanup()

    def add_files(self, *args):
        """Register temporary files to be destroyed when
        context manager exits.
        """
        self._tempfiles.update(args)

    def add_dirs(self, *args):
        """Register temporary directories to be destroyed when
        context manager exits.
        """
        self._tempdirs.update(args)

    def cleanup(self):
        if self.keep_tempfiles:
            return
        for f in self._tempfiles:
            try:
                os.remove(f)
            except Exception:
                print(f"warning: can't delete {f}")
        for d in self._tempdirs:
            try:
                shutil.rmtree(d)
            except Exception:
                print(f"warning: can't delete {d}")


def config_update(origin_config, args=None, install_conf_path=None, environ_var=None):
    """Configuration update sequence

    Update configuration values with the sequence priority:
    - Hard coded default values
    - Values defined in the configuration file referenced in install directory
    - Values defined in the configuration file referenced by `DRP_1DPIPE_STARTUP` enironment variable
    - Values defined in the configuration file set in the command line (--config argument)
    - Values defined by individual arguments in command line

    Parameters
    ----------
    origin_config : dict
        Configuration dictionary
    args : dict
        Arguments dictionary
    install_conf_path : str
        Path of the install configuration file
    
    Return
    ------
    :obj:`Config`
        Configuration object
    """
    config = ConfigJson(origin_config)
    # Update Config with drp_1dpipe.json content
    if install_conf_path is not None:
        config.load(install_conf_path)
    # Update Config with content of the specified environment variable if exists
    if environ_var is not None:
        if environ_var in os.environ.keys():
            config.load(os.environ[environ_var])
    # Update Config with config argument content if provided
    if args is not None:
        if 'config' in args.keys():
            if args['config'] is not None:
                config.load(args['config'])
        # Update Config with argument line values
        config.update(args)
    if hasattr(config, 'loglevel'):
        try:
            config.log_level = _loglevels[config.loglevel]
        except KeyError:
            raise KeyError("Unknown loglevel. Should be in [ERROR|WARNING|INFO|DEBUG], found : {}".format(config.loglevel))
    if hasattr(config, 'calibration_dir') and not os.path.isabs(config.calibration_dir):
        config.calibration_dir = os.path.join(config.workdir, config.calibration_dir)
    if hasattr(config, 'spectra_dir') and not os.path.isabs(config.spectra_dir):
        config.spectra_dir = os.path.join(config.workdir, config.spectra_dir)
    return config


def config_save(config, filename, indent=2):
    """Save configuration file

    The configuration file is saved into output directory

    Parameters
    ----------
    config : dict
        Configuration dictionary
    """
    os.makedirs(config.output_dir, exist_ok=True)
    path = os.path.join(config.output_dir, filename)
    config.save(path, indent=indent)
    