import os
import shutil
import copy
import logging
import time
import datetime
import argparse
from drp_1dpipe import VERSION

_loglevels = {
    'CRITICAL': logging.CRITICAL,
    'FATAL': logging.CRITICAL,
    'ERROR': logging.ERROR,
    'WARNING': logging.WARNING,
    'WARN': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
    'NOTSET': logging.NOTSET,
}

class AbspathAction(argparse.Action):
    """Force absolute path
    """
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super(AbspathAction, self).__init__(option_strings, dest, **kwargs)
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, os.path.abspath(values))


class LogLevelAction(argparse.Action):
    """Parse --loglevel argument"""

    def __init__(self, option_strings, dest, **kwargs):
        super(LogLevelAction, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string):
        if values in _loglevels:
            setattr(namespace, self.dest, _loglevels[values])
        else:
            try:
                level = int(values)
                setattr(namespace, self.dest, level)
            except ValueError:
                raise logging.ArgumentError(f'Invalid log level {values}')


def init_argparse():
    """Initialize command-line argument parser with common arguments.

    :return: An initialized ArgumentParsel object.
    """
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--version', action='version', version=VERSION)
    parser.add_argument('--workdir', default=os.getcwd(), action=AbspathAction,
                        help='The root working directory where data is '
                        'located.')
    parser.add_argument('--logdir',
                        default=os.path.join(os.getcwd(), 'logdir'),
                        help='The logging directory.')
    parser.add_argument('--loglevel', default=30,
                        action=LogLevelAction,
                        help='The logging level. '
                        'One of ERROR, WARNING, INFO or DEBUG.')

    return parser


def get_auxiliary_path(file_name):
    """Get the full path of file in auxiliary directory.

    :param file_name: Name of the file.
    :return: Full path of auxiliary directory.

    :Example:

    get_auxiliary_path("my_data.dat") # -> /python/package/path/my_data.dat
    """
    return os.path.join(os.path.dirname(__file__), 'auxdir', file_name)


def get_conf_path(file_name):
    """Get the full path of file in configuration directory.

    :param file_name: Name of the file.
    :return: Full path of configuration file.

    :Example:

    get_conf_path("my_conf.conf") # -> /python/package/path/my_conf.conf
    """
    return os.path.join(os.path.dirname(__file__), 'conf', file_name)


def init_logger(process_name, logdir, loglevel):
    """Initializes a logger depending on which module calls it.

    :param process_name: name of the module calling it.

    :Example:

    In define_program_options() of process_spectra.py :

    init_logger("pre_process")
    """

    os.makedirs(logdir, exist_ok=True)

    logger = logging.getLogger(process_name)
    logger.setLevel(loglevel)
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

    # file handler
    file_handler = logging.FileHandler(os.path.join(logdir,
                                                    process_name + '.log'),
                                       'w')
    file_handler.setLevel(loglevel)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(loglevel)
    logger.addHandler(stream_handler)


def init_environ(workdir):
    """Initializes the working environment.

    :param workdir: name of the working directory.
    """
    os.environ['WORKDIR']=os.path.normpath(os.path.expanduser(workdir))


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


def save_config_file(args):
    """Save the main config file

    :param args: the argparse Namespace object
    """
    outdir = normpath(args.workdir, args.output_dir)
    os.makedirs(outdir, exist_ok=True)
    with open(normpath(outdir, 'config.conf'), 'w') as ff:
        dargs=vars(args)
        ff.write("# Edited on {}\n".format(
            datetime.datetime.now().isoformat(timespec='seconds'))
            )
        for arg in dargs.keys():
            ff.write("{} = {}\n".format(arg, dargs[arg]))


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


class TemporaryFilesSet:
    """A context manager to handle a set of temporary files"""

    def __init__(self, keep_tempfiles=False):
        self._tempfiles = []
        self._tempdirs = []
        self.keep_tempfiles = keep_tempfiles

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cleanup()

    def add_files(self, *args):
        """Register temporary files to be destroyed when
        context manager exits.
        """
        self._tempfiles.extend(args)

    def add_dirs(self, *args):
        """Register temporary directories to be destroyed when
        context manager exits.
        """
        self._tempdirs.extend(args)

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
