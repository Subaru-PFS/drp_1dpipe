import os.path
import logging
from logging.handlers import RotatingFileHandler


def get_auxiliary_path(file_name):
    """Return the full path of file in auxiliary directory

    :param file_name: name of the file
    :return: full path

    :Example:

    get_auxiliary_path("my_data.dat") # -> /python/package/path/my_data.dat
    """
    return os.path.join(os.path.dirname(__file__), 'auxdir', file_name)


def get_conf_path(file_name):
    """Return the full path of file in configuration directory

    :param file_name: name of the file
    :return: full path

    :Example:

    get_conf_path("my_conf.conf") # -> /python/package/path/my_conf.conf
    """
    return os.path.join(os.path.dirname(__file__), 'conf', file_name)


def init_logger(process_name):
    """initializes a logger depending on which module calls it.

    :param process_name: name of the module calling it.

    :Example:

    In define_program_options() of process_spectra.py :

    init_logger("pre_process")
    """
    logger = logging.getLogger(process_name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

    # file handler
    file_handler = logging.FileHandler(process_name + '.log', 'w')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)


def get_args_from_file(file_name, args):
    """Get arguments value from configuration file

    :param file_name: name of the configuration file
    :param args: object (ex. retrieve from argparse module)

    Get arguments value from configuration file. Value has to be formatted as
    ``option = string``. To comment use ``#``.
    The function modifies every ``None`` args attribue.
    The function add new args attribute.

    :Example:

    In ``my_conf.conf`` file:
    arg1 = 2
    arg2 = foo

    class MyCls():
        arg1=1

    args = MyCls()
    get_args_from_file("my_conf.conf",args)

    args.arg1 # -> 1
    args.arg2 # -> foo
    """
    with open(file_name, 'r') as ff:
        lines = ff.readlines()
    for line in lines:
        try:
            key, value = line.replace('\n', '').split('#')[0].split("=")
        except ValueError:
            continue
        try:
            if getattr(args, key.strip()) is None:
                setattr(args, key.strip(), value.strip())
        except AttributeError:
            setattr(args, key.strip(), value.strip())
