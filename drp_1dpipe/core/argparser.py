import os
import argparse
import logging

from drp_1dpipe import VERSION
from pylibamazed.redshift import get_version

_loglevels = {
    'ERROR': logging.ERROR,
    'WARNING': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
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


class ShowParametersAction(argparse.Action):

    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super(ShowParametersAction, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if values == 'galaxy+star+qso':
            params_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                       "../auxdir/parameters_stellar_galaxy_qso.json")
        elif values == 'galaxy':
            params_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../auxdir/parameters_galaxy.json")
        elif values == 'star':
            params_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../auxdir/parameters_stellar.json")
        else:
            raise logging.ArgumentError(f'Invalid parameter confi {values}')
        setattr(namespace, self.dest, values)
        with open(params_path, 'r') as f:
            print(f.read())
    
    
def define_global_program_options(parser):
    """Initilize command line argument parser with common arguments.

    Parameters
    ----------
    parser : :obj:`ArgumentParser`
        An initialized ArgumentParser object
    """

    parser.add_argument('--config', '-c', metavar='FILE', action=AbspathAction,
                        help='Configuration file giving all these command line arguments')
    parser.add_argument('--workdir', default=os.getcwd(), action=AbspathAction,
                        help='The root working directory where data is located.')
    parser.add_argument('--logdir', action=AbspathAction,
                        help='The logging directory.')
    parser.add_argument('--loglevel',
                    choices=['ERROR', 'WARNING', 'INFO', 'DEBUG'],
                    # action=LogLevelAction,
                    help='The logging level. One of ERROR, WARNING, INFO or DEBUG.')
    parser.add_argument('-v', '--version', action='version',
                        version='%(prog)s {} (pylibamazed {})'.format(VERSION, get_version()))
