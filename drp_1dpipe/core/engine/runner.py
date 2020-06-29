import logging

from drp_1dpipe.core.utils import TemporaryFilesSet

_runners = {}


def register_runner(class_):
    """Register a runner
    
    Parameters
    ----------
    class_ : str
        Class name of the runner to register
    """
    _runners[class_.__name__.lower()] = class_


def get_runner(name):
    """Get a runner class by its name
    
    Parameters
    ----------
    name : str
        Runner name
    
    Return
    ------
    :obj:`runner`
        Runner class
    """
    try:
        return _runners[name.lower()]
    except KeyError:
        return False


def list_runners():
    """Return the list of registered runners"""
    return _runners.keys()


class Runner:
    """Abstract class for runner
    """

    def __init__(self, config, tmpcontext=None, logger=None):
        """Constructor

        Parameters
        ----------
        config : :obj:`dict`
            Runner configuration
        tmpcontext : :obj:TemporaryFilesSet, optional
            TemporaryFileSet context to use
        logger : :obj:Logger, optional
            Logger to use, by default None
        """
        self.concurrency = config.concurrency
        self.venv = config.venv
        self.workdir = config.workdir
        self.logdir = config.logdir
        if tmpcontext is None:
            self.tmpcontext = TemporaryFilesSet()
        else:
            self.tmpcontext = tmpcontext
        if logger is None:
            self.logger = logging.getLogger("runner")
        else:
            self.logger = logger

    def single(self, command, args):
        """Run a single command task

        Parameters
        ----------
        command : str
            Path to command to execute
        args : :obj:`dict`
            extra parameters to give to command

        Raises
        ------
        NotImplementedError
            Abstract class, should not be instanciated
        """        
        raise NotImplementedError

    def parallel(self, command, parallel_args, args):
        """Run a parallel command task.

        Parameters
        ----------
        command : str
            Path to command to execute
        parallel_args : 
            command line arguments related to each parallel task
        args : [type]
            command line arguments common to all parallel tasks

        Raises
        ------
        NotImplementedError
            Abstract class, should not be instanciated
        """        
        raise NotImplementedError
