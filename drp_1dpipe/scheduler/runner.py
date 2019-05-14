from drp_1dpipe.io.utils import TemporaryFilesSet

_runners = {}


def register_runner(class_):
    """Register a runner"""
    _runners[class_.__name__.lower()] = class_


def get_runner(name):
    """Get a runner by its name"""
    try:
        return _runners[name.lower()]
    except KeyError:
        return False


def list_runners():
    """Return the list of registered runners"""
    return _runners.keys()


class Runner:

    def __init__(self, tmpcontext=None):
        """
        :param tmpcontext: TemporaryFileSet context to use
        """
        if tmpcontext is None:
            self.tmpcontext = TemporaryFilesSet()
        else:
            self.tmpcontext = tmpcontext

    def single(self, command, args):
        """Run a single command.

        :param command: Path to command to execute
        :param args: extra parameters to give to command, as a dictionnary
        """
        raise NotImplementedError

    def parallel(self, command, filelist, arg_name,
                 seq_arg_name=None, args=None):
        """Run a command in parallel.

        :param command: Path to command to execute
        :param filelist: JSON file. a list of list of FITS file
        :param arg_name: command-line argument name that will be given the
                         FITS-list file name
        :param seq_arg_name: command-line argument that will have appended a
                             sequence index
        :param args: extra parameters to give to command, as a dictionnary
        """
        raise NotImplementedError
