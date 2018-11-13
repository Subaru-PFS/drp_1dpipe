import subprocess

def parallel(command, filelist, arg_name, args):
    """Run a command on local host

    :param command: Path to command to execute
    :param filelist: JSON file. a list of list of FITS file
    :param arg_name: command-line argument name that will be given the FITS-list file name
    :param args: extra parameters to give to command, as a dictionnary
    """

    # read list of tasks
    with open(filelist, 'r') as f:
        subtasks = json.load(f)

    extra_args = ' '.join(['--{}={}'.format(k,v) for k,v in args.items()])

    # process each task
    for arg_value in subtasks:
        subprocess.call('{command} --{arg_name}={arg_value} {extra_args}'.format(command=command,
                                                                                 arg_name=arg_name,
                                                                                 arg_value=arg_value,
                                                                                 extra_args=extra_args))
