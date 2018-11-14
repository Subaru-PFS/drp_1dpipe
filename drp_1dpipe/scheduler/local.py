import subprocess
import json

def single(command, args):
    """Run a single command on local host

    :param command: Path to command to execute
    :param args: extra parameters to give to command, as a dictionnary
    """
    task = [command]
    extra_args = ['--{}={}'.format(k,v) for k,v in args.items() if k != 'pre_commands']
    task.extend(extra_args)
    subprocess.run(task)

def parallel(command, filelist, arg_name, args):
    """Run a command on local host

    Warning: pre_commands is not honored

    :param command: Path to command to execute
    :param filelist: JSON file. a list of list of FITS file
    :param arg_name: command-line argument name that will be given the FITS-list file name
    :param args: extra parameters to give to command, as a dictionnary
    """

    # read list of tasks
    with open(filelist, 'r') as f:
        subtasks = json.load(f)

    extra_args = ['--{}={}'.format(k,v) for k,v in args.items() if k != 'pre_commands']

    # process each task
    for arg_value in subtasks:
        task = [command, '--{arg_name}={arg_value}'.format(arg_name=arg_name, arg_value=arg_value)]
        task.extend(extra_args)
        subprocess.run(task)
