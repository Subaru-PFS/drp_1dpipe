import concurrent.futures
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

def parallel(command, filelist, arg_name, seq_arg_name=None, args=None):
    """Run a command on local host

    Warning: pre_commands is not honored

    :param command: Path to command to execute
    :param filelist: JSON file. a list of list of FITS file
    :param arg_name: command-line argument name that will be given the FITS-list file name
    :param seq_arg_name: command-line argument that will have appended a sequence index
    :param args: extra parameters to give to command, as a dictionnary
    """

    # read list of tasks
    with open(filelist, 'r') as f:
        subtasks = json.load(f)

    extra_args = ['--{}={}'.format(k, v)
                  for k, v in args.items()
                  if k not in ('pre_commands', seq_arg_name)]

    # process each task
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for i, arg_value in enumerate(subtasks):
            task = [command, '--{arg_name}={arg_value}'.format(arg_name=arg_name, arg_value=arg_value)]
            task.extend(extra_args)
            if seq_arg_name:
                task.append('--{}={}{}'.format(seq_arg_name, args[seq_arg_name], i))
            executor.submit(subprocess.run, task)
