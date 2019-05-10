import concurrent.futures
import subprocess
import json


def single(command, args):
    """Run a single command on local host

    :param command: Path to command to execute
    :param args: extra parameters to give to command, as a dictionnary
    """
    task = [command]
    extra_args = ['--{}={}'.format(k, v)
                  for k, v in args.items() if k != 'pre-commands']
    task.extend(extra_args)
    subprocess.run(task)


def process_done_factory(notifier, node_id):
    """Notify task result"""
    def process_callback(future):
        if future.result().returncode != 0:
            notifier.update(node_id, state='ERROR')
        else:
            notifier.update(node_id, state='SUCCESS')
    return process_callback


def parallel(command, filelist, arg_name, seq_arg_name=None, args=None):
    """Run a command on local host

    Warning: pre-commands is not honored

    :param command: Path to command to execute
    :param filelist: JSON file. a list of list of FITS file
    :param arg_name: command-line argument name that will be given the
                     FITS-list file name
    :param seq_arg_name: command-line argument that will have appended a
                         sequence index
    :param args: extra parameters to give to command, as a dictionnary
    """

    # read list of tasks
    with open(filelist, 'r') as f:
        subtasks = json.load(f)

    extra_args = ['--{}={}'.format(k, v)
                  for k, v in args.items()
                  if k not in ('pre-commands', seq_arg_name,
                               'notifier')]

    # setup pipeline notifier
    notifier = args['notifier']
    notifier.update(command,
                    children=['{}-{}'.format(command, i)
                              for i in range(len(subtasks))])
    for i, arg_value in enumerate(subtasks):
        notifier.update('{}-{}'.format(command, i), state='WAITING')
    notifier.update(command, 'RUNNING')

    # process each task
    futures = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for i, arg_value in enumerate(subtasks):
            task = [command,
                    '--{arg_name}={arg_value}'.format(arg_name=arg_name,
                                                      arg_value=arg_value)]
            task.extend(extra_args)
            if seq_arg_name:
                task.append('--{}={}{}'.format(seq_arg_name,
                                               args[seq_arg_name], i))
            f = executor.submit(subprocess.run, task)
            f.add_done_callback(process_done_factory(args['notifier'],
                                                     '{}-{}'.format(command, i)))
            futures.append(f)
            args['notifier'].update('{}-{}'.format(command, i),
                                    state='RUNNING')

    if any([f.result().returncode != 0 for f in futures]):
        notifier.update(command, state='ERROR')
        raise Exception('A task returned non-zero value. '
                        '{}'.format([f.result() for f in futures]))
    else:
        notifier.update(command, state='SUCCESS')
