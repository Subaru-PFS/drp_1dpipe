import os
import json
import textwrap
import subprocess
import tempfile
import uuid
from drp_1dpipe.io.utils import normpath, wait_semaphores


single_script_template = textwrap.dedent("""\
            #PBS -N {command}
            #PBS -l nodes=1:ppn=1
            #PBS -l walltime=00:05:00
            cd {workdir}
            {pre_commands}
            {command} {extra_args} >> out-{task_id}.txt
            echo "$?" >> {workdir}/{task_id}.done
            """)

parallel_script_template = textwrap.dedent("""\
            #PBS -N {executor_script}
            #PBS -l nodes=1:ppn=1
            #PBS -l walltime=01:00:00
            #PBS -t 1-{jobs}
            cd {workdir}
            {pre_commands}
            /usr/bin/env python3 {executor_script} ${{PBS_ARRAYID}} >> out-{task_id}-${{PBS_ARRAYID}}.txt
            echo "$?" >> {workdir}/{task_id}_${{PBS_ARRAYID}}.done
            """)

def _build_pre_commands(pre_commands):
    """Build pre-commands string."""

    _cmds = eval(pre_commands)
    if not isinstance(_cmds, list):
        raise Exception("pre_commands must be a list of commands")
    for command in _cmds:
        if not isinstance(command, list):
            raise Exception("each command must be a list of words, as `args` in subprocess.run()")

    result = '\n'.join([' '.join([w for w in command]) for command in _cmds])
    return result

def single(command, args):
    """Run a single command on a PBS cluster.

    :param command: Path to command to execute
    :param args: extra parameters to give to command, as a dictionnary
    """

    task_id = uuid.uuid4().hex

    # generate pbs script
    extra_args = ' '.join(['--{}={}'.format(k,v) for k,v in args.items() if k != 'pre_commands'])
    pre_commands = _build_pre_commands(args['pre_commands'])

    script = single_script_template.format(workdir=normpath(args['workdir']),
                                           pre_commands=pre_commands,
                                           command=command,
                                           extra_args=extra_args,
                                           task_id=task_id)
    pbs_script_name = normpath(args['workdir'], 'pbs_script_{}.sh'.format(task_id))
    with open(pbs_script_name, 'w') as pbs_script:
        pbs_script.write(script)

    # run pbs
    result = subprocess.run(['qsub', pbs_script_name])
    assert result.returncode == 0

    # block until completion
    wait_semaphores([normpath(args['workdir'], '{}.done'.format(task_id))])

def parallel(command, filelist, arg_name, seq_arg_name=None, args=None):
    """Run a command in parallel on a PBS cluster.

    :param command: Path to command to execute
    :param filelist: JSON file. a list of list of FITS file
    :param arg_name: command-line argument name that will be given the FITS-list file name
    :param seq_arg_name: command-line argument that will have appended a sequence index
    :param args: extra parameters to give to command, as a dictionnary
    """

    task_id = uuid.uuid4().hex
    executor_script = normpath(args['workdir'], 'pbs_executor_{}.py'.format(task_id))

    # generate pbs_executor script
    tasks = []
    extra_args = ['--{}={}'.format(k, v)
                  for k, v in args.items()
                  if k not in ('pre_commands', seq_arg_name)]

    with open(filelist, 'r') as f:
        subtasks = json.load(f)

    for i, arg_value in enumerate(subtasks):
        task = [command, '--{arg_name}={arg_value}'.format(arg_name=arg_name, arg_value=arg_value)]
        task.extend(extra_args)
        if seq_arg_name:
            task.append('--{}={}{}'.format(seq_arg_name, args[seq_arg_name], i))
        tasks.append(task)

    with open(os.path.join(os.path.dirname(__file__), 'pbs_executor.py.in'), 'r') as f:
        pbs_executor = f.read().format(tasks=tasks)

    with open(executor_script, 'w') as executor:
        executor.write(pbs_executor)

    # generate pbs script
    pre_commands = _build_pre_commands(args['pre_commands'])

    script = parallel_script_template.format(jobs=len(subtasks),
                                             workdir=normpath(args['workdir']),
                                             pre_commands=pre_commands,
                                             executor_script=executor_script,
                                             task_id=task_id)
    pbs_script_name = normpath(args['workdir'], 'pbs_script_{}.sh'.format(task_id))
    with open(pbs_script_name, 'w') as pbs_script:
        pbs_script.write(script)

    # run pbs
    result = subprocess.run(['qsub', pbs_script_name])
    assert result.returncode == 0

    # wait all sub-tasks
    semaphores = [normpath(args['workdir'], '{}_{}.done'.format(task_id, i))
                  for i in range(1, len(subtasks)+1)]
    wait_semaphores(semaphores)

