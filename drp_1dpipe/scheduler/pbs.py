import os
import textwrap
import subprocess
import tempfile
import uuid
from paramiko.client import SSHClient
from paramiko.sftp_client import SFTPClient


pbs_script_template = textwrap.dedent("""\
            #PBS -N pbs_pipeline_test
            #PBS -l nodes=1:ppn=1
            #PBS -l walltime=0:1:0
            #PBS -t 1-{jobs}
            cd $PBS_O_WORKDIR
            {executor_script} ${{PBS_ARRAYID}} >> out-{task_id}-${{PBS_ARRAYID}}.txt
            echo "$?" >> {task_id}-${{PBS_ARRAYID}}.done
            """)

def parallel(command, filelist, arg_name, args):
    """Run a command in parallel on a PBS cluster

    :param command: Path to command to execute
    :param filelist: JSON file. a list of list of FITS file
    :param arg_name: command-line argument name that will be given the FITS-list file name
    :param args: extra parameters to give to command, as a dictionnary
    """

    task_id = uuid.uuid4().hex
    executor_script = 'pbs_executor_{}.py'.format(task_id)

    # generate pbs_executor script
    tasks = []
    extra_args = ' '.join(['--{}={}'.format(k,v) for k,v in args.items()])

    with open(filelist, 'r') as f:
        subtasks = json.load(f)

    for arg_value in subtasks:
        tasks.append('{command} --{arg_name}={arg_value} {extra_args}'.format(command=command,
                                                                              arg_name=arg_name,
                                                                              arg_value=arg_value,
                                                                              extra_args=extra_args))

    with open(os.path.join(os.path.basedir(__file__), 'pbs_executor.py.in'), 'r') as f:
        pbs_executor = f.read().format(tasks=tasks)

    with open(os.path.join(arg.workdir, executor_script), 'w') as executor:
        executor.write(pbs_executor)

    # generate pbs script
    script = pbs_script_template.format(jobs=len(filelist),
                                        executor=executor_script,
                                        task_id=task_id)
    pbs_script_name = os.path.join(arg.workdir, 'pbs_script_{}.sh'.format(task_id))
    with open(pbs_script_name), 'w') as pbs_script:
        pbs_script.write(script)

    # run pbs
    result = subprocess.call('qsub {}'.format(pbs_script_name))
    assert result == 0


