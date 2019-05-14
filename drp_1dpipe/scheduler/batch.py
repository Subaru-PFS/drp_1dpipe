import os
import json
import subprocess
import uuid
from drp_1dpipe.io.utils import normpath, wait_semaphores
from .runner import Runner


class BatchQueue(Runner):

    batch_submitter = "# Program that queue a task"
    single_script_template = "# Batch script for single task"

    parallel_script_template = "# Batch script for parallel task"

    def single(self, command, args):
        """Run a single command using batch queue."""

        task_id = uuid.uuid4().hex

        # generate batch script
        extra_args = ' '.join(['--{}={}'.format(k, v)
                               for k, v in args.items() if k != 'pre-commands'])

        script = self.single_script_template.format(workdir=normpath(args['workdir']),
                                                    pre_commands=args['pre-commands'],
                                                    command=command,
                                                    extra_args=extra_args,
                                                    task_id=task_id)
        batch_script_name = normpath(args['workdir'],
                                     'batch_script_{}.sh'.format(task_id))
        with open(batch_script_name, 'w') as batch_script:
            batch_script.write(script)
        self.tmpcontext.add_files(batch_script_name)

        # run batch
        result = subprocess.run(['sbatch', batch_script_name])
        assert result.returncode == 0

        # block until completion
        semaphores = [normpath(args['workdir'], '{}.done'.format(task_id))]
        self.tmpcontext.add_files(*semaphores)
        wait_semaphores(semaphores)

    def parallel(self, command, filelist, arg_name, seq_arg_name=None, args=None):
        """Run a command in parallel using batch queue.

        :param command: Path to command to execute
        :param filelist: JSON file. a list of list of FITS file
        :param arg_name: command-line argument name that will be given the FITS-list file name
        :param seq_arg_name: command-line argument that will have appended a sequence index
        :param args: extra parameters to give to command, as a dictionnary
        """

        task_id = uuid.uuid4().hex
        executor_script = normpath(args['workdir'], 'batch_executor_{}.py'.format(task_id))
        self.tmpcontext.add_files(executor_script)

        # generate batch_executor script
        tasks = []
        extra_args = ['--{}={}'.format(k, v)
                      for k, v in args.items()
                      if k not in ('pre-commands', seq_arg_name, 'notifier')]

        # setup tasks
        with open(filelist, 'r') as f:
            subtasks = json.load(f)
            # register these files for deletion
            self.tmpcontext.add_files(*subtasks)

        for i, arg_value in enumerate(subtasks):
            task = [command,
                    '--{arg_name}={arg_value}'.format(arg_name=arg_name,
                                                      arg_value=arg_value)]
            task.extend(extra_args)
            if seq_arg_name:
                task.append('--{}={}{}'.format(seq_arg_name,
                                               args[seq_arg_name], i))
            tasks.append(task)

        # setup pipeline notifier
        notifier = args['notifier']
        notifier.update(command,
                        children=['{}-{}'.format(command, i)
                                  for i in range(len(subtasks))])
        for i, arg_value in enumerate(subtasks):
            notifier.update('{}-{}'.format(command, i), state='WAITING')
        notifier.update(command, 'RUNNING')

        # generate batch script
        with open(os.path.join(os.path.dirname(__file__), 'executor.py.in'),
                  'r') as f:
            batch_executor = f.read().format(tasks=tasks,
                                             notification_url=(notifier.pipeline_url
                                                               if notifier.pipeline_url
                                                               else ''))
        with open(executor_script, 'w') as executor:
            executor.write(batch_executor)

        # generate batch script
        script = self.parallel_script_template.format(jobs=len(subtasks),
                                                      workdir=normpath(args['workdir']),
                                                      pre_commands=args['pre-commands'],
                                                      executor_script=executor_script,
                                                      task_id=task_id)
        batch_script_name = normpath(args['workdir'],
                                     f'batch_script_{task_id}.sh')
        with open(batch_script_name, 'w') as batch_script:
            batch_script.write(script)
        self.tmpcontext.add_files(batch_script_name)

        # run batch
        result = subprocess.run([self.batch_submitter, batch_script_name])
        assert result.returncode == 0

        # wait all sub-tasks
        semaphores = [normpath(args['workdir'], f'{task_id}_{i}.done')
                      for i in range(1, len(subtasks)+1)]
        self.tmpcontext.add_files(*semaphores)

        wait_semaphores(semaphores)
        notifier.update(command, 'SUCCESS')
