import os
import json
import subprocess
import uuid
from drp_1dpipe.core.utils import normpath, wait_semaphores, convert_dl_to_ld
from drp_1dpipe.core.engine.runner import Runner

class BatchQueue(Runner):

    batch_submitter = "# Program that queue a task"
    single_script_template = "# Batch script for single task"

    parallel_script_template = "# Batch script for parallel task"

    def single(self, command, args):
        """Run a single command using batch queue."""

        task_id = uuid.uuid4().hex

        # generate batch script
        extra_args = ' '.join(['--{}={}'.format(k, v) for k, v in args.items()])

        script = self.single_script_template.format(workdir=normpath(self.workdir),
                                                    venv=self.venv,
                                                    command=command,
                                                    extra_args=extra_args,
                                                    task_id=task_id)
        batch_script_name = normpath(self.workdir,
                                     'batch_script_{}.sh'.format(task_id))
        with open(batch_script_name, 'w') as batch_script:
            batch_script.write(script)
        self.tmpcontext.add_files(batch_script_name)

        # run batch
        result = subprocess.run([self.batch_submitter, batch_script_name])
        assert result.returncode == 0

        # block until completion
        semaphores = [normpath(self.workdir, '{}.done'.format(task_id))]
        self.tmpcontext.add_files(*semaphores)
        wait_semaphores(semaphores)
        return batch_script_name

    def parallel(self, command, parallel_args=None, args=None):
        """Execute parallel task for batch runners

        Parameters
        ----------
        command : str
            Path to command to execute
        parallel_args : dict, optional
            command line arguments to related to each parallel task, by default None
        args : dict, optional
            command line arguments common to all parallel tasks, by default None
        """
        task_id = uuid.uuid4().hex
        executor_script = normpath(self.workdir, 'batch_executor_{}.py'.format(task_id))
        self.tmpcontext.add_files(executor_script)

        # Convert dictionnary of list to list of dictionnaries
        pll_args = convert_dl_to_ld(parallel_args)

        # generate batch_executor script
        tasks = []
        extra_args = ['--{}={}'.format(k, v)
                      for k, v in args.items()]
                    #   if k not in ('pre-commands', seq_arg_name, 'notifier')]

        # setup tasks
        # with open(filelist, 'r') as f:
        #     subtasks = json.load(f)
        #     # register these files for deletion
        #     self.tmpcontext.add_files(*subtasks)

        for k, v in parallel_args.items():
            task = [command,
                    '--{arg_name}={arg_value}'.format(arg_name=k,
                                                      arg_value=v)]
            task.extend(extra_args)
            tasks.append(task)

        # for i, arg_value in enumerate(subtasks):
        #     task = [command,
        #             '--{arg_name}={arg_value}'.format(arg_name=arg_name,
        #                                               arg_value=arg_value)]
        #     task.extend(extra_args)
        #     if seq_arg_name:
        #         [task.append('--{}={}'.format(
        #           seq_arg,
        #           os.path.join(args[seq_arg], 'B'+str(i)))
        #           ) for seq_arg in seq_arg_name]
        #     tasks.append(task)

        # setup pipeline notifier
        # notifier = args['notifier']
        # notifier.update(command,
        #                 children=['{}-{}'.format(command, i)
        #                           for i in range(ntasks)])
        # for i in range(ntasks):
        #     notifier.update('{}-{}'.format(command, i), state='WAITING')
        # notifier.update(command, 'RUNNING')

        # generate batch script
        with open(os.path.join(os.path.dirname(__file__), 'resources', 'executor.py.in'),
                  'r') as f:
            batch_executor = f.read().format(tasks=tasks, notification_url='')
            # batch_executor = f.read().format(tasks=tasks,
            #                                  notification_url=(notifier.pipeline_url
            #                                                    if notifier.pipeline_url
            #                                                    else ''))
        with open(executor_script, 'w') as executor:
            executor.write(batch_executor)

        # generate batch script
        script = self.parallel_script_template.format(jobs=len(tasks),
                                                      workdir=normpath(self.workdir),
                                                      pre_commands=self.venv,
                                                      executor_script=executor_script,
                                                      task_id=task_id)
        batch_script_name = normpath(self.workdir,
                                     f'batch_script_{task_id}.sh')
        with open(batch_script_name, 'w') as batch_script:
            batch_script.write(script)
        self.tmpcontext.add_files(batch_script_name)

        # run batch
        result = subprocess.run([self.batch_submitter, batch_script_name])
        assert result.returncode == 0

        # wait all sub-tasks
        semaphores = [normpath(self.workdir, f'{task_id}_{i}.done')
                      for i in range(1, ntasks+1)]
        self.tmpcontext.add_files(*semaphores)

        wait_semaphores(semaphores)
        # notifier.update(command, 'SUCCESS')
