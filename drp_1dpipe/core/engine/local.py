import concurrent.futures
import subprocess
import json
import os.path
import logging

from drp_1dpipe.core.utils import convert_dl_to_ld
from .runner import Runner, register_runner

class Local(Runner):

    def single(self, command, args):
        """Run a single command on local host

        Parameters
        ----------
        command : str
            Path to command to execute
        args : :obj:`dict`
            extra parameters to give to command
        """        
        task = [command]
        extra_args = ['--{}={}'.format(k, v) for k, v in args.items()]
        task.extend(extra_args)
        subprocess.run(task)
        return task

    # def process_done_factory(self, notifier, node_id):
    #     """Notify task result"""
    #     def process_callback(future):
    #         if future.result().returncode != 0:
    #             notifier.update(node_id, state='ERROR')
    #         else:
    #             notifier.update(node_id, state='SUCCESS')
    #     return process_callback

    def parallel(self, command, parallel_args=None, args=None):
        """Run a parallel command on local host

        Parameters
        ----------
        command : str
            Path to command to execute
        parallel_args : :obj:`dict`, optional
            command line arguments related to each parallel task, by default None
        args : :obj:`dict`, optional
            command line arguments common to all tasks, by default None
        """
        # read list of tasks
        # with open(filelist, 'r') as f:
        #     subtasks = json.load(f)
        #     # register these files for deletion
        #     self.tmpcontext.add_files(*subtasks)

        # Convert dictionnary list to list of dictionnaries
        pll_args = convert_dl_to_ld(parallel_args)

        extra_args = ['--{}={}'.format(k, v)
                      for k, v in args.items()]

        # setup pipeline notifier
        # notifier = args['notifier']
        # notifier.update(command,
        #                 children=['{}-{}'.format(command, i)
        #                           for i in range(ntasks)])
        # for i in range(ntasks):
        #     notifier.update('{}-{}'.format(command, i), state='WAITING')
        # notifier.update(command, 'RUNNING')

        # process each task
        futures = []
        tasks = []
        with concurrent.futures.ProcessPoolExecutor(self.concurrency) as executor:
            for i, arg_value in enumerate(pll_args):
                task = [command]
                for k, v in arg_value.items():
                    task.append('--{arg_name}={arg_value}'.format(arg_name=k, arg_value=v))
                task.extend(extra_args)
                # if seq_arg_name:
                #     [task.append('--{}={}'.format(
                #       seq_arg,
                #       os.path.join(args[seq_arg], 'B'+str(i)))
                #       ) for seq_arg in seq_arg_name]
                # logger.info(" ".join(task))
                f = executor.submit(subprocess.run, task)
                # f.add_done_callback(self.process_done_factory(args['notifier'],
                #                                               '{}-{}'.format(command, i)))
                futures.append(f)
                tasks.append(task)
                # args['notifier'].update('{}-{}'.format(command, i),
                #                         state='RUNNING')

        return tasks
        # if any([f.result().returncode != 0 for f in futures]):
        #     notifier.update(command, state='ERROR')
        #     raise Exception('A task returned non-zero value. '
        #                     '{}'.format([f.result() for f in futures]))
        # else:
        #     notifier.update(command, state='SUCCESS')


register_runner(Local)
