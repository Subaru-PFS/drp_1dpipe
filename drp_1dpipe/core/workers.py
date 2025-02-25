import argparse
import concurrent.futures
import copy
import json
import os
import subprocess
import textwrap
import time
import traceback
import uuid
from .utils import normpath
#
# Workers registry
#
_workers = {}


def register_worker(class_):
    """Register a worker"""
    _workers[class_.__name__.lower()] = class_


def get_worker(name):
    """Get a worker by its name"""
    try:
        return _workers[name.lower()]
    except KeyError:
        return False


def list_workers():
    """Return the list of registered workers"""
    return _workers.keys()


#
# Workers implementations
#

class Worker:
    def __init__(self, config):
        self.config = config
        self.concurrency = config.concurrency
        self.pre_command = ''
        self.output_dir = config.output_dir

    def get_process_spectra_args(self, args):
        bunch_id = args[1]
        spectra_listfile = os.path.join(self.output_dir,f'spectralist_B{bunch_id}.json')
        bunch_dir = os.path.join(self.output_dir,f'B{bunch_id}')
        log_dir = os.path.join(self.output_dir,'log',f'B{bunch_id}')
        ps_args = dict()
        ps_args["workdir"]=self.config.workdir
        ps_args["output_dir"]=self.output_dir
        ps_args["spectra_dir"]=self.config.spectra_dir
        if self.config.parameters_file:
            ps_args["parameters_file"]=self.config.parameters_file
        ps_args["spectra_listfile"]=spectra_listfile
        ps_args["logdir"]=log_dir

        ps_args_list = ["process_spectra"]
        for k,v in ps_args.items():
            ps_args_list.append(f'--{k}')
            ps_args_list.append(v)

        return ps_args_list

    def get_merge_args(self):
        return ["merge_results","--workdir",self.config.workdir,"--output_dir",self.output_dir]
    
    def run(self,  args):
        pass

    def wait_all(self):
        pass


class Local(Worker):
    """Multi-process worker"""

    def __init__(self, config):
        super().__init__(config)
        if self.concurrency <= 0:
            concurrency = 1
        else:
            concurrency = self.concurrency
        self.executor = concurrent.futures.ThreadPoolExecutor(concurrency)
        self.futures = []

        
    def run(self,  args):
        if args[0] == "process_spectra":
            sp = self.get_process_spectra_args(args)
        else:
            sp = self.get_merge_args()
        f = self.executor.submit(subprocess.run, sp)

        self.futures.append(f)

    def wait_all(self):
        concurrent.futures.wait(self.futures)


register_worker(Local)

class Debug(Worker):
    """Multi-process worker"""

    def __init__(self, config):
        super().__init__(config)

        
    def run(self,  args):
        return 
        
    def wait_all(self):
        return


register_worker(Debug)

class Slurm(Worker):
    """SLURM worker"""

    batch_submitter = 'sbatch'

    script_template = textwrap.dedent("""\
            #!/bin/bash
            #SBATCH --export=NONE
            #SBATCH --job-name=process_{bunch_id}
            #SBATCH --time={walltime}
            #SBATCH --mem={mem}
            #SBATCH --ntasks=1
            #SBATCH --array=1-{jobs}%{concurrency}

            cd {workdir}
            {pre_command}

            {process_spectra_args}
            
            """)

    finish_template = textwrap.dedent("""\
            #!/bin/bash
            #SBATCH --export=NONE
            #SBATCH --job-name=finalize
            #SBATCH --time={walltime}
            #SBATCH --mem={mem}
            #SBATCH --ntasks=1
            {pre_command}

            cd {workdir}
            {merge_args}

            """)
    def __init__(self, config):
        super().__init__(config)
        self.pre_command = "source " + os.environ["VIRTUAL_ENV"] + "/bin/activate"
        
        self.walltime = "02:00:00"
        self.mem = "3G"
        self.workdir = os.getcwd()
        self.job_ids = []

    def wait_all(self):
        return 0
    
    def run(self, args):
        if args[0] == "process_spectra":
            bunch_id = args[1]
            script = self.script_template.format(jobs=1, #self.nb_jobs,
                                                 workdir=self.workdir,
                                                 pre_command=self.pre_command or '',
                                                 walltime=self.walltime,
                                                 mem=self.mem,
                                                 concurrency=self.concurrency,
                                                 bunch_id = bunch_id,
                                                 process_spectra_args = " ".join(self.get_process_spectra_args(args))
            )
            if self.concurrency == -1:
                # no limit is given, let's get rid of this option
                script = script.replace("%-1","")

            spath = os.path.join(self.output_dir,f"process_spectra_{bunch_id}.sh")
            with open(spath,"w") as f:
                f.write(script)
                f.close()
            output = subprocess.run(["sbatch",spath],stdout=subprocess.PIPE).stdout.decode()
            job_id = output.strip().split(" ")[-1]
            self.job_ids.append(job_id)
        if args[0] == "merge_results":
            script = self.finish_template.format(pre_command=self.pre_command or '',
                                                 walltime=self.walltime,
                                                 mem=self.mem,
                                                 workdir=self.workdir,
                                                 merge_args=" ".join(self.get_merge_args()))
            spath = os.path.join(self.output_dir,f"finalize")
            with open(spath,'w') as f:
                f.write(script)
                f.close()
            subprocess.run(["sbatch",f"--depend=afterany:{','.join(self.job_ids)}",spath])
            
register_worker(Slurm)

