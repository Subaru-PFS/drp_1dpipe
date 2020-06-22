import textwrap
from .batch import BatchQueue
from .runner import register_runner


class PBS(BatchQueue):

    batch_submitter = 'qsub'

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


register_runner(PBS)
