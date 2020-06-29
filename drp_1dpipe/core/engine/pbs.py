import textwrap
from drp_1dpipe.core.engine.batch import BatchQueue
from drp_1dpipe.core.engine.runner import register_runner


class PBS(BatchQueue):

    batch_submitter = 'qsub'

    single_script_template = textwrap.dedent("""\
                #PBS -N {command}
                #PBS -l nodes=1:ppn=1
                #PBS -l walltime=00:05:00
                cd {workdir}
                source {venv}/bin/activate
                {command} {extra_args} >> out-{task_id}.txt
                echo "$?" >> {workdir}/{task_id}.done
                """)

    parallel_script_template = textwrap.dedent("""\
                #PBS -N {executor_script}
                #PBS -l nodes=1:ppn=1
                #PBS -l walltime=01:00:00
                #PBS -t 1-{jobs}
                cd {workdir}
                source {venv}/bin/activate
                /usr/bin/env python3 {executor_script} ${{PBS_ARRAYID}} >> out-{task_id}-${{PBS_ARRAYID}}.txt
                echo "$?" >> {workdir}/{task_id}_${{PBS_ARRAYID}}.done
                """)


register_runner(PBS)
