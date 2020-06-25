import textwrap

from drp_1dpipe.core.engine.batch import BatchQueue
from drp_1dpipe.core.engine.runner import register_runner


class Slurm(BatchQueue):

    batch_submitter = 'sbatch'

    single_script_template = textwrap.dedent("""\
                #!/bin/bash
                #SBATCH --export=NONE
                #SBATCH --job-name={command}
                #SBATCH --time=00:02:00
                #SBATCH --ntasks=1
                #SBATCH --mem-per-cpu=1

                cd {workdir}
                source {venv}/bin/activate
                {command} {extra_args} >> out-{task_id}.txt
                echo "$?" >> {workdir}/{task_id}.done
                """)

    parallel_script_template = textwrap.dedent("""\
                #!/bin/bash
                #SBATCH --export=NONE
                #SBATCH --job-name={executor_script}
                #SBATCH --time=01:00:00
                #SBATCH --ntasks=1
                #SBATCH --array=1-{jobs}

                cd {workdir}
                source {venv}/bin/activate
                /usr/bin/env python3 {executor_script} ${{SLURM_ARRAY_TASK_ID}} >> out-{task_id}-${{SLURM_ARRAY_TASK_ID}}.txt
                echo "$?" >> {workdir}/{task_id}_${{SLURM_ARRAY_TASK_ID}}.done
                """)


register_runner(Slurm)
