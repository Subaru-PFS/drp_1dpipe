import pytest
import os
import logging
import logging.handlers
import tempfile

from drp_1dpipe.core.engine.runner import Runner, register_runner, get_runner, list_runners
from drp_1dpipe.core.config import Config
from drp_1dpipe.core.engine.local import Local

class RunnerClass(Runner):
    def single(self, command, args):
        self.logger.info("{} {}".format(command, args))


def test_register_runner():
    register_runner(RunnerClass)
    assert get_runner('RunnerClass') is RunnerClass


def test_runner():
    runner_class = get_runner('RunnerClass')
    config = Config({"concurrency":1, "venv":"/venv", "workdir":"/wd", "logdir":"/ld"})
    lf = tempfile.NamedTemporaryFile()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(lf.name)
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)
    runner = runner_class(config, tmpcontext=None, logger=logger)
    runner.single('command',{"k":"v"})
    with open(lf.name) as ff:
        line = ff.readline()
    assert line.strip() == "command {'k': 'v'}".strip()

def test_local():
    config = Config({"concurrency":1, "venv":"/venv", "workdir":"/wd", "logdir":"/ld"})
    runner = Local(config)
    task = runner.single("drp_1dpipe",{"version":0})
    assert task[0] == "drp_1dpipe"
    assert task[1] == "--version=0"
    tasks = runner.parallel("drp_1dpipe",{"version":[0,1]},{"arg":0})
    assert len(tasks) == 2
    assert tasks[0][0] == "drp_1dpipe"
    assert tasks[0][1] == "--version=0"
    assert tasks[0][2] == "--arg=0"
    assert tasks[1][0] == "drp_1dpipe"
    assert tasks[1][1] == "--version=1"
    assert tasks[1][2] == "--arg=0"
