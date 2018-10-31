"""
File: drp_1dpipe/tests/test_utils.py

Created on: 31/10/18
Author: PSF DRP1D developers
"""

import pytest
import os.path
from drp_1dpipe.io.utils import get_auxiliary_path, get_conf_path


def test_auxdir():
    """
    @brief The "test_auxdir" function.
    @details
        This function test features concerning auxiliary directory.
    """
    assert os.path.exists(get_auxiliary_path("."))
    assert not(os.path.exists(get_auxiliary_path("foo.txt")))


def test_confdir():
    """
    @brief The "test_confdir" function.
    @details
        This function test features concerning configuration directory.
    """
    assert os.path.exists(get_conf_path("."))
    assert not(os.path.exists(get_conf_path("foo.txt")))
