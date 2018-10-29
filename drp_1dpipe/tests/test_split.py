"""
File: drp_1dpipe/tests/test_split.py

Created on: 25/10/18
Author: CeSAM
"""

import pytest
import random
from tempfile import NamedTemporaryFile
from drp_1dpipe.split.split import bunch, split

def test_bunch():
    """
    @brief The "test_bunch" function.
    @details
        This function test the "bunch" function of "split.py" module.
    """

    # "bunch" function argument : list containing fits to process
    # set a random number of element in this list
    d = {}
    my_list = []
    input_fits_number = random.randint(1, 100)
    bunch_size = random.randint(1, 100)

    for f in range(1, input_fits_number):
        d["{0}".format(f)] = NamedTemporaryFile()

    for kv in d.values():
        my_list.append(kv)

    res = bunch(my_list, bunch_size)
    assert res is not None


def test_split():
    class Args():
        workdir = 'my_workdir'
        spectra_sets_list = 0
        bunch_size = 3

        def my_function(self):

            my_args = Args()
            split(self.spectra_sets_list, self.bunch_size)

            assert self.spectra_sets_list == 1
