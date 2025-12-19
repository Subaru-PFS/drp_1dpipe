from drp_1dpipe.io.redshiftCoCandidates import filter_warning


def test_filter_warning():
    """
    Check the behavior of filter_warning function
    """
    assert filter_warning(5,[0]) == 1
    assert filter_warning(5,[2]) == 4
    assert filter_warning(5,[1]) == 0
    assert filter_warning(5,[0, 2]) == 5
    assert filter_warning(7,[0, 1, 2]) == 7