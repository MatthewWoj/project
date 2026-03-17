import numpy as np

from patternfail.stats.multiple_testing import benjamini_hochberg
from patternfail.stats.significance import mc_pvalue


def test_mc_pvalue_bounds():
    p = mc_pvalue(2.0, np.array([1.0, 2.0, 3.0]))
    assert 0.0 <= p <= 1.0


def test_bh_bounds():
    q = benjamini_hochberg([0.01, 0.2, 0.05])
    assert all(0.0 <= x <= 1.0 for x in q)
