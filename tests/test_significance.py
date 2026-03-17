import numpy as np

from patternfail.stats.multiple_testing import benjamini_hochberg
from patternfail.stats.significance import mc_pvalue


def test_mc_pvalue_bounds():
    p = mc_pvalue(2.0, np.array([1.0, 2.0, 3.0]))
    assert 0.0 <= p <= 1.0


def test_bh_bounds():
    q = benjamini_hochberg([0.01, 0.2, 0.05])
    assert all(0.0 <= x <= 1.0 for x in q)
from pattern_failures.significance import bh_fdr, mc_pvalue


def test_mc_pvalue_bounds():
    p = mc_pvalue(5, np.array([1, 2, 3]))
    assert 0 <= p <= 1


def test_bh_fdr_monotone():
    q = bh_fdr([0.01, 0.02, 0.5])
    assert all(0 <= x <= 1 for x in q)
