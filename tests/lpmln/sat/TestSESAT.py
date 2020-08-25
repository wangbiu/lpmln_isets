
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-08-25 15:33
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : TestSESAT.py
"""

from lpmln.sat.ASPSEChecking import ASPSEChecking
from lpmln.sat.LPMLNSEChecking import LPMLNSEChecking


valid_rules = [
    ({1}, {1}, {}),
    ({2}, {1}, {1}),
    ({3}, {2}, {1}, {1}),
    ({1}, {}, {1, 2})
]


def test_lpmln_se_valid():
    for r in valid_rules:
        se_sat = LPMLNSEChecking.se_check_kmn_program([], [r], [])
        print(se_sat)


def test_asp_se_valid():
    for r in valid_rules:
        se_sat = ASPSEChecking.se_check_kmn_program([], [r], [])
        print(se_sat)


if __name__ == '__main__':
    pass
    