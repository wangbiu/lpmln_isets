
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/4 20:41
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : TestSAT.py
"""

from lpmln.sat.ASPSAT import ASPSAT
from lpmln.sat.LPMLNSAT import LPMLNSAT


here_interpretation = [1]
there_interpretation = [1, 3]

rules = [
    [[1], [1, 2], [3]],
    [[1], [2], [3]],
    [[1], [], [1, 2]],
    [[], [2, 3], [1, 3]],
    [[1], [1], [1]],
    [[], [1, 3], []],
]


def test_asp_sat_rule():
    for rule in rules:
        sat = ASPSAT.se_satisfy_rule(ASPSAT, here_interpretation, there_interpretation, rule)
        print("rule: ", rule, ", sat: ", sat)
    sat = ASPSAT.se_satisfy_program(ASPSAT, here_interpretation, there_interpretation, rules)
    print("program sat: ", sat)


def test_lpmln_sat_rule():
    for rule in rules:
        sat = LPMLNSAT.se_satisfy_rule(LPMLNSAT, here_interpretation, there_interpretation, rule)
        print("rule: ", rule, ", sat: ", sat)
    sat = LPMLNSAT.se_satisfy_program(LPMLNSAT, here_interpretation, there_interpretation, rules)
    print("program sat: ", sat)


if __name__ == '__main__':
    pass
    