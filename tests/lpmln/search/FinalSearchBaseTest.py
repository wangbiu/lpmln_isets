
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-26 19:34
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : FinalSearchBaseTest.py
"""

from lpmln.search.distributed.final.FinalSearchBase import FinalIConditionsSearchBaseWorker
import itertools


def generate_same_isets(rule_number):
    isets = list()
    for i in range(1, 8):
        iset = "0o%d%d" % (i, i)
        isets.append(int(iset, 8) - 1)

    return isets


def generate_same_rules_isets(rule_number):
    isets = generate_same_isets(rule_number)
    ne_isets = list()
    for i in range(len(isets) + 1):
        iter = itertools.combinations(isets, i)
        for ist in iter:
            ne_isets.append(set(ist))

    return ne_isets


def generate_non_same_rules_isets(rule_number):
    ne_isets = generate_same_rules_isets(rule_number)
    for ne in ne_isets:
        ne.add(7)
    return ne_isets


def test_check_same_rules():
    rule_number = 2
    is_use_exrule = False
    ne_isets = generate_same_rules_isets(rule_number)
    for isets in ne_isets:
        has_same_rule = FinalIConditionsSearchBaseWorker.has_same_rules(isets, rule_number, is_use_exrule)
        if not has_same_rule:
            raise RuntimeError("wrong case ", isets)

    print("check pass... check %d ne_isets " % len(ne_isets))


def test_check_different_rules():
    rule_number = 2
    is_use_exrule = False
    ne_isets = generate_non_same_rules_isets(rule_number)
    for isets in ne_isets:
        has_same_rule = FinalIConditionsSearchBaseWorker.has_same_rules(isets, rule_number, is_use_exrule)
        if has_same_rule:
            raise RuntimeError("wrong case ", isets)

    print("check pass... check %d ne_isets " % len(ne_isets))


def test_check_has_same_rules():
    rule_number = 3
    is_use_exrule = False
    ne_isets = [
        {218, 17, 428, 510},
        {218, 17, 428, 510, 504},
        {218, 17, 428, 510, 502},
        {218, 17, 428, 510, 437},
    ]
    results = [True, False, False, True]

    for i in range(len(ne_isets)):
        isets = ne_isets[i]
        has_same_rule = FinalIConditionsSearchBaseWorker.has_same_rules(isets, rule_number, is_use_exrule)
        if has_same_rule != results[i]:
            raise RuntimeError("wrong case ", isets)

    print("check pass... check %d ne_isets " % len(ne_isets))


if __name__ == '__main__':
    pass
    