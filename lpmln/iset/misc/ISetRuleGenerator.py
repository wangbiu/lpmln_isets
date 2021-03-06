
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-14 22:20
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ISetRuleGenerator.py
"""

from lpmln.utils.counter.BaseCounter import BaseCounter
import lpmln.iset.ISetCompositionUtils as iscm
import math


def generate_all_rules(max_iset_atom_size=1, is_use_extended_rules=False):
    rule_set_size = 3
    if is_use_extended_rules:
        rule_set_size = 4

    iset_number = 2 ** rule_set_size - 1
    counter = BaseCounter(iset_number, [max_iset_atom_size] * iset_number)
    rules = list()
    while True:
        rule_idx = counter.get_current_indicator()
        # print(rule_idx)
        if rule_idx is None:
            break

        rule = construct_rule_from_isets_atom_sizes(rule_set_size, rule_idx)
        rules.append(rule)

    return rules


def generate_all_rules_iset_fmt(max_iset_atom_size=1, is_use_extended_rules=False):
    rule_set_size = 3
    if is_use_extended_rules:
        rule_set_size = 4

    iset_number = 2 ** rule_set_size - 1
    counter = BaseCounter(iset_number, [max_iset_atom_size] * iset_number)
    rules = list()
    while True:
        rule_idx = counter.get_current_indicator()
        # print(rule_idx)
        if rule_idx is None:
            break

        rule = list()
        for i in range(iset_number):
            iset = set()
            for j in range(rule_idx[i]):
                iset.add(i+1+j*iset_number)
            rule.append(iset)

        rules.append(rule)

    return rules


def construct_rule_from_isets_atom_sizes(rule_set_size, isets_atom_sizes):
    rule = list()
    for i in range(rule_set_size):
        rule.append(set())
    iset_number = len(isets_atom_sizes)
    for iset_id in range(1, iset_number + 1):
        composition = iscm.get_iset_binary_bits(iset_id, rule_set_size)
        for i in range(rule_set_size):
            if composition[i] == 1:
                for j in range(isets_atom_sizes[iset_id-1]):
                    rule[i].add(iset_number * j + iset_id)
    return rule


def construct_rule_from_isets(isets):
    rule = list()
    isets_size = len(isets)
    rule_set_size = int(math.log2(isets_size + 1))


    for i in range(rule_set_size):
        rule.append(set())
    iset_number = len(isets)
    for iset_id in range(1, iset_number + 1):
        composition = iscm.get_iset_binary_bits(iset_id, rule_set_size)
        for i in range(rule_set_size):
            if composition[i] == 1:
                for j in isets[iset_id-1]:
                    rule[i].add(j)
    return rule



if __name__ == '__main__':
    # rules = generate_all_rules(1, False)
    rules = generate_all_rules_iset_fmt(1, False)
    cnt = 0
    for r in rules:
        cnt += 1
        print(cnt, " : ", r, construct_rule_from_isets(r))
    pass
    