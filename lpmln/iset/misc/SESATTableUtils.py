
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-08-22 10:42
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : SESATTableUtils.py
"""

import itertools
from lpmln.utils.counter.BaseCounter import BaseCounter
import copy


def generate_all_subsets(atom_size=7):
    atom_sets = [i for i in range(atom_size)]
    rule_sets = []
    for i in range(0, atom_size + 1):
        it = itertools.combinations(atom_sets, i)
        for rs in it:
            rule_sets.append(set(rs))
    return rule_sets


def generate_all_rules(atom_size=7, is_use_extended_rules=False):
    all_rules = list()
    all_sets = generate_all_subsets(atom_size)
    rule_set_size = 3
    if is_use_extended_rules:
        rule_set_size = 4

    all_sets_size = len(all_sets)
    boundary = [all_sets_size - 1] * rule_set_size
    counter = BaseCounter(rule_set_size, boundary)

    current_indicator = counter.get_current_indicator()
    while current_indicator is not None:
        rule = []
        for i in current_indicator:
            rule.append(all_sets[i])
        all_rules.append(rule)
        current_indicator = counter.get_current_indicator()

    return all_rules


def get_atom_set_from_rule(rule):
    atoms = set()
    for lit in rule:
        atoms = atoms.union(lit)
    return atoms


def generate_single_deletion_empty_table():
    table = list()
    for i in range(3):
        item = list()
        for j in range(2):
            item.append(set())
        table.append(item)
    return table


def generate_0000_single_deletion_sat_result():
    table = generate_single_deletion_empty_table()
    for model in table:
        model[0].add(0)
        model[1].add(1)
    return table


def print_single_deletion_sat_result_table(all_case_flags, result_table):
    for case in all_case_flags:
        table = result_table[case]
        print(case, "|", table[0][1], "|", table[0][0])

    print("")
    for case in all_case_flags:
        table = result_table[case]
        print(case, "|", table[1][1], "|", table[1][0], "|", table[2][1], "|", table[2][0])

    print("\n")


def extract_direct_single_deletion_case_sat_results(case_sat_results, total_results):
    for case in case_sat_results:
        if case not in total_results:
            total_results[case] = generate_single_deletion_empty_table()
        case_table = total_results[case]
        sat_results = case_sat_results[case]

        for sr in sat_results:
            del_rule_sat = sr[3]
            for i in range(0, 3):
                case_table[i][sr[i]].add(del_rule_sat)


def deep_copy_rule(rule):
    new_rule = []
    for r in rule:
        new_rule.append(copy.deepcopy(r))
    return new_rule

if __name__ == '__main__':
    all_rules = generate_all_rules(3, False)
    print(len(all_rules))
    for r in all_rules:
        print(r)
    pass
    