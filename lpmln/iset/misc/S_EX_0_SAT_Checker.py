
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-14 23:31
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : S_EX_0_SAT_Checker.py
"""

import lpmln.iset.misc.ISetRuleGenerator as ig
import itertools
import copy
from lpmln.sat.LPMLNSAT import LPMLNSAT
from lpmln.sat.ASPSAT import ASPSAT


def get_rule_universe(rule):
    universe = set()
    for s in rule:
        universe = universe.union(s)
    return universe


def get_all_ht_interpretations(universe):
    size = len(universe)
    ht_ints = list()
    for t_len in range(0, size + 1):
        t_iter = itertools.combinations(universe, t_len)
        for there in t_iter:
            there = set(there)
            for h_len in range(len(there) + 1):
                h_iter = itertools.combinations(there, h_len)
                for here in h_iter:
                    here = set(here)
                    ht_ints.append((copy.deepcopy(here), copy.deepcopy(there)))

    return ht_ints


def get_all_s_ex_0_rules(rule_isets):
    new_atom = -1
    rules = list()
    for iset_id in range(len(rule_isets)):
        if len(rule_isets[iset_id]) == 0:
            new_rule_isets = copy.deepcopy(rule_isets)
            new_rule_isets[iset_id].add(new_atom)
            rule = ig.construct_rule_from_isets(new_rule_isets)
            rules.append((iset_id, rule))
    return rules, new_atom


def get_all_s_ex_1_rules(rule_isets):
    new_atom = -1
    rules = list()
    for iset_id in range(len(rule_isets)):
        if len(rule_isets[iset_id]) > 0:
            new_rule_isets = copy.deepcopy(rule_isets)
            new_rule_isets[iset_id].add(new_atom)
            rule = ig.construct_rule_from_isets(new_rule_isets)
            rules.append((iset_id, rule))
    return rules, new_atom


def get_ht_interpretations_by_adding_atom(ht_interpretation, atom):
    hts = list()
    hts.append(copy.deepcopy(ht_interpretation))
    for i in range(2):
        ht = copy.deepcopy(ht_interpretation)
        ht[1].add(atom)
        hts.append(ht)
    hts[2][0].add(atom)
    return hts


def generate_empty_sat_table_for_adding_atom_transformations(is_use_extended_rules):
    case_size = 7
    if is_use_extended_rules:
        case_size = 15

    table = dict()
    for i in range(case_size):
        case_item = list()
        table[i] = case_item
        for row_case in range(2):
            row = list()
            case_item.append(row)
            for cell_case in range(3):
                cell = set()
                row.append(cell)

    return table


def extract_add_atom_sat_results(results, is_use_extended_rules):
    table = generate_empty_sat_table_for_adding_atom_transformations(is_use_extended_rules)
    for r in results:
        title_id = int(r[0])
        case_id = r[1]
        for cell_id in range(3):
            cell_sat = int(r[cell_id+2])
            table[case_id][title_id][cell_id].add(cell_sat)
    return table


def print_adding_result_table(table, rule_set_size):
    hts = ["(X, Y)", "(X, Y+)", "(X+, Y+)"]
    print("case", "| \t", "  HT  ", " \t| ", "SAT", " \t|  ", "N-SAT")

    if rule_set_size == 3:
        case_keys = [3, 1, 0, 4, 2, 5, 6]
    else:
        case_keys = [3, 1, 0, 4, 2, 5, 6]

    for case in case_keys:
        table_item = table[case]
        case_str = bin(case + 1)[2:]
        case_str = "0" * (rule_set_size - len(case_str)) + case_str
        print("-----------------------------------------------")
        for i in range(3):
            print(case_str, " | \t", hts[i], " \t| ", table_item[1][i], " \t|  ", table_item[0][i])
    print("-----------------------------------------------")

def check_all_rules_for_s_ex_0(max_iset_atom_size=2, is_use_extended_rules=False):
    all_rules_isets = ig.generate_all_rules_iset_fmt(max_iset_atom_size, is_use_extended_rules)
    all_results = list()
    for rule in all_rules_isets:
        results = s_ex_0_check(rule)
        all_results.extend(results)
    table = extract_add_atom_sat_results(all_results, is_use_extended_rules)
    rule_set_size = 3
    if is_use_extended_rules:
        rule_set_size = 4
    print(table.keys())
    print_adding_result_table(table, rule_set_size)


def check_all_rules_for_s_ex_1(max_iset_atom_size=2, is_use_extended_rules=False):
    all_rules_isets = ig.generate_all_rules_iset_fmt(max_iset_atom_size, is_use_extended_rules)
    all_results = list()
    for rule in all_rules_isets:
        results = s_ex_1_check(rule)
        all_results.extend(results)
    table = extract_add_atom_sat_results(all_results, is_use_extended_rules)
    rule_set_size = 3
    if is_use_extended_rules:
        rule_set_size = 4
    print_adding_result_table(table, rule_set_size)


def s_ex_0_check(rule_isets):
    universe = get_rule_universe(rule_isets)
    all_hts = get_all_ht_interpretations(universe)
    rule = ig.construct_rule_from_isets(rule_isets)
    all_ex_0_rules, new_atom = get_all_s_ex_0_rules(rule_isets)
    results = list()
    for ht in all_hts:
        ht_sat = LPMLNSAT.se_satisfy_rule(LPMLNSAT, *ht, rule)
        for ex_rule in all_ex_0_rules:
            item = list()
            item.append(ht_sat)
            item.append(ex_rule[0])
            new_hts = get_ht_interpretations_by_adding_atom(ht, new_atom)
            for nht in new_hts:
                item.append(LPMLNSAT.se_satisfy_rule(LPMLNSAT, *nht, ex_rule[1]))
            results.append(item)
    return results


def s_ex_1_check(rule_isets):
    universe = get_rule_universe(rule_isets)
    all_hts = get_all_ht_interpretations(universe)
    rule = ig.construct_rule_from_isets(rule_isets)
    all_ex_0_rules, new_atom = get_all_s_ex_1_rules(rule_isets)
    results = list()
    for ht in all_hts:
        ht_sat = LPMLNSAT.se_satisfy_rule(LPMLNSAT, *ht, rule)
        for ex_rule in all_ex_0_rules:
            item = list()
            item.append(ht_sat)
            item.append(ex_rule[0])
            new_hts = get_ht_interpretations_by_adding_atom(ht, new_atom)
            for nht in new_hts:
                item.append(LPMLNSAT.se_satisfy_rule(LPMLNSAT, *nht, ex_rule[1]))
            results.append(item)
    return results




if __name__ == '__main__':
    universe = [i for i in range(3)]
    ht = ({1}, {1, 2})
    # hts = get_all_ht_interpretations(universe)

    # hts = get_ht_interpretations_by_adding_atom(ht, -1)
    #
    # for ht in hts:
    #     print(ht)

    check_all_rules_for_s_ex_0(1, False)
    # check_all_rules_for_s_ex_1(1, False)

    pass
    