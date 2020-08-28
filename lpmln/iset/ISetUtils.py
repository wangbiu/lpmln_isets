
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/5 15:50
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ISetUtils.py
"""

import math
import copy
import itertools
from lpmln.iset.IndependentSet import IndependentSet


def get_real_nonempty_iset_ids_from_partial_nonemtpy_iset_ids(partial_non_empty_iset_ids, known_empty_iset_ids, known_non_empty_iset_ids=list()):
    if len(partial_non_empty_iset_ids) == 0:
        return partial_non_empty_iset_ids

    partial_non_empty_iset_ids = copy.deepcopy(partial_non_empty_iset_ids)
    partial_non_empty_iset_ids.sort()

    known_positions = list(known_empty_iset_ids)
    known_positions.extend(list(known_non_empty_iset_ids))
    known_positions.sort()

    piset_size = len(partial_non_empty_iset_ids)
    for pid in known_positions:
        for i in range(piset_size):
            if partial_non_empty_iset_ids[i] >= pid:
                for j in range(i, piset_size):
                    partial_non_empty_iset_ids[j] += 1
                break
    for iset_id in known_non_empty_iset_ids:
        partial_non_empty_iset_ids.append(iset_id)
    return partial_non_empty_iset_ids


def construct_iset_condition_from_non_emtpy_iset_ids(non_empty_iset_ids, iset_number):
    """
        non_empty_set_id = iset_id - 1 (list index)
    """
    icondition = [0] * iset_number
    for ne in non_empty_iset_ids:
        icondition[ne] = 1
    return icondition


def construct_isets_from_non_empty_iset_ids(non_empty_iset_ids, iset_number, is_use_extended_rule, iset_atom_number=1):
    icondition = construct_iset_condition_from_non_emtpy_iset_ids(non_empty_iset_ids, iset_number)
    isets = construct_isets_from_iset_condition(icondition, iset_atom_number, is_use_extended_rule)
    return isets


def construct_isets_from_iset_condition(icondition, is_use_extended_rule, iset_atom_number=1):
    iset_number = len(icondition)
    rset_number = int(math.log(iset_number + 1, 2))
    independent_sets = dict()

    atom_id = 1
    for iset_id in range(iset_number):
        if icondition[iset_id] == -1:
            continue

        iset = IndependentSet(is_use_extended_rule)
        iset.generate_iset_iusets_from_iset_id(iset_id + 1, rset_number)
        member = set()
        if icondition[iset_id] == 1:
            for i in range(iset_atom_number):
                member.add(atom_id)
                atom_id += 1

        iset.set_members(member)
        independent_sets[iset.get_iset_id()] = iset

    return independent_sets


def construct_sets_from_isets(ind_sets=dict(), is_print=False):
    ind_values = list(ind_sets.values())
    rset_number = len(ind_values[0].intersect_sets) + len(ind_values[0].union_sets)
    sets = [set()] * rset_number

    for ind in ind_values:
        for cn in ind.intersect_sets:
            sets[cn] = sets[cn].union(ind.members)

    if is_print:
        for i in range(rset_number):
            print(i, sets[i])

    return sets


def construct_rules_from_isets(independent_sets, rule_number, rule_set_size):
    rule_sets = construct_sets_from_isets(independent_sets)
    rules = list()
    for i in range(rule_number):
        rule = []
        for j in range(rule_set_size):
            rule.append(rule_sets[rule_set_size * i + j])
        rules.append(rule)
    return rules


def construct_kmn_program_from_isets(isets, k_size, m_size, n_size, rule_set_size):
    rule_number = k_size + m_size + n_size
    rules = construct_rules_from_isets(isets, rule_number, rule_set_size)
    prg_k = list()
    prg_m = list()
    prg_n = list()
    km_size = k_size + m_size
    for i in range(rule_number):
        r = rules[i]
        if i < k_size:
            prg_k.append(r)
        elif k_size <= i < km_size:
            prg_m.append(r)
        else:
            prg_n.append(r)
    return prg_k, prg_m, prg_n


def construct_iset_condition_from_icondition_id(condition_id, iset_number):
    id_str = bin(condition_id)[2:]
    id_str = "0" * (iset_number - len(id_str)) + id_str
    icondition = [int(s) for s in id_str]
    return icondition


def construct_isets_from_icondition_id(condition_id, iset_number, iset_atom_number=1):
    icondition = construct_iset_condition_from_icondition_id(condition_id, iset_number)
    return construct_isets_from_iset_condition(icondition, iset_atom_number=iset_atom_number)


def compute_iset_number_from_kmn(k_size, m_size, n_size, rule_set_size):
    return 2 ** (rule_set_size * (k_size + m_size + n_size)) - 1


def get_universe(sets=list()):
    univ = set()
    for s in sets:
        univ = univ.union(s)
    return univ


def compute_isets_for_sets(sets, is_use_extended_rule, is_print=False):
    univ = get_universe(sets)
    set_number = len(sets)
    set_names = set([n for n in range(set_number)])
    ind_sets = dict()
    for i in range(1, set_number + 1):
        for ist_set in itertools.combinations(set_names, i):
            union_sets = set_names.difference(ist_set)
            iset = IndependentSet(is_use_extended_rule)

            iset.set_intersect_sets(ist_set)
            iset.set_union_sets(union_sets)
            iset_id = iset.get_iset_id()

            member = univ
            for s in ist_set:
                member = member.intersection(sets[s])
            union = set()
            for s in union_sets:
                union = union.union(sets[s])
            iset.set_members(member.difference(union))
            ind_sets[iset_id] = iset

    if is_print:
        for key in ind_sets:
            print(ind_sets[key])

    return ind_sets


def compute_isets_from_program(program, is_use_extended_rule):
    sets = []
    for rule in program:
        sets.extend(rule)
    return compute_isets_for_sets(sets, is_use_extended_rule)


def load_iconditions_from_file(file):
    iconditions = list()
    with open(file, mode="r", encoding="utf-8") as f:
        for row in f:
            row = row.strip('\n')
            iconditions.append(row)

    print("load %d iconditions from %s" % (len(iconditions), file))
    return iconditions


def parse_iconditions(iconditions):
    parsed = list()
    for cdt in iconditions:
        pos = cdt.find(":")
        if pos != -1:
            parts = cdt.split(":")
            singletons = parts[1].split(",")
            singletons = [int(s) for s in singletons]
            cdts = parts[0].split(",")
        else:
            cdts = cdt.split(",")
            singletons = list()

        cdts = [int(s) for s in cdts]
        parsed.append((cdts, singletons))
    return parsed


def parse_iconditions_ignore_singletons(iconditions):
    parsed = parse_iconditions(iconditions)
    results = list()
    for ict in parsed:
        results.append(ict[0])
    return results


def get_ne_iset_number(condition):
    number = 0
    for c in condition:
        number += c

    return number


def get_ne_iset_ids(condition):
    ids = set()
    for i in range(len(condition)):
        if condition[i] == 1:
            ids.add(i)
    return ids





if __name__ == '__main__':
    pass
    