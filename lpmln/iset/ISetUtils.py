
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/5 15:50
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ISetUtils.py
"""

import math
from lpmln.iset.IndependentSet import IndependentSet
import copy


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
    icondition = [0] * iset_number
    for ne in non_empty_iset_ids:
        icondition[ne] = 1
    return icondition


def construct_isets_from_non_empty_iset_ids(non_empty_iset_ids, iset_number, iset_atom_number=1):
    icondition = construct_iset_condition_from_non_emtpy_iset_ids(non_empty_iset_ids, iset_number)
    isets = construct_isets_from_iset_condition(icondition, iset_atom_number)
    return isets


def construct_isets_from_iset_condition(icondition, iset_atom_number=1):
    iset_number = len(icondition)
    rset_number = int(math.log(iset_number + 1, 2))
    independent_sets = dict()

    atom_id = 1
    for iset_id in range(iset_number):
        if icondition[iset_id] == -1:
            continue

        iset = IndependentSet()
        iset.generate_iset_iusets_from_iset_id(iset_id, rset_number)
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
    rset_number = len(ind_values[0].cap_sets) + len(ind_values[0].union_sets)
    sets = [set()] * rset_number

    for ind in ind_values:
        for cn in ind.cap_sets:
            sets[cn] = sets[cn].union(ind.ind_set)

    if is_print:
        for i in range(rset_number):
            print(i, sets[i])

    return sets


def construct_rules_from_isets(independent_sets, rule_number):
    rule_sets = construct_sets_from_isets(independent_sets)
    rules = list()
    for i in range(rule_number):
        rule = []
        for j in range(3):
            rule.append(rule_sets[3 * i + j])
        rules.append(rule)
    return rules


def construct_kmn_program_from_isets(isets, k_size, m_size, n_size):
    rule_number = k_size + m_size + n_size
    rules = construct_rules_from_isets(isets, rule_number)
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


def compute_iset_number_from_kmn(k_size, m_size, n_size):
    return 2 ** (3 * (k_size + m_size + n_size)) - 1

if __name__ == '__main__':
    pass
    