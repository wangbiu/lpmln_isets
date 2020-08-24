
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/5 15:59
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : OptimizationISetsUtils.py
"""

import lpmln.iset.ISetUtils as isu
from lpmln.iset.IndependentSet import IndependentSet
import copy

"""
hpn independent sets: 
h \cap pb, pb \cap nb sets,  h \cap pb = \emptyset, pb \cap nb = \emptyset 
"""


# TODO: 优化方法未定
def get_hpn_independent_set_ids(rule_number, is_use_extended_rules):
    iset = IndependentSet(is_use_extended_rules)
    rset_number = iset.rule_set_size * rule_number
    iset_number = 2 ** rset_number - 1

    empty_iset_ids = set()

    for iset_id in range(0, iset_number):
        iset.generate_iset_iusets_from_iset_id(iset_id + 1, rset_number)
        caps = set(iset.intersect_sets)

        for r in range(rule_number):
            pb_set_id = iset.rule_set_size * r + 1
            nb_set_id = pb_set_id + 1

            if pb_set_id in caps:  # nb \cap pb \neq \emptyset, pb \cap h \neq \emptyset
                if pb_set_id - 1 in caps or nb_set_id in caps:
                    empty_iset_ids.add(iset_id)

            if is_use_extended_rules:  # nb \cap nh \neq \emptyset
                if nb_set_id in caps and nb_set_id + 1 in caps:
                    empty_iset_ids.add(iset_id)

    # print("rule number %d, find %d empty sets, remain %d independent sets" % (
    #     rule_number, len(empty_iset_ids), iset_number - len(empty_iset_ids)))
    return empty_iset_ids


# TODO: 优化方法未定
def get_hn_independent_set_ids(rule_number, is_use_extended_rules):
    iset = IndependentSet(is_use_extended_rules)
    rset_number = iset.rule_set_size * rule_number
    iset_number = 2 ** rset_number - 1

    empty_iset_ids = set()

    for iset_id in range(0, iset_number):
        iset.generate_iset_iusets_from_iset_id(iset_id + 1, rset_number)
        caps = set(iset.intersect_sets)

        for r in range(rule_number):
            h_set_id = 3 * r
            if h_set_id in caps:
                if h_set_id + 2 in caps:
                    empty_iset_ids.add(iset_id)

    # print("rule number %d, find %d empty sets, remain %d independent sets" % (
    #     rule_number, len(empty_iset_ids), iset_number - len(empty_iset_ids)))
    return empty_iset_ids


def get_empty_indpendent_set_ids(rule_number, is_use_extended_rules):
    ids = set()
    if rule_number == 2:
        hpn_ids = get_hpn_independent_set_ids(rule_number, is_use_extended_rules)
        ids = ids.union(hpn_ids)

    if rule_number >= 3:
        hpn_ids = get_hpn_independent_set_ids(rule_number, is_use_extended_rules)
        ids = ids.union(hpn_ids)
        hn_ids = get_hn_independent_set_ids(rule_number, is_use_extended_rules)
        ids = ids.union(hn_ids)
    rule_set_size = 3
    if is_use_extended_rules:
        rule_set_size = 4
    iset_number = 2 ** (rule_set_size * rule_number) - 1
    print("rule number %d, find %d empty sets, remain %d independent sets" % (
        rule_number, len(ids), iset_number - len(ids)))

    return ids


# TODO: 未使用的方法
def get_real_non_empty_iset_ids_from_partial_iset_list(unknown_iset_nonempty_list, empty_iset_ids):
    if len(unknown_iset_nonempty_list) == 0:
        return unknown_iset_nonempty_list

    unknown_iset_nonempty_list = copy.deepcopy(unknown_iset_nonempty_list)
    unknown_iset_nonempty_list.sort()
    empty_iset_ids = list(empty_iset_ids)
    empty_iset_ids.sort()

    uise_size = len(unknown_iset_nonempty_list)
    for isid in empty_iset_ids:
        for i in range(uise_size):
            if unknown_iset_nonempty_list[i] >= isid:
                for j in range(i, uise_size):
                    unknown_iset_nonempty_list[j] += 1
                break

    return unknown_iset_nonempty_list


if __name__ == '__main__':
    pass
    