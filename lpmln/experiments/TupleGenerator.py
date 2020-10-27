
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-10 4:53
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : TupleGenerator.py
"""

import itertools
import copy
import random

atom_universe = [i for i in range(1, 2*7+1)]


def get_all_rules_by_isets(atom_size):
    it = itertools.combinations(atom_universe, atom_size)
    rules = list()
    for atoms in it:
        atoms = list(atoms)
        for at in atoms:
            flag = at % 7
            rule = (set(), set(), set())
            flag = bin(flag)[2:]
            flag = "0" * (3 - len(flag)) + flag
            for i in range(len(flag)):
                if flag[i] == "1":
                    rule[i].add(at)

        rules.append(rule)
    return  rules


def get_all_sets_of_atoms(atom_size=3):
    atoms = [i+1 for i in range(atom_size)]
    subsets = []
    subsets.append(set())
    for i in range(1, atom_size + 1):
        itr = itertools.combinations(atoms, i)
        for sub in itr:
            subsets.append(set(sub))
    return subsets


def get_all_rules_of_atoms(atom_size=3):
    atom_sets = get_all_sets_of_atoms(atom_size)
    rules = list()
    atom_sets_size = len(atom_sets)
    for i in range(atom_sets_size):
        for j in range(atom_sets_size):
            for k in range(atom_sets_size):
                rules.append((copy.deepcopy(atom_sets[i]), copy.deepcopy(atom_sets[j]), copy.deepcopy(atom_sets[k])))

    return rules


def get_kmn_tuples(k, m, n, atom_size=3, tuple_size=1000000):
    all_rules = get_all_rules_by_isets(atom_size)
    kmn_tuples = list()
    for i in range(tuple_size):
        tp = get_a_random_kmn_tuple(k, m, n, all_rules)
        kmn_tuples.append(tp)
        # print(tp)

    return kmn_tuples


def get_a_random_kmn_tuple(k, m, n, all_rules):
    used_rules = set()
    prg_k = get_random_program(k, len(all_rules), used_rules)
    prg_m = get_random_program(m, len(all_rules), used_rules)
    prg_n = get_random_program(n, len(all_rules), used_rules)
    # print(prg_k, prg_m, prg_n)

    prg_k = [copy.deepcopy(all_rules[r]) for r in prg_k]
    prg_m = [copy.deepcopy(all_rules[r]) for r in prg_m]
    prg_n = [copy.deepcopy(all_rules[r]) for r in prg_n]

    return prg_k, prg_m, prg_n


def get_random_program(prg_size, rule_size, used_rules):
    prg = set()

    while len(prg) < prg_size:
        r = random.randint(0, rule_size-1)
        if r not in used_rules:
            used_rules.add(r)
            prg.add(r)
    return prg


def check_se_by_se_condition(prg_k, prg_m, prg_n):
    prg_k = remove_se_valid_rule(prg_k)
    prg_m = remove_se_valid_rule(prg_m)
    prg_n = remove_se_valid_rule(prg_n)

    rule_size = len(prg_k) + len(prg_m) + len(prg_n)
    if rule_size == 0:
        return True
    elif rule_size == 1:
        return False
    elif rule_size > 2:
        return None

    rules = list()
    rules.extend(prg_k)
    rules.extend(prg_m)
    rules.extend(prg_n)

    isets_list = list()
    for r in rules:
        iset = compute_isets_of_rule(r)
        isets_list.append(iset)

    # print(isets_list)
    i36 = isets_list[0][3].intersection(isets_list[1][3])
    if len(i36) == 0:
        return False

    common_empty_isets = [[0, 4], [1, 4], [2, 4], [5, 4], [1, 0], [1, 2], [2, 0], [2, 1], [2, 5], [4, 0], [4, 2], [5, 0], [5, 2]]
    iset_011_empty_isets = [[0, 1], [0, 2], [0, 5], [4, 1], [4, 5]]

    is_common_empty = check_empty_isets(isets_list, common_empty_isets)
    if not is_common_empty:
        return False

    if len(prg_n) == 0:
        return is_common_empty

    is_011_empty = check_empty_isets(isets_list, iset_011_empty_isets)
    return is_011_empty


def check_empty_isets(isets_list, empty_isets):
    is_empty = True
    for iset in empty_isets:
        s1 = isets_list[0][iset[0]]
        s2 = isets_list[1][iset[1]]

        if len(s1) == 0 or len(s2) == 0:
            continue
        elif len(s1.intersection(s2)) == 0:
            continue
        else:
            is_empty = False
            break

    return is_empty


def compute_isets_of_rule(rule):
    isets = list()
    for flag in range(1, 8):
        intersect = None
        union = set()
        iset_id = bin(flag)[2:]
        iset_id = "0" * (3 - len(iset_id)) + iset_id
        for i in range(3):
            if iset_id[i] == "1":
                if intersect is None:
                    intersect = rule[i]
                else:
                    intersect = intersect.intersection(rule[i])
            else:
                union = union.union(rule[i])

        isets.append(intersect.difference(union))

    return isets


def remove_se_valid_rule(prg):
    new_prg = list()
    for r in prg:
        if r[0].issubset(r[2]) or r[1].intersection(r[2]) or r[0].intersection(r[1]):
            continue
        else:
            new_prg.append(r)
    return new_prg


def generate_empty_iset_110():
    nse_ids = [3, 11, 19, 43]
    se_ids = [0, 1, 4, 7, 8, 9, 12, 15, 16, 17, 20, 31, 32, 33, 35, 36, 39, 40, 41, 44]
    ids = nse_ids + se_ids
    ignore_isets_110 = {35, 8, 12, 17, 40, 44, 0, 1, 4, 32, 36}
    empty_ids = list()
    for id in ids:
        if id not in ignore_isets_110:
            empty_ids.append(id)
    print(empty_ids, len(empty_ids))
    empty_ids_comp = list()
    for id in empty_ids:
        empty_ids_comp.append(compute_iset_composition(id, 2))
    return empty_ids_comp



def generate_empty_iset_011():
    nse_ids = [3, 11, 19, 31, 32, 33, 36, 43]
    se_ids = [0, 1, 4, 7, 8, 9, 12, 15, 16, 17, 20, 35, 39, 40, 41, 44]
    ids = nse_ids + se_ids
    ignore_isets_011 = {35, 8, 12, 17, 40, 44}
    empty_ids = list()
    for id in ids:
        if id not in ignore_isets_011:
            empty_ids.append(id)
    print(empty_ids, len(empty_ids))
    empty_ids_comp = list()
    for id in empty_ids:
        empty_ids_comp.append(compute_iset_composition(id, 2))
    return empty_ids_comp


def compute_iset_composition(iset_id, rule_number=2):
    iset_id += 1
    iset_id = oct(iset_id)[2:]
    iset_id = "0" * (rule_number - len(iset_id)) + iset_id
    tuple = [int(s) - 1 for s in iset_id]
    return tuple



def compute_iset_composition_for_isetlist(iset_ids):
    comps = list()
    for id in iset_ids:
        comps.append(compute_iset_composition(id, 2))
    return comps


if __name__ == '__main__':
    # sub = get_all_sets_of_atoms()
    # print(len(sub), sub)
    # tuples = get_kmn_tuples(0, 1, 1, atom_size=3, tuple_size=10)
    # for t in tuples:
    #     print(t)

    isets = compute_isets_of_rule([{1}, {2}, {1}])
    # print(isets)
    # generate_empty_iset_011()
    # generate_empty_iset_110()

    iset_011_empty_ids = [0, 1, 4, 32, 36]
    iset_rule2_common_empty_ids = [3, 11, 19, 43, 7, 9, 15, 16, 20, 31, 33, 39, 41]

    common = compute_iset_composition_for_isetlist(iset_rule2_common_empty_ids)
    iset_011 = compute_iset_composition_for_isetlist(iset_011_empty_ids)
    print(common)
    print(iset_011)

    pass
    