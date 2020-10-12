
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/10/12 13:50
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : IConditionCliquesUtils.py
"""

import lpmln.iset.IConditionUtils as iscu
import itertools
import copy
import lpmln.config.GlobalConfig as cfg
config = cfg.load_configuration()
import itertools
from lpmln.iset.IConditionGroup import IConditionGroup
import json
import logging


def empty_isets_generator(ne_isets):
    for i in range(len(ne_isets)):
        check_iter = itertools.combinations(ne_isets, i)
        for ci in check_iter:
            yield ci


def construct_kmn_group_only_contains_icondition_ids(kmn_groups, icondition_ids, removed_icondition_ids):
    group = dict()
    for g in icondition_ids:
        group[g] = copy.deepcopy(kmn_groups[g])

    for g in group:
        children = group[g].group_children
        parents = group[g].group_parents
        group[g].group_children = children.difference(removed_icondition_ids)
        group[g].group_parents = parents.difference(removed_icondition_ids)

    return group


def split_kmn_group_into_single_root_groups(kmn_groups):
    groups = list()
    roots = get_group_roots(kmn_groups)
    if len(roots) == 1:
        groups.append(kmn_groups)
        return groups

    all_icondition_ids = set()
    for g in groups:
        all_icondition_ids.add(g)

    for r in roots:
        icondition_ids = copy.deepcopy(kmn_groups[r].group_children)
        icondition_ids.add(r)
        removed_icondition_ids = all_icondition_ids.difference(icondition_ids)
        group = construct_kmn_group_only_contains_icondition_ids(kmn_groups, icondition_ids, removed_icondition_ids)
        groups.append(group)

    return groups


def get_kmn_groups_from_iconditions_contain_all_isets(kmn_groups, iconditions, isets):
    used_iconditions = get_iconditions_contain_all_isets(iconditions, isets)
    all_iconditions = get_all_icondition_ids_from_kmn_groups(kmn_groups)
    new_kmn_group = construct_kmn_group_only_contains_icondition_ids(
        kmn_groups, used_iconditions.intersection(all_iconditions), all_iconditions.difference(used_iconditions))

    return new_kmn_group


def get_kmn_groups_from_iconditions_contain_none_of_isets(kmn_groups, iconditions, isets):
    removed_iconditions = get_iconditions_contain_at_least_one_of_isets(iconditions, isets)
    all_iconditions = get_all_icondition_ids_from_kmn_groups(kmn_groups)
    new_kmn_group = construct_kmn_group_only_contains_icondition_ids(
        kmn_groups, all_iconditions.difference(removed_iconditions), removed_iconditions)

    return new_kmn_group


def check_cliques_of_kmn_groups(kmn_groups, iconditions):
    clique_data = list()
    roots = get_group_roots(kmn_groups)
    for r in roots:
        max_ne_isets = copy.deepcopy(iconditions[r].ne_iset_ids)
        ic_ids = copy.deepcopy(kmn_groups[r].group_children)
        ic_ids.add(r)
        common_ne_isets = get_common_ne_isets(ic_ids, iconditions)
        clique_size = 2 ** (len(max_ne_isets) - len(common_ne_isets))
        if clique_size == len(ic_ids):
            clique_data.append((ic_ids, common_ne_isets, max_ne_isets))

    return clique_data


def get_common_ne_isets(iconditions_ids, iconditions):
    common = None
    for ic_id in iconditions_ids:
        ne_isets = iconditions[ic_id].ne_iset_ids
        if common is None:
            common = ne_isets
        else:
            common = common.intersection(ne_isets)

    return common


def check_iconditions_in_cliques(all_icondition_ids, clique_data):
    clique_icondition_ids = get_clique_data_icondition_ids(clique_data)

    if len(all_icondition_ids) == len(clique_icondition_ids):
        return True
    else:
        return False


def get_all_kmn_cliques(kmn_groups, iconditions):
    all_icondition_ids = get_all_icondition_ids_from_kmn_groups(kmn_groups)
    group_iconditions = list()
    for g in kmn_groups:
        group_iconditions.append(iconditions[g])

    group_ne_isets = iscu.get_iconditions_ne_isets(group_iconditions)
    check_cases = 2 ** len(group_ne_isets) - 1
    print("has %d non-empty isets, check %d cases" % (len(group_ne_isets), check_cases))
    empty_isets = empty_isets_generator(group_ne_isets)

    clique_data = list()

    for eis in empty_isets:
        clique_meta = list()
        new_kmn_groups = get_kmn_groups_from_iconditions_contain_all_isets(kmn_groups, iconditions, set(eis))
        cliques = check_cliques_of_kmn_groups(new_kmn_groups, iconditions)
        for cq in cliques:
            clique_meta.append(len(cq[0]))
        clique_data.extend(cliques)
        is_finish = check_iconditions_in_cliques(all_icondition_ids, clique_data)
        print("\t check non-empty isets: ", [i+1 for i in eis],
              ", find %d cliques, is finish %s, clique sizes: " % (len(cliques), str(is_finish)), clique_meta)
        if is_finish:
            break

        clique_meta = list()
        new_kmn_groups = get_kmn_groups_from_iconditions_contain_none_of_isets(kmn_groups, iconditions, set(eis))
        cliques = check_cliques_of_kmn_groups(new_kmn_groups, iconditions)
        for cq in cliques:
            clique_meta.append(len(cq[0]))
        clique_data.extend(cliques)
        is_finish = check_iconditions_in_cliques(all_icondition_ids, clique_data)
        print("\t check empty isets: ",  [i+1 for i in eis],
              ", find %d cliques, is finish %s, clique sizes: " % (len(cliques), str(is_finish)), clique_meta)

        if is_finish:
            break

    return clique_data


def get_all_icondition_ids_from_kmn_groups(kmn_groups):
    all_ids = set()
    for g in kmn_groups:
        all_ids.add(g)

    return all_ids


def get_iconditions_contain_at_least_one_of_isets(iconditions, isets):
    result_iconditions = set()
    for i in range(len(iconditions)):
        ne_isets = iconditions[i].ne_iset_ids
        for s in isets:
            if s in ne_isets:
                result_iconditions.add(i)

    return result_iconditions


def get_iconditions_contain_all_isets(iconditions, isets):
    result_iconditions = set()
    for i in range(len(iconditions)):
        is_contain_all = True
        ne_isets = iconditions[i].ne_iset_ids
        for s in isets:
            if s not in ne_isets:
                is_contain_all = False
        if is_contain_all:
            result_iconditions.add(i)

    return result_iconditions


def get_group_roots(groups):
    roots = list()
    for g in groups:
        if len(groups[g].group_parents) == 0:
            roots.append(g)
    return roots


def get_group_leaves(groups):
    leaves = list()
    for g in groups:
        if len(groups[g].group_children) == 0:
            leaves.append(g)
    return leaves


def get_group_descentdants(groups, g):
    parent = groups[g]
    children = parent.group_children
    descendant = list()
    descendant.extend(children)

    while len(children) > 0:
        new_children = list()
        for ch in children:
            child = groups[ch]
            new_children.extend(child.group_children)
            descendant.extend(child.group_children)

        new_children = set(new_children)
        children = new_children

    descendant = set(descendant)
    return descendant


def get_group_ascentdants(groups, g):
    child = groups[g]
    parents = child.group_parents
    ascendant = list()
    ascendant.extend(parents)

    while len(parents) > 0:
        new_parents = list()
        for pa in parents:
            parent = groups[pa]
            new_parents.extend(parent.group_parents)
            ascendant.extend(parent.group_parents)

        parents = set(new_parents)

    ascendant = set(ascendant)
    return ascendant


def get_clique_data_icondition_ids(clique_data):
    icondition_ids = set()
    for cq in clique_data:
        icondition_ids = icondition_ids.union(cq[0])

    return icondition_ids


def find_max_cliques_from_clique_data(clique_data):
    print("finding max cliques from clique data ... ")
    size = len(clique_data)
    meta = list()
    max_cliques_ids = set()
    non_max_clique_ids = set()

    for i in range(size):
        if i in non_max_clique_ids:
            continue

        is_max_clique = True
        cq_desc_i = set(clique_data[i][0])
        for j in range(size):
            if j == i:
                continue

            cq_desc_j = set(clique_data[j][0])

            if cq_desc_i.issubset(cq_desc_j):
                non_max_clique_ids.add(i)
                is_max_clique = False
                break
            elif cq_desc_j.issubset(cq_desc_i):
                non_max_clique_ids.add(j)

        if is_max_clique:
            max_cliques_ids.add(i)
            meta.append(len(clique_data[i][0]))

    print("find %d max cliques, meta data: " % len(max_cliques_ids), meta)
    return max_cliques_ids


def simplify_kmn_se_conditions(k_size, m_size, n_size, min_ne, max_ne, type, result_postfix=""):
    group_file = iscu.get_icondition_refine_group_file(k_size, m_size, n_size, min_ne, max_ne, type)
    clique_file = iscu.get_icondition_refine_group_file(k_size, m_size, n_size, min_ne, max_ne, type) + ".clique"
    sim_file = iscu.get_icondition_simplified_file(k_size, m_size, n_size, min_ne, max_ne, type, result_postfix)

    print("init simplify_kmn_se_conditions ... ")

    groups = iscu.load_iconditions_groups(group_file)
    ic_file = config.get_isc_results_file_path(k_size, m_size, n_size, min_ne, max_ne, type)

    print("load iconditions from %s " % ic_file)
    iconditions = iscu.load_iconditions_from_file(ic_file)
    all_icondition_ids = get_all_icondition_ids_from_kmn_groups(groups)

    print("init groups data ...")
    for g in groups:
        desc = get_group_descentdants(groups, g)
        asc = get_group_ascentdants(groups, g)
        groups[g].group_parents = asc
        groups[g].group_children = desc

    print("compute single root groups ... ")
    single_root_groups = split_kmn_group_into_single_root_groups(groups)
    print("simplify %d-%d-%d (%d ~ %d) %s iconditions, load %d iconditions, find %d single root gropus" % (
        k_size, m_size, n_size, min_ne, max_ne, type, len(iconditions), len(single_root_groups)))
    clique_data = list()

    for sg in single_root_groups:
        cliques = get_all_kmn_cliques(sg, iconditions)
        clique_data.extend(cliques)

    is_finish = check_iconditions_in_cliques(all_icondition_ids, clique_data)
    clique_icondition_ids = get_clique_data_icondition_ids(clique_data)
    remained_iconditions = all_icondition_ids.difference(clique_icondition_ids)

    print("total find %d cliques, is finish %s, remain %d iconditions, remained iconditions " % (
        len(clique_data), str(is_finish), len(remained_iconditions)), remained_iconditions)

    if not is_finish:
        raise RuntimeError("clique not complete ...")

    max_clique_ids = find_max_cliques_from_clique_data(clique_data)
    all_ne_isets = iscu.get_iconditions_ne_isets(iconditions)
    all_singleton_isets = iscu.get_iconditions_singleton_isets(iconditions)
    max_clique_data = list()
    for m in max_clique_ids:
        common_empty = all_ne_isets.difference(clique_data[m][2])
        singletons = all_singleton_isets.intersection(clique_data[m][1])
        max_clique_data.append((clique_data[m][0], clique_data[m][1], common_empty, singletons))

    print("\nmax cliques: \n")
    prettify_max_clique_from_clique_data(max_clique_data, clique_file)
    dump_simplified_se_conditions(max_clique_data, sim_file)


def dump_simplified_se_conditions(clique_data, sim_file):
    with open(sim_file, encoding="utf-8", mode="w") as f:
        for data in clique_data:
            common_ne = copy.deepcopy(data[1])
            common_empty = copy.deepcopy(data[2])
            singletons = copy.deepcopy(data[3])
            common_ne = [str(s) for s in common_ne]
            common_empty = [str(s) for s in common_empty]
            singletons = [str(s) for s in singletons]
            condition = "%s:%s:%s\n" % (",".join(common_ne), ",".join(common_empty), ",".join(singletons))
            f.write(condition)


def prettify_max_clique_from_clique_data(clique_data, outf=None):
    if outf is not None:
        outf = open(outf, encoding="utf-8", mode="w")

    total_cnt = 0
    for i in range(len(clique_data)):
        data = clique_data[i]

        ne_strs = ["I%d neq es" % (i + 1) for i in data[1]]
        empty_strs = ["I%d = es" % (i + 1) for i in data[2]]
        singleton_strs = ["|I%d| = 1" % (i + 1) for i in data[3]]

        group_msg = "group %d has %d iconditions \n" % (i, len(data[0]))
        group_msg = group_msg + "\t non-empty:" + ", ".join(ne_strs)
        group_msg = group_msg + "\n\t empty: " + ", ".join(empty_strs)
        group_msg = group_msg + "\n\t singleton: " + ", ".join(singleton_strs)
        total_cnt += len(data[0])
        print(group_msg)
        if outf is not None:
            outf.write(group_msg)
            outf.write("\n")

    msg = "total has %d iconditios" % total_cnt
    print(msg)
    if outf is not None:
        outf.write("\n")
        outf.write(msg)
        outf.write("\n")

        outf.close()


if __name__ == '__main__':
    pass
    