
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-25 21:35
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : IConditionUtils.py
"""


from lpmln.iset.ISetCondition import ISetCondition
import lpmln.config.GlobalConfig as cfg
import lpmln.iset.ISetCompositionUtils as iscom
config = cfg.load_configuration()
from sympy import symbols, simplify, true, false
from sympy.logic.boolalg import And, Or, Not, to_dnf
import itertools
from lpmln.iset.IConditionGroup import IConditionGroup
import json
import logging
import copy


def load_iconditions_from_file(ic_file, is_ne_formate=True):
    conditions = list()
    if is_ne_formate:
        parse = parse_ne_formate_icondition
    else:
        parse = parse_01_formate_icondition
    with open(ic_file, encoding="utf-8", mode="r") as icf:
        for ic in icf:
            ic = ic.strip("\r\n ")
            ic = parse(ic)
            conditions.append(ic)

    return conditions


def get_icondition_group_file(k_size, m_size, n_size, min_ne, max_ne, type):
    group_file = config.get_isc_results_file_path(k_size, m_size, n_size, min_ne, max_ne, type)
    group_file = group_file + ".group"
    return group_file


def get_icondition_refine_group_file(k_size, m_size, n_size, min_ne, max_ne, type,level=0):
    group_file = get_icondition_group_file(k_size, m_size, n_size, min_ne, max_ne, type)
    group_file = group_file + ".refine"
    if level != 0:
        group_file = group_file + "-%d" % level
    return group_file


def parse_ne_formate_icondition(data):
    cdt, singleton = parse_raw_icondition_data(data)
    condition = ISetCondition(list(), list(), True)
    condition.set_ne_iset_ids(set(cdt))
    condition.singletom_iset_ids = set(singleton)
    return condition


def parse_01_formate_icondition(data):
    cdt, singleton = parse_raw_icondition_data(data)
    condition = ISetCondition(cdt, singleton, False)
    return condition


def parse_raw_icondition_data(data):
    if data == "":
        return list(), list()

    data = data.split(":")
    cdt = data[0].split(",")
    cdt = [int(s) for s in cdt]
    if len(data) == 1:
        singleton = list()
    else:
        singleton = data[1].split(",")
        singleton = [int(s) for s in singleton]

    return cdt, singleton


def get_iconditions_ne_isets(iconditions):
    ne_isets = set()
    for ic in iconditions:
        ne_isets = ne_isets.union(ic.ne_iset_ids)

    return ne_isets


def get_iconditions_ne_isets_logic_symbols(iconditions, ignore_ne_isets):
    ne_isets = get_iconditions_ne_isets(iconditions)
    ne_symboles = dict()
    for ne in ne_isets:
        if ne not in ignore_ne_isets:
            ne_symboles[ne] = symbols("a"+str(ne+1))

    return ne_symboles


def convert_icondition_2_conjunctive_formula(icondition, ne_symbols):
    ic_symbols = list()
    for ne in ne_symbols:
        if ne in icondition.ne_iset_ids:
            ic_symbols.append(ne_symbols[ne])
        else:
            ic_symbols.append(Not(ne_symbols[ne]))

    if len(ic_symbols) == 0:
        return true

    formula = ic_symbols[0]
    for i in range(1, len(ic_symbols)):
        formula = And(formula, ic_symbols[i])

    return formula


def convert_iconditions_2_disjunctive_formula(iconditions, ne_symbols):
    conjunctive_formulas = list()
    for ic in iconditions:
        f = convert_icondition_2_conjunctive_formula(ic, ne_symbols)
        conjunctive_formulas.append(f)

    if len(conjunctive_formulas) == 0:
        return True

    dnf = conjunctive_formulas[0]
    for i in range(1, len(conjunctive_formulas)):
        dnf = Or(dnf, conjunctive_formulas[i])

    return dnf


def simplify_iconditions(iconditions, ignore_ne_isets=set()):
    ne_symbols = get_iconditions_ne_isets_logic_symbols(iconditions, ignore_ne_isets)
    dnf = convert_iconditions_2_disjunctive_formula(iconditions, ne_symbols)
    simplified_dnf = simplify(dnf)
    sim_dnf = to_dnf(dnf)
    return simplified_dnf, sim_dnf


def find_common_ne_isets_from_iconditions(iconditions):
    ne_isets = get_iconditions_ne_isets(iconditions)
    ne_isets_count = dict()
    for ne in ne_isets:
        ne_isets_count[ne] = 0

    for ic in iconditions:
        for ne in ic.ne_iset_ids:
            ne_isets_count[ne] += 1

    common_ne_isets = set()
    for ne in ne_isets_count:
        if ne_isets_count[ne] == len(iconditions):
            common_ne_isets.add(ne)

    return common_ne_isets


def normalize_iconditions(iconditions, outf=None):
    ne_isets = get_iconditions_ne_isets(iconditions)
    ne_isets = list(ne_isets)
    ne_isets.sort()
    normalized_iconditions = list()
    tmp_iconditions = dict()
    ne_numbers = set()
    for ic in iconditions:
        ic_ne_isets = list()
        ne_number = len(ic.ne_iset_ids)
        ne_numbers.add(ne_number)
        for ne in ne_isets:
            if ne in ic.ne_iset_ids:
                ic_ne_isets.append(ne)
            else:
                ic_ne_isets.append(-2)
        if ne_number not in tmp_iconditions:
            tmp_iconditions[ne_number] = list()

        tmp_iconditions[ne_number].append(ic_ne_isets)

    ne_numbers = list(ne_numbers)
    ne_numbers.sort()
    for ne in ne_numbers:
        for ic in tmp_iconditions[ne]:
            normalized_iconditions.append(ic)

    nm_ic_str = list()
    for ic in normalized_iconditions:
        ic_str = [str(int(s)+1) for s in ic]
        nm_ic_str.append(",".join(ic_str))
        # print(",".join(ic_str))

    if outf is not None:
        with open(outf, encoding="utf-8", mode="w") as f:
            for ic in nm_ic_str:
                f.write(ic)
                f.write("\n")
    return nm_ic_str


def count_ne_iset_occurrences(iconditions):
    ne_isets = get_iconditions_ne_isets(iconditions)
    ne_isets = list(ne_isets)
    ne_isets.sort()

    stat = dict()
    for ne in ne_isets:
        stat[ne] = 0

    for ic in iconditions:
        for ne in ne_isets:
            if ne in ic.ne_iset_ids:
                stat[ne] += 1

    for ne in stat:
        print("ne %d : %d" % (ne+1, stat[ne]))

    return stat


def count_ne_iset_co_occurrences(iconditions, order=2):
    ne_isets = get_iconditions_ne_isets(iconditions)
    ne_isets = list(ne_isets)
    ne_isets.sort()

    co_ne_iter = itertools.combinations(ne_isets, order)

    for co_ne in co_ne_iter:
        cnt = 0
        for ic in iconditions:
            is_co_occur = True
            for ne in set(co_ne):
                if ne not in ic.ne_iset_ids:
                    is_co_occur = False
            if is_co_occur:
                cnt += 1
        print(co_ne, ": %d" % cnt)


def get_iconditions_not_contains_all_ne_isets(iconditions, ne_isets):
    conditions = list()
    for ic in iconditions:
        is_remove = False
        for ne in ne_isets:
            if ne in ic.ne_iset_ids:
                is_remove = True
                break
        if not is_remove:
            conditions.append(ic)

    return conditions


def get_iconditions_contains_all_ne_isets(iconditions, ne_isets):
    conditions = list()
    for ic in iconditions:
        is_remove = False
        for ne in ne_isets:
            if ne not in ic.ne_iset_ids:
                is_remove = True
                break
        if not is_remove:
            conditions.append(ic)

    return conditions


def get_iconditions_contains_at_least_one_of_ne_isets(iconditions, ne_isets):
    conditions = list()
    for ic in iconditions:
        contain = icondition_contains_at_least_one_of_ne_isets(ic, ne_isets)
        if contain:
            conditions.append(ic)
    return conditions


def icondition_contains_all_ne_isets(icondition, ne_isets):
    for ne in ne_isets:
        if ne not in icondition.ne_iset_ids:
            return False
    return True


def icondition_contains_at_least_one_of_ne_isets(icondition, ne_isets):
    for ne in ne_isets:
        if ne in icondition.ne_iset_ids:
            return True
    return False

def get_rule_sets(rule_number, is_use_extended_rules):
    rule_sets_names = ["h(%d)", "pb(%d)", "nb(%d)"]
    if is_use_extended_rules:
        rule_sets_names = ["h(%d)", "pb(%d)", "nb(%d)"]
    rule_sets = list()
    for i in range(1, rule_number + 1):
        for rs in rule_sets_names:
            rs = rs % i
            rule_sets.append(rs)

    return rule_sets


def complete_and_analyse_icondition(ne_isets, condition_space_isets, meta):
    search_space_iset_ids = set(meta.search_space_iset_ids)
    non_se_iset_ids = set(meta.non_se_iset_ids)
    search_space_isets = search_space_iset_ids.union(non_se_iset_ids)
    e_isets = search_space_isets.difference(condition_space_isets)
    free_isets = condition_space_isets.difference(ne_isets)

    rule_number = sum(meta.kmn)

    rule_sets = get_rule_sets(rule_number, meta.is_use_extended_rules)
    condition_comp = dict()
    for rs in rule_sets:
        condition_comp[rs] = list()

    compute_isets_compostions(condition_comp, ne_isets, "non-empty", rule_sets)

    compute_isets_compostions(condition_comp, e_isets, "empty", rule_sets)

    compute_isets_compostions(condition_comp, free_isets, "free", rule_sets)

    return condition_comp


def compute_isets_compostions(comp_data, isets, label, rule_sets):
    for s in comp_data:
        comp_data[s].append(label)

    for s in isets:
        comp = iscom.get_iset_binary_bits(s + 1, len(rule_sets))
        for i in range(len(comp)):
            if comp[i] == 1:
                comp_data[rule_sets[i]].append(s)


def preliminary_group_kmn_iconditions(k_size, m_size, n_size, min_ne, max_ne, type):
    ic_file = config.get_isc_results_file_path(k_size, m_size, n_size, min_ne, max_ne, type)
    iconditions = load_iconditions_from_file(ic_file)
    group_file = get_icondition_group_file(k_size, m_size, n_size, min_ne, max_ne, type)

    groups = dict()
    size = len(iconditions)

    check_cnt = size * (size - 1) // 2
    logging.info("check %d pairs" % check_cnt)
    print_loop = 100000
    print_cnt = 0

    for i in range(size):
        gp = IConditionGroup(i)
        groups[i] = gp

    for i in range(size):
        for j in range(i+1, size):
            print_cnt += 1
            ne1 = set(iconditions[i].ne_iset_ids)
            ne2 = set(iconditions[j].ne_iset_ids)

            if ne1.issubset(ne2):
                parent = j
                child = i
            elif ne2.issubset(ne1):
                parent = i
                child = j
            else:
                continue

            groups[parent].group_children.append(child)
            groups[child].group_parents.append(parent)

            if print_cnt % print_loop == 0:
                logging.info("check %d / %d pairs" % (print_cnt, check_cnt))

    logging.info("check %d / %d pairs" % (print_cnt, check_cnt))

    dump_iconditions_groups(groups, group_file)


def refine_iconditions_groups(k_size, m_size, n_size, min_ne, max_ne, type):
    group_file = get_icondition_group_file(k_size, m_size, n_size, min_ne, max_ne, type)
    ic_file = config.get_isc_results_file_path(k_size, m_size, n_size, min_ne, max_ne, type)
    iconditions = load_iconditions_from_file(ic_file)
    groups = load_iconditions_groups(group_file)
    refine_cnt = 0
    # ic_id = 1
    # print(iconditions[ic_id])
    # print(groups[ic_id])

    roots = list()
    for g in groups:
        groups[g].to_set()
        if len(groups[g].group_parents) == 0:
            roots.append(g)

    # for r in roots:
    #     print(r, iconditions[r])

    for g in groups:
        refine_cnt += 1
        current = groups[g]
        parents = current.group_parents
        redundant_parents = set()
        for p in parents:
            pa = groups[p]
            pa_parents = pa.group_parents
            common_parents = parents.intersection(pa_parents)
            redundant_parents = redundant_parents.union(common_parents)
            for cp in common_parents:
                cp_pa = groups[cp]
                if g in cp_pa.group_children:
                    cp_pa.group_children.remove(g)

        current.group_parents = parents.difference(redundant_parents)

    print("\t refine %d groups" % refine_cnt)

    for g in groups:
        groups[g].to_list()
    group_file = get_icondition_refine_group_file(k_size, m_size, n_size, min_ne, max_ne, type)
    dump_iconditions_groups(groups, group_file)


def compute_common_isets_from_groups_and_iconditions(groups, iconditions):
    compute_groups = set()
    total_groups = set()
    iset_space = get_iconditions_ne_isets(iconditions)

    leaves = list()
    compute_cnt = 0
    for g in groups:
        total_groups.add(g)
        if len(groups[g].group_children) == 0:
            leaves.append(g)
            ic_ne_isets = copy.deepcopy(iconditions[g].ne_iset_ids)
            ic_ne_isets = list(ic_ne_isets)
            groups[g].group_common_ne_isets = ic_ne_isets
            compute_groups.add(g)
            compute_cnt += 1

    while len(leaves) > 0:
        new_leaves = list()
        for g in leaves:
            parents = groups[g].group_parents
            for p in parents:
                if p in compute_groups:
                    continue

                # print("compute common atoms of leaf ", p)
                compute_cnt += 1
                compute_groups.add(p)
                new_leaves.append(p)
                element_list = list()
                children = groups[p].group_children
                for ch in children:
                    element_list.append(copy.deepcopy(groups[ch].group_common_ne_isets))
                common = get_common_elements(element_list)
                groups[p].group_common_ne_isets = common
        leaves = new_leaves
        print("\t compute %d nodes " % compute_cnt, "remained groups", total_groups.difference(compute_groups))

    print("compute descendant number ")
    for g in groups:
        desc = get_group_descentdants(groups, g)
        groups[g].group_descendant_number = len(desc)
        group_ne_isets = set()
        group_ne_isets = group_ne_isets.union(iconditions[g].ne_iset_ids)
        for d in desc:
            group_ne_isets = group_ne_isets.union(iconditions[d].ne_iset_ids)
        group_common_empty_isets = iset_space.difference(group_ne_isets)
        groups[g].group_common_empty_isets = list(group_common_empty_isets)


def compute_common_isets(k_size, m_size, n_size, min_ne, max_ne, type):
    group_file = get_icondition_refine_group_file(k_size, m_size, n_size, min_ne, max_ne, type)
    outf = get_icondition_refine_group_file(k_size, m_size, n_size, min_ne, max_ne, type, 1)
    groups = load_iconditions_groups(group_file)
    ic_file = config.get_isc_results_file_path(k_size, m_size, n_size, min_ne, max_ne, type)
    iconditions = load_iconditions_from_file(ic_file)
    compute_common_isets_from_groups_and_iconditions(groups, iconditions)
    dump_iconditions_groups(groups, outf)


def get_common_elements(element_lists):
    all_elements = list()
    common = list()
    for ele in element_lists:
        all_elements.extend(ele)

    all_elements = set(all_elements)

    for e in all_elements:
        is_common = True
        for ele in element_lists:
            if e not in ele:
                is_common = False
        if is_common:
            common.append(e)

    return common


def check_max_clique(groups, root_id, condition):
    ne_isets = condition.ne_iset_ids
    root = groups[root_id]
    common_ne_isets = root.group_common_ne_isets
    # common_empty_isets = root.group_common_empty_isets

    max_clique_size = 2 ** (len(ne_isets) - len(common_ne_isets)) - 1
    # print("group %d max clique size %d descendant size %d " % (root_id, max_clique_size, root.group_descendant_number))
    if max_clique_size == root.group_descendant_number:
        return True
    else:
        return False


def find_max_clique(k_size, m_size, n_size, min_ne, max_ne, type):
    group_file = get_icondition_refine_group_file(k_size, m_size, n_size, min_ne, max_ne, type, 1)
    clique_file = get_icondition_refine_group_file(k_size, m_size, n_size, min_ne, max_ne, type) + ".clique"

    groups = load_iconditions_groups(group_file)
    ic_file = config.get_isc_results_file_path(k_size, m_size, n_size, min_ne, max_ne, type)
    iconditions = load_iconditions_from_file(ic_file)
    roots = get_group_roots(groups)
    used_nodes = set()
    max_cliques = set()

    print("roots", roots)

    has_clique = False
    while len(roots) > 0:
        new_roots = list()
        for r in roots:
            if r in used_nodes:
                continue

            print("check root ", r)
            is_max_clique = check_max_clique(groups, r, iconditions[r])
            if is_max_clique:
                max_cliques.add(r)
                desc = get_group_descentdants(groups, r)
                used_nodes = used_nodes.union(desc)
            else:
                new_roots.extend(groups[r].group_children)

        roots = set(new_roots)

    prettify_max_clique(groups, max_cliques, iconditions, clique_file)


def find_max_clique_2(k_size, m_size, n_size, min_ne, max_ne, type):
    group_file = get_icondition_refine_group_file(k_size, m_size, n_size, min_ne, max_ne, type, 1)
    clique_file = get_icondition_refine_group_file(k_size, m_size, n_size, min_ne, max_ne, type) + ".clique"

    groups = load_iconditions_groups(group_file)
    ic_file = config.get_isc_results_file_path(k_size, m_size, n_size, min_ne, max_ne, type)
    iconditions = load_iconditions_from_file(ic_file)
    roots = get_group_roots(groups)

    all_group_ids = set()
    for g in groups:
        all_group_ids.add(g)

    used_nodes = set()
    max_cliques = set()

    print("roots", roots)

    while len(roots) > 0:
        new_roots = list()
        for r in roots:
            if r in used_nodes:
                continue

            is_max_clique = check_max_clique(groups, r, iconditions[r])
            if is_max_clique:
                max_cliques.add(r)
                desc = get_group_descentdants(groups, r)
                used_nodes = used_nodes.union(desc)
                used_nodes.add(r)
                remove_clique_from_groups(groups, r)
                compute_common_isets_from_groups_and_iconditions(groups, iconditions)
            else:
                new_roots.extend(groups[r].group_children)

        roots = set(new_roots)

    print("remained groups: ", all_group_ids.difference(used_nodes))
    prettify_max_clique(groups, max_cliques, iconditions, clique_file)


def find_max_clique_3(k_size, m_size, n_size, min_ne, max_ne, type):
    group_file = get_icondition_refine_group_file(k_size, m_size, n_size, min_ne, max_ne, type, 1)
    clique_file = get_icondition_refine_group_file(k_size, m_size, n_size, min_ne, max_ne, type) + ".clique"

    groups = load_iconditions_groups(group_file)
    ic_file = config.get_isc_results_file_path(k_size, m_size, n_size, min_ne, max_ne, type)
    iconditions = load_iconditions_from_file(ic_file)
    roots = get_group_roots(groups)

    all_group_ids = set()
    for g in groups:
        all_group_ids.add(g)

    used_nodes = set()
    max_cliques = set()

    print("roots", roots)

    clique_id = -1
    for r in roots:
        while clique_id != r:
            clique_id = find_a_max_clique_from_root(groups, r, iconditions)
            print("find clique %d" % clique_id)
            max_cliques.add(clique_id)
            desc = get_group_descentdants(groups, clique_id)
            used_nodes = used_nodes.union(desc)
            used_nodes.add(clique_id)
            remove_clique_from_groups(groups, clique_id)
            compute_common_isets_from_groups_and_iconditions(groups, iconditions)

    print("remained groups: ", all_group_ids.difference(used_nodes))
    prettify_max_clique(groups, max_cliques, iconditions, clique_file)


def find_a_max_clique_from_root(groups, root_id, iconditions):
    roots = [root_id]
    clique_id = -1
    while len(roots) > 0:
        new_roots = list()
        for r in roots:
            root = groups[r]
            is_max_clique = check_max_clique(groups, r, iconditions[r])
            if is_max_clique:
                clique_id = r
                break
            else:
                new_roots.extend(root.group_children)

        if clique_id != -1:
            break

        roots = set(new_roots)

    return clique_id


def remove_clique_from_groups(groups, clique_id):
    clique = groups[clique_id]
    desc = get_group_descentdants(groups, clique_id)
    desc.add(clique_id)
    nodes = clique.group_parents

    while len(nodes) > 0:
        new_nodes = set()
        for n in nodes:
            children = copy.deepcopy(groups[n].group_children)
            children = set(children)
            children = children.difference(desc)
            new_nodes = new_nodes.union(children)
            groups[n].group_children = list(children)
        nodes = new_nodes


def prettify_max_clique(groups, cliques, iconditions, outf=None):
    if outf is not None:
        outf = open(outf, encoding="utf-8", mode="w")

    total_cnt = 0
    for c in cliques:
        desc = get_group_descentdants(groups, c)
        node = groups[c]
        ne_strs = ["I%d neq es" % (i + 1) for i in node.group_common_ne_isets]
        empty_strs = ["I%d = es" % (i + 1) for i in node.group_common_empty_isets]

        parents = [str(s) for s in node.group_parents]
        parents = "[%s]" % (", ".join(parents))
        group_msg = "group %d has %d iconditions, ne isets: %s, parents: %s \n" % (c, len(desc) + 1, str(iconditions[c]), parents)
        group_msg = group_msg + "\t" + ", ".join(ne_strs)
        group_msg = group_msg + "\n\t" + ", ".join(empty_strs)
        total_cnt += len(desc) + 1
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


def process_children(groups, roots):
    for r in roots:
        parent = groups[r]
        children = parent.group_children
        new_childern = copy.deepcopy(children)
        for ch in children:
            child = groups[ch]
            if len(child.group_parents) > 1:
                new_childern.remove(ch)
                child.group_parents.remove(r)

        parent.group_children = new_childern
        new_roots = copy.deepcopy(new_childern)
        process_children(groups, new_roots)


def dump_iconditions_groups(groups, group_file):
    group_json = dict()
    for g in groups:
        group_json[g] = groups[g].to_map()

    with open(group_file, encoding="utf-8", mode="w") as f:
        json.dump(group_json, f, indent=2)


def load_iconditions_groups(group_file):
    with open(group_file, encoding="utf-8", mode="r") as f:
        data = json.load(f)
        groups = dict()
        for d in data:
            gp = IConditionGroup(0)
            gp.load_from_map(data[d])
            groups[int(d)] = gp
        return groups


def split_iconditions(k_size, m_size, n_size, min_ne, max_ne, type):
    ic_file = config.get_isc_results_file_path(k_size, m_size, n_size, min_ne, max_ne, type)
    iconditions = load_iconditions_from_file(ic_file)
    ns_ic_file = config.get_isc_results_file_path(k_size, m_size, n_size, min_ne, max_ne, "ns")
    s_ic_file = config.get_isc_results_file_path(k_size, m_size, n_size, min_ne, max_ne, "s")

    ns_ic_file = open(ns_ic_file, mode="w", encoding="utf-8")
    s_ic_file = open(s_ic_file, mode="w", encoding="utf-8")

    for ic in iconditions:
        if len(ic.singletom_iset_ids) == 0:
            ns_ic_file.write(str(ic))
            ns_ic_file.write("\n")
        else:
            s_ic_file.write(str(ic))
            s_ic_file.write("\n")

    ns_ic_file.close()
    s_ic_file.close()






if __name__ == '__main__':
    params = (0, 2, 1, 1, 33)
    preliminary_group_kmn_iconditions(*params, "")
    group_file = get_icondition_group_file(*params, "")
    # groups = load_iconditions_groups(group_file, "")
    # print(len(groups))
    # print(groups[0])
    refine_iconditions_groups(*params, "")
    # split_iconditions(1,1,1,1,45, "")
    compute_common_isets(*params, "")
    pass
    