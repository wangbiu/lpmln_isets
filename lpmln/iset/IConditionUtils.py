
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


def load_iconditions_from_file(ic_file, is_ne_formate=True):
    conditions = list()
    if is_ne_formate:
        parse = parse_ne_formate_icondition
    else:
        parse = parse_01_formate_icondition
    with open(ic_file, encoding="utf-8", mode="r") as icf:
        for ic in icf:
            ic = parse(ic)
            conditions.append(ic)

    return conditions


def get_icondition_group_file(k_size, m_size, n_size, min_ne, max_ne):
    group_file = config.get_isc_results_file_path(k_size, m_size, n_size, min_ne, max_ne, "group")
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


def preliminary_group_kmn_iconditions(k_size, m_size, n_size, min_ne, max_ne):
    ic_file = config.get_isc_results_file_path(k_size, m_size, n_size, min_ne, max_ne)
    iconditions = load_iconditions_from_file(ic_file)
    group_file = get_icondition_group_file(k_size, m_size, n_size, min_ne, max_ne)

    groups = dict()
    size = len(iconditions)

    check_cnt = size * (size - 1) // 2
    logging.info("check %d pairs" % check_cnt)
    print_loop = 1000
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

            groups[parent].group_childern.append(child)
            groups[child].group_childern.append(parent)

            if print_cnt % print_loop == 0:
                logging.info("check %d / %d pairs" % (print_cnt, check_cnt))

    logging.info("check %d / %d pairs" % (print_cnt, check_cnt))

    dump_iconditions_groups(groups, group_file)


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
            groups[d] = data[d]



if __name__ == '__main__':
    preliminary_group_kmn_iconditions(0, 2, 1, 1, 33)
    pass
    