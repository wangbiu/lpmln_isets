# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/1 19:36
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : SERuleRelationChecker.py
"""

import itertools
import copy
from lpmln.sat.LPMLNSAT import LPMLNSAT
import lpmln.iset.ISetUtils as isu

all_case_flags = ["000", "100", "010", "001", "101", "110", "011", "111"]


def generate_empty_table():
    table = list()
    for i in range(3):
        item = list()
        for j in range(2):
            item.append(set())
        table.append(item)
    return table


def generate_000_sat_result():
    table = generate_empty_table()
    for model in table:
        model[0].add(0)
        model[1].add(1)
    return table


def extract_case_sat_results(case_sat_results, total_results):
    for case in case_sat_results:
        if case not in total_results:
            total_results[case] = generate_empty_table()
        case_table = total_results[case]
        sat_results = case_sat_results[case]

        for sr in sat_results:
            rule_sat = sr[0]
            for i in range(1, 3):
                case_table[i][sr[i]].add(rule_sat)
            case_table[0][rule_sat].add(sr[3])


def extract_direct_case_sat_results(case_sat_results, total_results):
    for case in case_sat_results:
        if case not in total_results:
            total_results[case] = generate_empty_table()
        case_table = total_results[case]
        sat_results = case_sat_results[case]

        for sr in sat_results:
            del_rule_sat = sr[3]
            for i in range(0, 3):
                case_table[i][sr[i]].add(del_rule_sat)


def print_sat_result_table(result_table):
    for case in all_case_flags:
        table = result_table[case]
        print(case, "|", table[0][1], "|", table[0][0])

    print("")
    for case in all_case_flags:
        table = result_table[case]
        print(case, "|", table[1][1], "|", table[1][0], "|", table[2][1], "|", table[2][0])

    print("\n")


def generate_all_subsets(atom_size=7):
    atom_sets = [i for i in range(atom_size)]
    rule_sets = []
    for i in range(0, atom_size + 1):
        it = itertools.combinations(atom_sets, i)
        for rs in it:
            rule_sets.append(set(rs))
    return rule_sets


def se_rule_relation_checker(atom_size=7):
    result_table = dict()

    rule_sets = generate_all_subsets(atom_size)
    set_size = len(rule_sets)
    for hi in range(set_size):
        for pbi in range(set_size):
            for nbi in range(set_size):
                h_set = copy.deepcopy(rule_sets[hi])
                pb_set = copy.deepcopy(rule_sets[pbi])
                nb_set = copy.deepcopy(rule_sets[nbi])

                case_sat_results = check_se_relation_for_one_rule((set(h_set), set(pb_set), set(nb_set)), atom_size)
                extract_direct_case_sat_results(case_sat_results, result_table)

    result_table[all_case_flags[0]] = generate_000_sat_result()
    print_sat_result_table(result_table)


def check_se_relation_for_one_rule(rule, atom_size=7):
    atom_set = set([i for i in range(atom_size)])
    se_case_results = dict()

    ind_sets = isu.compute_isets_for_sets(rule)
    for key in ind_sets:
        iset = ind_sets[key]
        case_flag = get_case_by_intersection_sets(iset.intersect_sets)
        if case_flag not in se_case_results:
            se_case_results[case_flag] = list()

        iset_atoms = iset.members
        # print(case_flag, iset_atoms)
        if len(iset_atoms) > 1:
            iset_list = list(iset_atoms)
            del_at = set(iset_list[0:1])
            interpretation_atoms = atom_set.difference(del_at)
            se_results = check_relations_for_one_rule_and_select_iset(interpretation_atoms, del_at, rule, case_flag)
            se_case_results[case_flag].extend(se_results)

            # debug info
            # if case_flag == "100":
            #     print("\n\t rule: ", rule, "del atom: ", del_at)
            #     print("\t atoms: ", interpretation_atoms)
            #     print("\t iset: ", iset)
            #     print("se results: ", se_results)


    return se_case_results


def check_relations_for_one_rule_and_select_iset(universe, del_atom, rule, case_flag):
    se_results = list()
    for ti in range(len(universe) + 1):
        there_it = itertools.combinations(universe, ti)
        for there in there_it:
            for hi in range(len(there)):
                here_it = itertools.combinations(there, hi)
                for here in here_it:
                    se_sats = check_relations_for_three_se_and_two_rule(set(here), set(there), del_atom, rule, case_flag)
                    se_results.append(se_sats)

                    # debug info
                    # if case_flag == "100":
                    #     if se_sats[2] == 0:
                    #         print("\nrule ", rule)
                    #         print("here ", here, "there ", there, "del atom ", del_atom)

    return se_results


def check_relations_for_three_se_and_two_rule(here, there, del_atom, rule, case_flag):
    se_sats = list()

    se_sat = LPMLNSAT.se_satisfy_rule(LPMLNSAT, here, there, rule)
    se_sats.append(int(se_sat))

    there_int = there.union(del_atom)
    se_sat = LPMLNSAT.se_satisfy_rule(LPMLNSAT, here, there_int, rule)
    se_sats.append(int(se_sat))

    here_int = here.union(del_atom)
    se_sat = LPMLNSAT.se_satisfy_rule(LPMLNSAT, here_int, there_int, rule)
    se_sats.append(int(se_sat))

    head = rule[0].difference(del_atom)
    pb = rule[1].difference(del_atom)
    nb = rule[2].difference(del_atom)

    se_sat = LPMLNSAT.se_satisfy_rule(LPMLNSAT, here, there, (head, pb, nb))
    se_sats.append(int(se_sat))

    # debug info
    # if case_flag == "100":
    #     if se_sats[2] == 0:
    #         print("\nrule ", rule)
    #         print("here ", here, "there ", there, "del atom ", del_atom)
    #         print("here+ ", here_int, "there+ ", there_int)

    return se_sats


def get_case_by_intersection_sets(intersection_sets):
    flags = ["0"] * 3
    for i in intersection_sets:
        flags[i] = "1"

    return "".join(flags)


if __name__ == '__main__':
    # generate_000_sat_result()
    se_rule_relation_checker(5)
    # print(int(False), int(True))
    pass
