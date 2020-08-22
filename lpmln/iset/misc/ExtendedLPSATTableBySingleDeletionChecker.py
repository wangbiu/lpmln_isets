
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-08-21 22:06
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ExtendedLPSATTableBySingleDeletionChecker.py
"""

import itertools
import copy
from lpmln.iset.misc.SATTableBySingleDeletionChecker import SATTableBySingleDeletionChecker


class ExtendedLPSATTableBySingleDeletionChecker(SATTableBySingleDeletionChecker):

    def __init__(self, lp_type="lpmln"):
        super(ExtendedLPSATTableBySingleDeletionChecker, self).__init__(lp_type)
        self.all_case_flags = ["0000", "1000", "0100", "0010", "1010", "1100", "0110", "1110",
                               "0001", "1001", "0101", "0011", "1011", "1101", "0111", "1111"]

    def get_case_by_intersection_sets(self, intersection_sets):
        flags = ["0"] * 4
        for i in intersection_sets:
            flags[i] = "1"

        return "".join(flags)

    def se_rule_relation_checker(self, atom_size=7):
        result_table = dict()

        rule_sets = self.generate_all_subsets(atom_size)
        set_size = len(rule_sets)
        for phi in range(set_size):
            for pbi in range(set_size):
                for nbi in range(set_size):
                    for nhi in range(set_size):
                        ph_set = copy.deepcopy(rule_sets[phi])
                        pb_set = copy.deepcopy(rule_sets[pbi])
                        nb_set = copy.deepcopy(rule_sets[nbi])
                        nh_set = copy.deepcopy(rule_sets[nhi])

                        atom_universe = set()
                        atom_universe = atom_universe.union(ph_set)
                        atom_universe = atom_universe.union(pb_set)
                        atom_universe = atom_universe.union(nb_set)
                        atom_universe = atom_universe.union(nh_set)
                        # atom_universe = set([a for a in range(atom_size)])
                        rule = (set(ph_set), set(pb_set), set(nb_set), set(nh_set))
                        case_sat_results = self.check_se_relation_for_one_rule(rule, atom_universe)
                        self.extract_direct_case_sat_results(case_sat_results, result_table)

        result_table[self.all_case_flags[0]] = self.generate_000_sat_result()
        self.print_sat_result_table(result_table)

    def check_relations_for_one_rule_and_select_iset(self, universe, op_atom, rule, case_flag):
        se_results = list()
        for ti in range(len(universe) + 1):
            there_it = itertools.combinations(universe, ti)
            for there in there_it:
                for hi in range(len(there) + 1):
                    here_it = itertools.combinations(there, hi)
                    for here in here_it:
                        se_sats = self.check_relations_for_three_se_and_two_rule(set(here), set(there), op_atom, rule,
                                                                                 case_flag)
                        se_results.append(se_sats)

                        # debug info
                        # if case_flag == "100":
                        #     if se_sats[2] == 0:
                        #         print("\nrule ", rule)
                        #         print("here ", here, "there ", there, "del atom ", del_atom)

        return se_results

    def check_relations_for_three_se_and_two_rule(self, here, there, op_atom, rule, case_flag):
        se_sats = list()

        se_sat = self.lp_type.se_satisfy_rule(self.lp_type, here, there, rule)
        se_sats.append(int(se_sat))

        there_int = there.union(op_atom)
        se_sat = self.lp_type.se_satisfy_rule(self.lp_type, here, there_int, rule)
        se_sats.append(int(se_sat))

        here_int = here.union(op_atom)
        se_sat = self.lp_type.se_satisfy_rule(self.lp_type, here_int, there_int, rule)
        se_sats.append(int(se_sat))

        head = rule[0].difference(op_atom)
        pb = rule[1].difference(op_atom)
        nb = rule[2].difference(op_atom)
        nh = rule[3].difference(op_atom)

        se_sat = self.lp_type.se_satisfy_rule(self.lp_type, here, there, (head, pb, nb, nh))
        se_sats.append(int(se_sat))

        # debug info
        # if case_flag == "100":
        #     if se_sats[2] == 0:
        #         print("\nrule ", rule)
        #         print("here ", here, "there ", there, "del atom ", del_atom)
        #         print("here+ ", here_int, "there+ ", there_int)

        return se_sats


if __name__ == '__main__':
    # checker = ExtendedLPSATTableBySingleDeletionChecker(lp_type="asp")
    checker = ExtendedLPSATTableBySingleDeletionChecker()
    # generate_000_sat_result()
    checker.se_rule_relation_checker(3)
    pass
    