
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/8 13:05
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : BaseSATTableChecker.py
"""

import abc
import itertools
import copy
from lpmln.sat.LPMLNSAT import LPMLNSAT
from lpmln.sat.ASPSAT import ASPSAT


class BaseSATTableChecker(abc.ABC):
    def __init__(self, lp_type="lpmln"):
        self.all_case_flags = ["000", "100", "010", "001", "101", "110", "011", "111"]
        if lp_type == "lpmln":
            self.lp_type = LPMLNSAT
        elif lp_type == "asp":
            self.lp_type = ASPSAT
        else:
            raise RuntimeError("unsupported logic formalism %s" % lp_type)

    @abc.abstractmethod
    def generate_empty_table(self):
        print("abstract method")
        pass

    @abc.abstractmethod
    def generate_000_sat_result(self):
        print("abstract method")
        pass

    @abc.abstractmethod
    def extract_direct_case_sat_results(self, case_sat_results, total_results):
        print("abstract method")
        pass

    @abc.abstractmethod
    def print_sat_result_table(self, result_table):
        print("abstract method")
        pass

    def get_case_by_intersection_sets(self, intersection_sets):
        flags = ["0"] * 3
        for i in intersection_sets:
            flags[i] = "1"

        return "".join(flags)

    def generate_all_subsets(self, atom_size=7):
        atom_sets = [i for i in range(atom_size)]
        rule_sets = []
        for i in range(0, atom_size + 1):
            it = itertools.combinations(atom_sets, i)
            for rs in it:
                rule_sets.append(set(rs))
        return rule_sets

    def se_rule_relation_checker(self, atom_size=7):
        result_table = dict()

        rule_sets = self.generate_all_subsets(atom_size)
        set_size = len(rule_sets)
        for hi in range(set_size):
            for pbi in range(set_size):
                for nbi in range(set_size):
                    h_set = copy.deepcopy(rule_sets[hi])
                    pb_set = copy.deepcopy(rule_sets[pbi])
                    nb_set = copy.deepcopy(rule_sets[nbi])

                    atom_universe = set()
                    atom_universe = atom_universe.union(h_set)
                    atom_universe = atom_universe.union(pb_set)
                    atom_universe = atom_universe.union(nb_set)
                    # atom_universe = set([a for a in range(atom_size)])

                    case_sat_results = self.check_se_relation_for_one_rule((set(h_set), set(pb_set), set(nb_set)), atom_universe)
                    self.extract_direct_case_sat_results(case_sat_results, result_table)

        result_table[self.all_case_flags[0]] = self.generate_000_sat_result()
        self.print_sat_result_table(result_table)

    @abc.abstractmethod
    def check_se_relation_for_one_rule(self, rule, atom_universe):
        print("abstract method")
        pass

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

    @abc.abstractmethod
    def check_relations_for_three_se_and_two_rule(self, here, there, op_atom, rule, case_flag):
        print("abstract method")
        pass


if __name__ == '__main__':
    pass
    