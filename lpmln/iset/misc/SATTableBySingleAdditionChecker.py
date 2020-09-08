
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/8 12:58
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : SATTableBySingleAdditionChecker.py
"""

import lpmln.iset.ISetUtils as isu
import copy
from lpmln.iset.misc.BaseSATTableChecker import BaseSATTableChecker


class SATTableBySingleAdditionChecker(BaseSATTableChecker):
    def generate_empty_table(self):
        table = list()
        for i in range(2):
            item = list()
            for j in range(3):
                item.append(set())
            table.append(item)
        return table

    def generate_000_sat_result(self):
        table = self.generate_empty_table()
        for i in range(2):
            for j in range(3):
                table[i][j].add(i)
        return table

    def extract_direct_case_sat_results(self, case_sat_results, total_results):
        for case in case_sat_results:
            if case not in total_results:
                total_results[case] = self.generate_empty_table()
            case_table = total_results[case]
            sat_results = case_sat_results[case]

            for sr in sat_results:
                original_rule_sat = sr[0]
                for i in range(3):
                    case_table[original_rule_sat][i].add(sr[i + 1])

    def print_sat_result_table(self, result_table):
        se_ints = ["(X, Y)  ", "(X, Y+) ", "(X+, Y+)"]
        hline = "--------------------------"
        for case in self.all_case_flags:
            table = result_table[case]
            print(hline)
            for i in range(3):
                print(case + " : " + se_ints[i], "|", table[1][i], "|", table[0][i])

        print(hline, "\n")

    def check_se_relation_for_one_rule(self, rule, atom_universe):
        se_case_results = dict()
        ind_sets = isu.compute_isets_for_sets(rule, self.is_use_extended_rules)
        for key in ind_sets:
            iset = ind_sets[key]
            case_flag = self.get_case_by_intersection_sets(iset.intersect_sets)
            if case_flag not in se_case_results:
                se_case_results[case_flag] = list()

            iset_atoms = iset.members
            if len(iset_atoms) > 0:
            # if len(iset_atoms) == 0:
                add_at = set([-1])
                # interpretation_atoms = atom_universe.union(add_at)
                se_results = self.check_relations_for_one_rule_and_select_iset(atom_universe, add_at, rule,
                                                                               case_flag)
                se_case_results[case_flag].extend(se_results)

        return se_case_results

    def check_relations_for_three_se_and_two_rule(self, here, there, op_atom, rule, case_flag):
        se_sats = list()

        new_rule = copy.deepcopy(rule)
        add_atom = list(op_atom)[0]
        for i in range(3):
            if case_flag[i] == "1":
                new_rule[i].add(add_atom)

        se_sat = self.lp_type.se_satisfy_rule(self.lp_type, here, there, rule)
        se_sats.append(int(se_sat))

        se_sat = self.lp_type.se_satisfy_rule(self.lp_type, here, there, new_rule)
        se_sats.append(int(se_sat))

        there_int = there.union(op_atom)
        se_sat = self.lp_type.se_satisfy_rule(self.lp_type, here, there_int, new_rule)
        se_sats.append(int(se_sat))

        here_int = here.union(op_atom)
        se_sat = self.lp_type.se_satisfy_rule(self.lp_type, here_int, there_int, new_rule)
        se_sats.append(int(se_sat))

        return se_sats

    def __init__(self, lp_type="lpmln"):
        super(SATTableBySingleAdditionChecker, self).__init__(lp_type)


if __name__ == '__main__':
    checker = SATTableBySingleAdditionChecker()
    checker.se_rule_relation_checker(atom_size=5)
    pass
    