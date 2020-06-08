# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/1 19:36
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : SERuleRelationChecker.py
"""

import lpmln.iset.ISetUtils as isu
from lpmln.iset.misc.BaseSATTableChecker import BaseSATTableChecker


class SATTableBySingleDeletionChecker(BaseSATTableChecker):
    def __init__(self, lp_type="lpmln"):
        super(SATTableBySingleDeletionChecker, self).__init__(lp_type)

    def generate_empty_table(self):
        table = list()
        for i in range(3):
            item = list()
            for j in range(2):
                item.append(set())
            table.append(item)
        return table

    def generate_000_sat_result(self):
        table = self.generate_empty_table()
        for model in table:
            model[0].add(0)
            model[1].add(1)
        return table

    def extract_direct_case_sat_results(self, case_sat_results, total_results):
        for case in case_sat_results:
            if case not in total_results:
                total_results[case] = self.generate_empty_table()
            case_table = total_results[case]
            sat_results = case_sat_results[case]

            for sr in sat_results:
                del_rule_sat = sr[3]
                for i in range(0, 3):
                    case_table[i][sr[i]].add(del_rule_sat)

    def print_sat_result_table(self, result_table):
        for case in self.all_case_flags:
            table = result_table[case]
            print(case, "|", table[0][1], "|", table[0][0])

        print("")
        for case in self.all_case_flags:
            table = result_table[case]
            print(case, "|", table[1][1], "|", table[1][0], "|", table[2][1], "|", table[2][0])

        print("\n")

    def check_se_relation_for_one_rule(self, rule, atom_universe):
        se_case_results = dict()

        ind_sets = isu.compute_isets_for_sets(rule)
        for key in ind_sets:
            iset = ind_sets[key]
            case_flag = self.get_case_by_intersection_sets(iset.intersect_sets)
            if case_flag not in se_case_results:
                se_case_results[case_flag] = list()

            if len(iset.members) > 1:
                iset_list = list(iset.members)
                del_at = set(iset_list[0:1])
                interpretation_atoms = atom_universe.difference(del_at)
                se_results = self.check_relations_for_one_rule_and_select_iset(interpretation_atoms, del_at, rule, case_flag)
                se_case_results[case_flag].extend(se_results)

                # debug info
                # if case_flag == "100":
                #     print("\n\t rule: ", rule, "del atom: ", del_at)
                #     print("\t atoms: ", interpretation_atoms)
                #     print("\t iset: ", iset)
                #     print("se results: ", se_results)

        return se_case_results

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

        se_sat = self.lp_type.se_satisfy_rule(self.lp_type, here, there, (head, pb, nb))
        se_sats.append(int(se_sat))

        # debug info
        # if case_flag == "100":
        #     if se_sats[2] == 0:
        #         print("\nrule ", rule)
        #         print("here ", here, "there ", there, "del atom ", del_atom)
        #         print("here+ ", here_int, "there+ ", there_int)

        return se_sats






if __name__ == '__main__':
    checker = SATTableBySingleDeletionChecker()
    # generate_000_sat_result()
    checker.se_rule_relation_checker(3)
    # print(int(False), int(True))
    pass
