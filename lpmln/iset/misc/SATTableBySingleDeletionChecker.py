# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/1 19:36
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : SERuleRelationChecker.py
"""

import lpmln.iset.ISetUtils as isu
from lpmln.iset.misc.BaseSATTableChecker import BaseSATTableChecker
import lpmln.iset.misc.SESATTableUtils as seut


class SATTableBySingleDeletionChecker(BaseSATTableChecker):
    def __init__(self, lp_type="lpmln"):
        super(SATTableBySingleDeletionChecker, self).__init__(lp_type)

    def generate_empty_table(self):
        table = seut.generate_single_deletion_empty_table()
        return table

    def generate_000_sat_result(self):
        table = seut.generate_0000_single_deletion_sat_result()
        return table

    def extract_direct_case_sat_results(self, case_sat_results, total_results):
        seut.extract_direct_single_deletion_case_sat_results(case_sat_results, total_results)

    def print_sat_result_table(self, result_table):
        seut.print_single_deletion_sat_result_table(self.all_case_flags, result_table)

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
