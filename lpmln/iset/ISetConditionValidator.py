
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/7 13:04
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ISetConditionValidator.py
"""

import lpmln.iset.ISetUtils as isu
from lpmln.sat.LPMLNSEChecking import LPMLNSEChecking
from lpmln.sat.ASPSEChecking import ASPSEChecking
import copy


class ISetConditionValidator:
    def __init__(self, lp_type="lpmln"):
        if lp_type == "asp":
            self.lp_se = ASPSEChecking
            raise RuntimeError("not support for now")
        elif lp_type == "lpmln":
            self.lp_se = LPMLNSEChecking
        else:
            raise RuntimeError("unknown SE checking logic: %s" % lp_type)

    def validate_isets_kmn_program(self, isets, k_size, m_size, n_size, is_check_valid_rule=True):
        kmn = isu.construct_kmn_program_from_isets(isets, k_size, m_size, n_size)
        is_contain_valid_rule = False
        is_se_sat = False

        if is_check_valid_rule:
            for prg in kmn:
                if self.lp_se.is_contain_se_valid_rule(self.lp_se, prg):
                    is_contain_valid_rule = True
                    break

            if is_contain_valid_rule:
                return is_contain_valid_rule, is_se_sat

        is_se_sat = self.lp_se.se_check_kmn_program(*kmn)
        return is_contain_valid_rule, is_se_sat

    def join_data_list(self, data_list, delimiter=","):
        str_list = [str(d) for d in data_list]
        return delimiter.join(str_list)

    def validate_isets_kmn_program_from_non_empty_ids(self, non_empty_ids, k_size, m_size, n_size, is_check_valid_rule=True):
        iset_number = isu.compute_iset_number_from_kmn(k_size, m_size, n_size)
        icondition = isu.construct_iset_condition_from_non_emtpy_iset_ids(non_empty_ids, iset_number)
        return self.validate_isets_kmn_program_from_iset_condition(icondition, k_size, m_size, n_size, is_check_valid_rule)

    def validate_isets_kmn_program_from_iset_condition(self, icondition, k_size, m_size, n_size, is_check_valid_rule=True):
        isets = isu.construct_isets_from_iset_condition(icondition, iset_atom_number=1)
        is_contain_valid, is_se_sat = self.validate_isets_kmn_program(isets, k_size, m_size, n_size, is_check_valid_rule)
        return is_contain_valid, is_se_sat, self.join_data_list(icondition, ",")

    def validate_isets_kmn_program_from_icondition_id(self, icondition_id, k_size, m_size, n_size, is_check_valid_rule=True):
        iset_number = isu.compute_iset_number_from_kmn(k_size, m_size, n_size)
        icondition = isu.construct_iset_condition_from_icondition_id(icondition_id, iset_number)
        return self.validate_isets_kmn_program_from_iset_condition(icondition, k_size, m_size, n_size, is_check_valid_rule)

    def validate_kmn_extended_iset_condition(self, icondition, k_size, m_size, n_size, is_check_valid_rule=True):
        isets = isu.construct_isets_from_iset_condition(icondition)
        singleton_iset_ids = list()
        is_contain_valid, is_se_sat = self.validate_isets_kmn_program(isets, k_size, m_size, n_size, is_check_valid_rule)
        if is_contain_valid or not is_se_sat:
            return is_contain_valid, is_se_sat, self.join_data_list(icondition, ",")

        non_empty_ids = list()
        for key in isets:
            if len(isets[key].members) != 0:
                non_empty_ids.append(key)
        new_atom = 0

        for nid in non_empty_ids:
            extended_isets = copy.deepcopy(isets)
            extended_isets[nid].members.add(new_atom)
            is_contain_valid, is_se_sat = self.validate_isets_kmn_program(extended_isets, k_size, m_size, n_size, is_check_valid_rule=False)
            if not is_se_sat:
                singleton_iset_ids.append(nid)

        is_se_sat = True
        condition = self.join_data_list(icondition, ",")
        if len(singleton_iset_ids) != 0:
            singleton_sets = self.join_data_list(singleton_iset_ids, ",")
            condition = condition + ":" + singleton_sets
        return is_contain_valid, is_se_sat, condition

    def validate_kmn_extended_iset_condition_from_non_emtpy_iset_ids(self, non_emtpy_iset_ids, k_size, m_size, n_size, is_check_valid_rule=True):
        iset_number = isu.compute_iset_number_from_kmn(k_size, m_size, n_size)
        icondition = isu.construct_iset_condition_from_non_emtpy_iset_ids(non_emtpy_iset_ids, iset_number)
        return self.validate_kmn_extended_iset_condition(icondition, k_size, m_size, n_size, is_check_valid_rule)


if __name__ == '__main__':
    pass
    