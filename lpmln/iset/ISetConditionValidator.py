
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


class ISetConditionValidator():
    def __int__(self, lp_type="lpmln"):
        if lp_type == "asp":
            self.lp_se = ASPSEChecking
            raise RuntimeError("not support for now")
        elif lp_type == "lpmln":
            self.lp_se = LPMLNSEChecking
        else:
            raise RuntimeError("unknown SE checking logic: %s" % lp_type)

    def validate_isets_kmn_program(self, isets, k_size, m_size, n_size):
        kmn = isu.construct_kmn_program_from_isets(isets, k_size, m_size, n_size)
        is_contain_valid_rule = False
        is_se_sat = False
        for prg in kmn:
            if self.lp_se.is_contain_se_valid_rule(prg):
                is_contain_valid_rule = True
                break

        if is_contain_valid_rule:
            return is_contain_valid_rule, is_se_sat

        is_se_sat = self.lp_se.se_check_kmn_program(*kmn)
        return is_contain_valid_rule, is_se_sat

    def validate_isets_kmn_program_from_non_empty_ids(self, non_empty_ids, k_size, m_size, n_size):
        iset_number = isu.compute_iset_number_from_kmn(k_size, m_size, n_size)
        isets = isu.construct_isets_from_non_empty_iset_ids(non_empty_ids, iset_number=iset_number, iset_atom_number=1)
        return self.validate_isets_kmn_program(isets, k_size, m_size, n_size)

    def validate_isets_kmn_program_from_iset_condition(self, icondition, k_size, m_size, n_size):
        isets = isu.construct_isets_from_iset_condition(icondition, iset_atom_number=1)
        return self.validate_isets_kmn_program(isets, k_size, m_size, n_size)

    def validate_isets_kmn_program_from_icondition_id(self, icondition_id, k_size, m_size, n_size):
        iset_number = isu.compute_iset_number_from_kmn(k_size, m_size, n_size)
        isets = isu.construct_isets_from_icondition_id(icondition_id, iset_number)
        return self.validate_isets_kmn_program(isets, k_size, m_size, n_size)

    def validate_kmn_extended_iset_condition(self, icondition, k_size, m_size, n_size):
        isets = isu.construct_isets_from_iset_condition(icondition)
        singleton_iset_ids = list()
        is_contain_valid, is_se_sat = self.validate_isets_kmn_program(isets, k_size, m_size, n_size)
        if is_contain_valid or not is_se_sat:
            return is_contain_valid, is_se_sat, singleton_iset_ids

        non_empty_ids = list()
        for key in isets:
            if len(isets[key].members) != 0:
                non_empty_ids.append(key)
        new_atom = 0

        for nid in non_empty_ids:
            extended_isets = copy.deepcopy(isets)
            extended_isets[nid].members.add(new_atom)
            is_contain_valid, is_se_sat = self.validate_isets_kmn_program(extended_isets, k_size, m_size, n_size)
            if not is_se_sat:
                singleton_iset_ids.append(nid)

        return is_contain_valid, is_se_sat, singleton_iset_ids

    def validate_extended_iset_condition_from_non_emtpy_iset_ids(self, non_emtpy_iset_ids, k_size, m_size, n_size):
        iset_number = isu.compute_iset_number_from_kmn(k_size, m_size, n_size)
        icondition = isu.construct_iset_condition_from_non_emtpy_iset_ids(non_emtpy_iset_ids, iset_number)
        return self.validate_kmn_extended_iset_condition(icondition, k_size, m_size, n_size)


if __name__ == '__main__':
    pass
    