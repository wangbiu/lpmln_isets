
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-10-06 16:14
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ParallelISetConditionValidator.py
"""

import lpmln.iset.ISetUtils as isu
from multiprocessing import Pool
from lpmln.sat.LPMLNSEChecking import LPMLNSEChecking
from lpmln.sat.ASPSEChecking import ASPSEChecking
import copy
from lpmln.iset.ISetCondition import ISetCondition
from lpmln.iset.ISetConditionValidator import ISetConditionValidator
import lpmln.config.GlobalConfig as cfg
config = cfg.load_configuration()


class ParallelIConditionValidator(ISetConditionValidator):
    def get_validate_kmn_extended_isets_from_non_emtpy_iset_ids(self, non_emtpy_iset_ids, k_size, m_size, n_size):
        iset_number = isu.compute_iset_number_from_kmn(k_size, m_size, n_size, self.rule_set_size)
        icondition = isu.construct_iset_condition_from_non_emtpy_iset_ids(non_emtpy_iset_ids, iset_number)
        isets = isu.construct_isets_from_iset_condition(icondition, self.is_use_extended_rules)

        all_kmn_isets = list()
        all_kmn_isets.append((-1, isets))
        for ne in non_emtpy_iset_ids:
            all_kmn_isets.append((ne, copy.deepcopy(isets)))

        return all_kmn_isets

    def validate(self, isets, k_size,  m_size, n_size, is_check_valid_rules):
        ne = isets[0]
        if ne != -1:
            isets[1][ne + 1].members.add(-1)

        is_contain_valid_rule, is_se_sat = self.validate_isets_kmn_program(
            isets[1], k_size, m_size, n_size, is_check_valid_rules)

        print("ne iset %d, is se %s" % (ne, str(is_se_sat)))
        # print(isets[1][ne + 1], isets[1][ne])

    def parallel_validate(self, non_emtpy_iset_ids, k_size, m_size, n_size, is_check_valid_rules=False):
        all_kmn_isets = self.get_validate_kmn_extended_isets_from_non_emtpy_iset_ids(
            non_emtpy_iset_ids, k_size, m_size, n_size)
        ht_check_pool = Pool(config.worker_payload)
        print("ne isets ", non_emtpy_iset_ids)
        for isets in all_kmn_isets:
            ht_check_pool.apply_async(self.validate, args=(isets, k_size, m_size, n_size, is_check_valid_rules))

        ht_check_pool.close()
        ht_check_pool.join()


if __name__ == '__main__':
    validator = ParallelIConditionValidator(False, "lpmln")
    ne_isets = {256,291,17,145,159}
    validator.parallel_validate(ne_isets, 2, 1, 0, False)
    pass
    