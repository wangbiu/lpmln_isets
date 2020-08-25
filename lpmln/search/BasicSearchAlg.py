# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/5 15:49
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : BasicSearchAlg.py
"""

import lpmln.iset.ISetUtils as isu
from lpmln.iset.ISetConditionValidator import ISetConditionValidator
import lpmln.config.GlobalConfig as cfg
config = cfg.load_configuration()
import logging


def search(k_size, m_size, n_size, is_use_extended_rules, is_check_valid_rule=False, lp_type="lpmln"):
    logging.info("basic isp searching start!")
    validator = ISetConditionValidator(is_use_extended_rules=is_use_extended_rules, lp_type=lp_type)
    rule_set_number = 3
    if is_use_extended_rules:
        rule_set_number = 4

    iset_number = isu.compute_iset_number_from_kmn(k_size, m_size, n_size, rule_set_number)
    searching_space_size = 2 ** iset_number
    se_conditions = list()
    non_se_conditions = list()
    print_loop = 1000
    search_cnt = 0
    for sid in range(searching_space_size):
        # is_contain_valid, is_se_sat, condition = validator.validate_isets_kmn_program_from_icondition_id(
        #     sid, k_size, m_size, n_size, is_check_valid_rule=is_check_valid_rule)
        icondition = isu.construct_iset_condition_from_icondition_id(sid, iset_number)
        is_contain_valid, is_se_sat, condition = validator.validate_kmn_extended_iset_condition(
            icondition, k_size, m_size, n_size, is_check_valid_rule)

        if not is_contain_valid:
            if is_se_sat:
                se_conditions.append(condition)
            else:
                non_se_conditions.append(condition)
        search_cnt += 1
        if search_cnt % print_loop == 0:
            logging.info("search progress: %d / %d" % (search_cnt, searching_space_size))

    logging.info("search progress: %d / %d" % (search_cnt, searching_space_size))

    print("%d-%d-%d problem: found %d SE-condition, found %d non-SE-condition" %
          (k_size, m_size, n_size, len(se_conditions), len(non_se_conditions)))

    print("\nSE-conditions (%d): " % len(se_conditions))
    for c in se_conditions:
        print("\t", c)

    print("\nnon-SE-conditions (%d): " % len(non_se_conditions))
    for c in non_se_conditions:
        print("\t", c)
    return se_conditions, non_se_conditions


if __name__ == '__main__':
    search(0, 1, 0, is_use_extended_rules=False, lp_type="asp")
    pass
