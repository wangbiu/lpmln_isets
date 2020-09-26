
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-26 20:19
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : FinalISearchHTWorkerSkipHTChecking.py
"""

import logging
from lpmln.iset.ISetCondition import ISetCondition
from lpmln.iset.ISetConditionValidator import ISetConditionValidator
import lpmln.config.GlobalConfig as cfg
import itertools
from lpmln.search.distributed.final.FinalSearchHTWorker import FinalIConditionsSearchHTWorker

config = cfg.load_configuration()


class FinalIConditionsSearchHTWorkerSkipHT(FinalIConditionsSearchHTWorker):
    @staticmethod
    def search_ht_task_slice(cls, itask, task_slice):
        task_check_number = 0
        is_check_valid_rules = False
        left_iset_ids = list(task_slice[0])

        se_conditions_cache = list()
        nse_conditions_cache = list()
        validator = ISetConditionValidator(lp_type=itask.lp_type, is_use_extended_rules=itask.is_use_extended_rules)

        task_iter = itertools.combinations(task_slice[1], task_slice[2])
        ne_iset_number = len(left_iset_ids) + task_slice[2]

        for right_ti in task_iter:
            non_ne_ids = list()
            non_ne_ids.extend(left_iset_ids)
            non_ne_ids.extend(list(right_ti))
            non_ne_ids = set(non_ne_ids)

            task_check_number += 1

            if len(non_ne_ids) > itask.rule_number:
                is_strongly_equivalent = True
                condition = ISetCondition(set(), list())
                condition.set_ne_iset_ids(non_ne_ids)
            else:
                is_contain_valid_rule, is_strongly_equivalent, condition = \
                    validator.validate_kmn_extended_iset_condition_from_non_emtpy_iset_ids_return_icondition_obj(
                        non_ne_ids, *itask.k_m_n, is_check_valid_rule=is_check_valid_rules)


            if is_strongly_equivalent:
                se_conditions_cache.append(condition)
            else:
                nse_conditions_cache.append(condition)
                if ne_iset_number > itask.rule_number:
                    logging.error(("wrong nse condition ", itask.k_m_n, non_ne_ids))

        return ne_iset_number, task_check_number, se_conditions_cache, nse_conditions_cache


if __name__ == '__main__':
    pass
