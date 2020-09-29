
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-29 16:25
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : HTCheckingWorkerDirectData.py
"""

from lpmln.search.distributed.ht.HTCheckingWorker import HTCheckingWorker
import linecache
from lpmln.iset.ISetConditionValidator import ISetConditionValidator
import lpmln.config.GlobalConfig as cfg
config = cfg.load_configuration()


class HTCheckingDirectWorker(HTCheckingWorker):
    @staticmethod
    def verify_ht_tasks_from_file_data(cls, itask, task_slice, data_file):
        se_conditions_cache = list()
        nse_conditions_cache = list()
        validator = ISetConditionValidator(lp_type=itask.lp_type, is_use_extended_rules=itask.is_use_extended_rules)
        task_check_number = len(task_slice)

        for data in task_slice:
            ne_isets = data.strip("\r\n ").split(",")
            ne_isets = [int(s) for s in ne_isets]
            ne_isets = set(ne_isets)

            is_contain_valid_rule, is_strongly_equivalent, condition = \
                validator.validate_kmn_extended_iset_condition_from_non_emtpy_iset_ids_return_icondition_obj(
                    ne_isets, *itask.k_m_n, is_check_valid_rule=False)

            if is_strongly_equivalent:
                se_conditions_cache.append(condition)
            else:
                nse_conditions_cache.append(condition)

        # print(task_slice, "complete", "task check number", task_check_number)
        return task_check_number, se_conditions_cache, nse_conditions_cache


if __name__ == '__main__':
    pass
    