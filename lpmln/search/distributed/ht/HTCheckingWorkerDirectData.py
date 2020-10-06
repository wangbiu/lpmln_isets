
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-29 16:25
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : HTCheckingWorkerDirectData.py
"""

import logging
from datetime import datetime
import time
import pathlib
from lpmln.iset.ISetConditionValidator import ISetConditionValidator
import lpmln.config.GlobalConfig as cfg
from lpmln.itask.ITask import ITaskConfig
import lpmln.iset.RawIConditionUtils as riu
from lpmln.search.distributed.final.FinalSearchBase import ITaskSignal, SearchQueueManager
from lpmln.search.distributed.ht.HTCheckingWorker import HTCheckingWorker
config = cfg.load_configuration()


class HTCheckingDirectWorker(HTCheckingWorker):
    @staticmethod
    def verify_ht_tasks_from_file_data(cls, itask, task_slice, data_file):
        se_conditions_cache = list()
        nse_conditions_cache = list()
        validator = ISetConditionValidator(lp_type=itask.lp_type, is_use_extended_rules=itask.is_use_extended_rules)
        task_check_number = len(task_slice)

        for data_item in task_slice:
            data_id = data_item[0]
            ne_isets = data_item[1].strip("\r\n ").split(",")
            ne_isets = [int(s) for s in ne_isets]
            ne_isets = set(ne_isets)

            is_contain_valid_rule, is_strongly_equivalent, condition = \
                validator.validate_kmn_extended_iset_condition_from_non_emtpy_iset_ids_return_icondition_obj(
                    ne_isets, *itask.k_m_n, is_check_valid_rule=False)

            if is_strongly_equivalent:
                se_cnt = [data_id]
                se_cnt.extend(condition.singletom_iset_ids)
                se_cnt = [str(d) for d in se_cnt]
                se_conditions_cache.append(",".join(se_cnt))
            else:
                nse_conditions_cache.append(str(data_id))

        # print(task_slice, "complete", "task check number", task_check_number)
        return task_check_number, se_conditions_cache, nse_conditions_cache


if __name__ == '__main__':
    pass
    