
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-16 20:02
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : EarlyTerminationPreSkipI4DSearch.py
"""

from lpmln.search.distributed.PreSkipI4DistributedSearch import PreSkipI4DistributedSearchWorker, PreSkipI4DistributedSearchMaster
import logging
import lpmln.config.GlobalConfig as cfg
from lpmln.itask.ITask import ITaskConfig
import itertools
import lpmln.iset.ISetCompositionUtils as iscm
from lpmln.utils.counter.CombinaryCounter import CombinaryCounter
from lpmln.iset.ISetConditionValidator import ISetConditionValidator
import lpmln.message.Messager as msg

import lpmln.iset.ISetNonSEUtils as isnse
from datetime import datetime
import pathlib

config = cfg.load_configuration()


class EarlyTerminationPreSkipI4DSearchMaster(PreSkipI4DistributedSearchMaster):

    @staticmethod
    def check_itasks_status(cls, itasks, host_ips, task_queue, working_host_number):
        is_finish = True
        for tid in range(len(itasks)):
            it = itasks[tid]
            if not it.is_task_finish:

                if it.is_no_new_se_condition():
                    isnse.create_and_send_task_early_terminate_flag_file(*it.k_m_n, host_ips)
                    it.is_task_finish = True
                    continue

                current_ne_number = it.working_ne_iset_numbers
                task_complete = it.hierarchical_task_complete_number[current_ne_number]
                task_total = it.hierarchical_task_number[current_ne_number]
                if task_complete == task_total:
                    it.save_progress_info()
                    nse_file = it.flush_non_se_condition()
                    isnse.transport_non_se_results([nse_file], host_ips)
                    isnse.create_and_send_transport_complete_flag_file(*it.k_m_n, current_ne_number, host_ips)
                    cls.send_itasks_progress_info(cls, itasks, task_queue, working_host_number, False)

                    if current_ne_number < it.max_ne:
                        it.working_ne_iset_numbers += 1
                        is_finish = False
                    else:
                        it.is_task_finish = True
                else:
                    is_finish = False
        return is_finish


class EarlyTerminationPreSkipI4DSearchWorker(PreSkipI4DistributedSearchWorker):
    pass



def init_task_master(isc_config_file="isets-tasks.json", sleep_time=30):
    EarlyTerminationPreSkipI4DSearchMaster.init_kmn_isc_task_master_from_config(EarlyTerminationPreSkipI4DSearchMaster, isc_config_file, sleep_time)


def init_task_worker(isc_config_file="isets-tasks.json", is_check_valid_rules=True):
    EarlyTerminationPreSkipI4DSearchWorker.init_kmn_isc_task_workers(EarlyTerminationPreSkipI4DSearchWorker, isc_config_file, is_check_valid_rules)



if __name__ == '__main__':
    pass
    