
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-16 19:51
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : NoWaitNSEPreSkipI4DSearch.py
"""

from lpmln.search.distributed.PreSkipI4DistributedSearch import PreSkipI4DistributedSearchWorker, PreSkipI4DistributedSearchMaster
from lpmln.search.distributed.AugumentedPreSkipI4DistributedSearch import AugumentedPreSkipI4DistributedSearchMaster, AugumentedPreSkipI4DistributedSearchWorker
from lpmln.search.distributed.DistributedSearch import SearchWorkerQueueManger, SearchMasterQueueManger, ITaskSignal
import logging
import lpmln.config.GlobalConfig as cfg


import lpmln.iset.ISetNonSEUtils as isnse
from datetime import datetime
import pathlib

import lpmln.utils.SSHClient as ssh
import itertools

config = cfg.load_configuration()


class NoWaitNSEPreSkipI4DSearchMaster(PreSkipI4DistributedSearchMaster):
    @staticmethod
    def check_itasks_status(cls, itasks, host_ips, task_queue, working_host_number):
        is_finish = True
        for tid in range(len(itasks)):
            it = itasks[tid]
            rule_number = it.rule_number
            if not it.is_task_finish:
                current_ne_number = it.working_ne_iset_numbers
                task_complete = it.hierarchical_task_complete_number[current_ne_number]
                task_total = it.hierarchical_task_number[current_ne_number]
                if task_complete == task_total:

                    if current_ne_number <= rule_number:
                        nse_file = it.flush_non_se_condition()
                        isnse.transport_non_se_results([nse_file], host_ips)
                        isnse.create_and_send_transport_complete_flag_file(*it.k_m_n, current_ne_number, host_ips)

                        cls.send_itasks_progress_info(cls, itasks, task_queue, working_host_number, False)

                    it.save_progress_info()
                    if it.is_early_terminate():
                        isnse.create_and_send_task_early_terminate_flag_file(*it.k_m_n, current_ne_number, host_ips)
                        it.save_progress_info()
                        continue

                    if current_ne_number < it.max_ne:
                        it.working_ne_iset_numbers += 1
                        is_finish = False
                    else:
                        it.is_task_finish = True
                        it.save_progress_info()
                else:
                    is_finish = False
        return is_finish


class NoWaitNSEPreSkipI4DSearchWorker(PreSkipI4DistributedSearchWorker):
    @staticmethod
    def process_kmn_itask_slice(cls, itask, task_slice, task_name, result_queue, is_check_valid_rules):
        time_fmt = "%Y-%m-%d %H:%M:%S.%f"

        itask_id = task_slice[0]
        task_params = task_slice[1]

        ne_iset_number = task_params[0]
        left_zone_isets = task_params[1]
        left_iset_ids = task_params[2]

        task_terminate_flag = isnse.get_task_early_terminate_flag_file(*itask.k_m_n)
        nse_iset_number = ne_iset_number - 1

        if nse_iset_number <= itask.rule_number:
            if nse_iset_number not in itask.loaded_non_se_condition_files:
                load_complete = False
                while not load_complete:
                    if pathlib.Path(task_terminate_flag).exists():
                        itask.is_task_finish = True
                        break
                    load_complete = cls.task_worker_load_nse_conditions(itask, nse_iset_number)

        if itask.is_task_finish:
            return True

        start_time = datetime.now()
        start_time_str = start_time.strftime(time_fmt)[:-3]

        right_zone_isets = set(itask.meta_data.search_space_iset_ids)
        right_zone_isets = right_zone_isets.difference(left_zone_isets)

        right_iset_number = ne_iset_number - len(left_iset_ids)

        msg_text = "%s: %d-%d-%d isc task: nonempty iset number %d, left zone length %d, left isets {%s}" % (
            task_name, *itask.k_m_n, ne_iset_number, len(left_zone_isets), cls.join_list_data(left_iset_ids))
        logging.info(msg_text)

        nse_cdt_cnt, check_cdt_cnt, task_number, semi_valid_skip_cnt, se_conditions_cache, nse_conditions_cache = \
            cls.search_task_slice(cls, itask, left_iset_ids, right_zone_isets, right_iset_number, is_check_valid_rules)

        # for sec in se_conditions_cache:
        if len(se_conditions_cache) > 0:
            result_queue.put((ITaskSignal.se_condition_signal, itask_id, se_conditions_cache))

        if len(nse_conditions_cache) > 0:
            result_queue.put((ITaskSignal.nse_condition_signal, itask_id, nse_conditions_cache))

        end_time = datetime.now()
        end_time_str = end_time.strftime(time_fmt)[:-3]

        msg_text = "%s: %d-%d-%d isc task: nonempty iset number %d, left zone length %d, left isets {%s}, start time %s, end time %s, find %d se conditions (no semi-valid rules), find %d non-se conditions" % (
            task_name, *itask.k_m_n, ne_iset_number, len(left_zone_isets), cls.join_list_data(left_iset_ids),
            start_time_str, end_time_str, len(se_conditions_cache), nse_cdt_cnt)

        logging.info(msg_text)
        result_queue.put(
            (ITaskSignal.stat_signal, itask_id, ne_iset_number, check_cdt_cnt, task_number, semi_valid_skip_cnt,
             (start_time, end_time)))

        return True



def init_task_master(isc_config_file="isets-tasks.json", sleep_time=30):
    NoWaitNSEPreSkipI4DSearchMaster.init_kmn_isc_task_master_from_config(NoWaitNSEPreSkipI4DSearchMaster, isc_config_file, sleep_time)


def init_task_worker(isc_config_file="isets-tasks.json", is_check_valid_rules=True):
    NoWaitNSEPreSkipI4DSearchWorker.init_kmn_isc_task_workers(NoWaitNSEPreSkipI4DSearchWorker, isc_config_file, is_check_valid_rules)




if __name__ == '__main__':
    pass
    