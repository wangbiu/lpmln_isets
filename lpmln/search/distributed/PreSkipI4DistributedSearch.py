
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-13 23:22
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : PreSkipI4DistributedSearch.py
"""

from lpmln.search.distributed.DistributedSearch import DistributedSearchIConditionsMaster, DistributedSearchIConditionsWorker
from lpmln.search.distributed.DistributedSearch import SearchWorkerQueueManger, SearchMasterQueueManger, ITaskSignal
import logging
import lpmln.config.GlobalConfig as cfg
from lpmln.itask.ITask import ITaskConfig
import itertools
import lpmln.iset.ISetCompositionUtils as iscm
from lpmln.utils.counter.CombinaryCounter import CombinaryCounter
from lpmln.iset.ISetConditionValidator import ISetConditionValidator
import lpmln.message.Messager as msg

config = cfg.load_configuration()


class PreSkipI4DistributedSearchMaster(DistributedSearchIConditionsMaster):

    @staticmethod
    def itask_slices_generator(cls, isc_config_file="isets-tasks.json"):
        msg_text = "%s init task slices generator ..." % str(cls)
        logging.info(msg_text)
        msg.send_message(msg_text)

        SearchWorkerQueueManger.register("get_task_queue")
        SearchWorkerQueueManger.register("get_result_queue")
        manager = SearchWorkerQueueManger(address=(config.task_host, config.task_host_port),
                                          authkey=bytes(config.task_host_key, encoding="utf-8"))
        manager.connect()
        task_queue = manager.get_task_queue()
        result_queue = manager.get_result_queue()

        isc_tasks_cfg = ITaskConfig(isc_config_file)
        isc_tasks = isc_tasks_cfg.isc_tasks

        for tid in range(len(isc_tasks)):
            it = isc_tasks[tid]
            min_ne = it.min_ne
            max_ne = it.max_ne
            unknown_iset_number = len(it.meta_data.search_space_iset_ids)
            rule_number = it.rule_number

            left_zone_iset_ids = it.meta_data.search_i4_composed_iset_ids

            left_zone_length = len(left_zone_iset_ids)
            right_zone_length = unknown_iset_number - left_zone_length
            is_use_extended_rules = it.is_use_extended_rules

            for i in range(min_ne, max_ne + 1):
                ne_iset_number = i
                for left_iset_number in range(ne_iset_number + 1):
                    right_iset_number = ne_iset_number - left_iset_number
                    if left_iset_number > left_zone_length or right_iset_number > right_zone_length:
                        continue

                    task_iter = itertools.combinations(left_zone_iset_ids, left_iset_number)
                    for left_ti in task_iter:
                        left_iset_ids = list(left_ti)
                        is_contain_semi_valid_rule = iscm.check_contain_rules_without_i_n_iset(
                            4, left_iset_ids, rule_number, is_use_extended_rules)

                        if is_contain_semi_valid_rule:
                            check_cnt = 0
                            # C(right_zone_length, right_iset_number)
                            task_number = CombinaryCounter.compute_comb(right_zone_length, right_iset_number)
                            semi_valid_skip_cnt = task_number
                            stat_item = (ITaskSignal.stat_signal, tid, ne_iset_number, check_cnt, task_number, semi_valid_skip_cnt, None)
                            result_queue.put(stat_item)
                        else:
                            task_item = (tid, (ne_iset_number, set(left_zone_iset_ids), left_iset_ids))
                            # print(task_item)
                            task_queue.put(task_item)

        working_hosts_number = 5
        for i in range(working_hosts_number * 200):
            task_queue.put((ITaskSignal.kill_signal, -1))
        logging.info("all itasks has been dispatched")


class PreSkipI4DistributedSearchWorker(DistributedSearchIConditionsWorker):

    @staticmethod
    def search_task_slice(cls, itask, left_iset_ids, right_zone_isets, right_iset_number, is_check_valid_rules):
        semi_valid_skip_cnt = 0
        check_cdt_cnt = 0
        task_number = 0
        nse_cdt_cnt = 0

        se_conditions_cache = list()
        nse_conditions_cache = list()
        validator = ISetConditionValidator(lp_type=itask.lp_type, is_use_extended_rules=itask.is_use_extended_rules)

        task_iter = itertools.combinations(right_zone_isets, right_iset_number)

        for right_ti in task_iter:
            non_ne_ids = list()
            non_ne_ids.extend(left_iset_ids)
            non_ne_ids.extend(list(right_ti))
            non_ne_ids = set(non_ne_ids)
            task_number += 1

            if cls.check_contain_nse_subparts(non_ne_ids, itask):
                nse_cdt_cnt += 1
                continue

            check_cdt_cnt += 1
            is_contain_valid_rule, is_strongly_equivalent, condition = \
                validator.validate_kmn_extended_iset_condition_from_non_emtpy_iset_ids_return_icondition_obj(
                    non_ne_ids, *itask.k_m_n, is_check_valid_rule=is_check_valid_rules)

            # if not is_contain_valid_rule:
            if is_strongly_equivalent:
                se_conditions_cache.append(condition)
            else:
                nse_cdt_cnt += 1
                nse_conditions_cache.append(condition)

        return nse_cdt_cnt, check_cdt_cnt, task_number, semi_valid_skip_cnt, se_conditions_cache, nse_conditions_cache


def init_task_master(isc_config_file="isets-tasks.json", sleep_time=30):
    PreSkipI4DistributedSearchMaster.init_kmn_isc_task_master_from_config(PreSkipI4DistributedSearchMaster, isc_config_file, sleep_time)


def init_task_worker(isc_config_file="isets-tasks.json", is_check_valid_rules=True):
    PreSkipI4DistributedSearchWorker.init_kmn_isc_task_workers(PreSkipI4DistributedSearchWorker, isc_config_file, is_check_valid_rules)


if __name__ == '__main__':
    init_task_worker()
    # init_task_master(sleep_time=2)
    pass
    