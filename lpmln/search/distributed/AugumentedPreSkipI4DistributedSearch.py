# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-14 14:32
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : AugumentedPreSkipI4DistributedSearch.py
"""

# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-13 23:22
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : PreSkipI4DistributedSearch.py
"""

from lpmln.search.distributed.PreSkipI4DistributedSearch import PreSkipI4DistributedSearchMaster, PreSkipI4DistributedSearchWorker
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


class AugumentedPreSkipI4DistributedSearchMaster(PreSkipI4DistributedSearchMaster):

    @staticmethod
    def itask_slices_second_phase_generator(cls, task_id, ne_iset_number, total_zone_length, all_left_zone_isets, first_phase_isets, phase2_left_zone_isets, task_queue):
        phase2_ne_iset_number = ne_iset_number - len(first_phase_isets)
        phase2_left_zone_length = len(phase2_left_zone_isets)
        phase2_right_zone_length = total_zone_length - len(all_left_zone_isets)

        for phase2_left_iset_number in range(phase2_ne_iset_number + 1):
            phase2_right_iset_number = phase2_ne_iset_number - phase2_left_iset_number
            if phase2_left_iset_number > phase2_left_zone_length or phase2_right_iset_number > phase2_right_zone_length:
                continue

            task_iter = itertools.combinations(phase2_left_zone_isets, phase2_left_iset_number)
            for left_ti in task_iter:
                all_left_isets = first_phase_isets + list(left_ti)
                task_item = (task_id, (ne_iset_number, all_left_zone_isets, all_left_isets))
                task_queue.put(task_item)

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
            search_isets = set(it.meta_data.search_space_iset_ids)
            search_isets_number = len(search_isets)
            rule_number = it.rule_number

            fixed_left_zone_length = int(search_isets_number / 2)
            if fixed_left_zone_length > 12:
                fixed_left_zone_length = 12

            left_zone_iset_ids = set(it.meta_data.search_i4_composed_iset_ids)

            left_zone_length = len(left_zone_iset_ids)
            right_zone_length = search_isets_number - left_zone_length
            is_use_extended_rules = it.is_use_extended_rules

            second_phase_flag = False
            second_phase_zone_isets = None
            second_phase_zone_length = 0
            all_left_zone_isets = None
            if fixed_left_zone_length > left_zone_length:
                second_phase_flag = True
                second_phase_zone_length = fixed_left_zone_length - left_zone_length
                right_zone_isets = list(search_isets.difference(left_zone_iset_ids))
                second_phase_zone_isets = right_zone_isets[0:second_phase_zone_length]
                all_left_zone_isets = second_phase_zone_isets + list(left_zone_iset_ids)
                all_left_zone_isets = set(all_left_zone_isets)


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
                            stat_item = (
                            ITaskSignal.stat_signal, tid, ne_iset_number, check_cnt, task_number, semi_valid_skip_cnt,
                            None)
                            result_queue.put(stat_item)
                        else:
                            if not second_phase_flag:
                                task_item = (tid, (ne_iset_number, left_zone_iset_ids, left_iset_ids))
                                # print(task_item)
                                task_queue.put(task_item)
                            else:
                                cls.itask_slices_second_phase_generator(cls, tid, ne_iset_number, search_isets_number, all_left_zone_isets, left_iset_ids, second_phase_zone_isets, task_queue)

        working_hosts_number = 5
        for i in range(working_hosts_number * 200):
            task_queue.put((ITaskSignal.kill_signal, -1))
        logging.info("all itasks has been dispatched")


class AugumentedPreSkipI4DistributedSearchWorker(PreSkipI4DistributedSearchWorker):
    pass


def init_task_master(isc_config_file="isets-tasks.json", sleep_time=30):
    AugumentedPreSkipI4DistributedSearchMaster.init_kmn_isc_task_master_from_config(AugumentedPreSkipI4DistributedSearchMaster,
                                                                          isc_config_file, sleep_time)


def init_task_worker(isc_config_file="isets-tasks.json", is_check_valid_rules=True):
    AugumentedPreSkipI4DistributedSearchWorker.init_kmn_isc_task_workers(AugumentedPreSkipI4DistributedSearchWorker, isc_config_file,
                                                               is_check_valid_rules)


if __name__ == '__main__':
    init_task_worker()
    # init_task_master(sleep_time=2)
    pass
