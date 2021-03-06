
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-19 14:45
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : FinalSearchHTWorker.py
"""

import logging
from multiprocessing import Process, Manager
from datetime import datetime
import time
import pathlib
from lpmln.iset.ISetCondition import ISetCondition
from lpmln.iset.ISetConditionValidator import ISetConditionValidator
import lpmln.iset.ISetUtils as isu
import lpmln.config.GlobalConfig as cfg
from lpmln.itask.ITask import ITaskConfig
import itertools
import copy
from lpmln.search.distributed.final.FinalSearchBase import FinalIConditionsSearchBaseWorker, ITaskSignal, SearchQueueManager
config = cfg.load_configuration()


class FinalIConditionsSearchHTWorker(FinalIConditionsSearchBaseWorker):

    @staticmethod
    def parallel_validate_kmn_extended_icondition(cls, ne_isets, itask, ht_validator):
        print("enter parallel validate ")
        is_use_extended_rules = itask.is_use_extended_rules
        icondition = isu.construct_iset_condition_from_non_emtpy_iset_ids(ne_isets, itask.iset_number)
        isets = isu.construct_isets_from_iset_condition(icondition, is_use_extended_rules)
        is_check_valid_rule = False
        singleton_iset_ids = list()
        validated_condition = ISetCondition(icondition, singleton_iset_ids)
        is_contain_valid, is_se_sat = ht_validator.validate_isets_kmn_program(isets, *itask.k_m_n, is_check_valid_rule)

        if not is_se_sat:
            return is_se_sat, validated_condition

        k_size = itask.k_m_n[0]
        m_size = itask.k_m_n[1]
        n_size = itask.k_m_n[2]
        ht_jobs = list()

        # manager = Manager()
        # singleton_results = manager.list()

        for sid in ne_isets:
            extended_isets = copy.deepcopy(isets)
            p = Process(target=cls.validate_extended_icondition,
                        args=(k_size, m_size, n_size, itask.lp_type, itask.is_use_extended_rules,
                                             extended_isets, sid + 1)) #, singleton_results))
            ht_jobs.append(p)
            p.start()

        for p in ht_jobs:
            p.join()

        # singleton_iset_ids.extend(singleton_results)
        return is_se_sat, validated_condition

    @staticmethod
    def validate_extended_icondition(k_size, m_size, n_size, lp_type, is_use_extended_rules, extended_isets, singleton_id):
        new_atom = -1
        ht_validator = ISetConditionValidator(lp_type=lp_type, is_use_extended_rules=is_use_extended_rules)
        extended_isets[singleton_id].members.add(new_atom)
        is_contain_valid, is_se_sat = ht_validator.validate_isets_kmn_program(
            extended_isets, k_size, m_size, n_size, False)
        print("validate ", singleton_id)
        # if not is_se_sat:
        #     results.append(singleton_id)

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

            is_contain_valid_rule, is_strongly_equivalent, condition = \
                validator.validate_kmn_extended_iset_condition_from_non_emtpy_iset_ids_return_icondition_obj(
                    non_ne_ids, *itask.k_m_n, is_check_valid_rule=is_check_valid_rules)

            # if ne_iset_number < 3:
            #     is_contain_valid_rule, is_strongly_equivalent, condition = \
            #         validator.validate_kmn_extended_iset_condition_from_non_emtpy_iset_ids_return_icondition_obj(
            #             non_ne_ids, *itask.k_m_n, is_check_valid_rule=is_check_valid_rules)
            # else:
            #     is_strongly_equivalent, condition = \
            #         cls.parallel_validate_kmn_extended_icondition(cls, non_ne_ids, itask, validator)

            if is_strongly_equivalent:
                se_conditions_cache.append(condition)
            else:
                nse_conditions_cache.append(condition)
                if ne_iset_number > itask.rule_number:
                    logging.error(("wrong nse condition ", itask.k_m_n, non_ne_ids))

        return ne_iset_number, task_check_number, se_conditions_cache, nse_conditions_cache

    @staticmethod
    def kmn_isc_task_worker(cls, isc_config_file="isets-tasks.json", worker_id=1, is_check_valid_rules=True):
        manager_tuple = SearchQueueManager.init_task_worker_queue_manager()
        # manager_tuple = (manager, task_queue, ht_task_queue, result_queue)
        ht_task_queue = manager_tuple[2]
        result_queue = manager_tuple[3]

        worker_name = "worker-%d" % worker_id
        worker_host_name = config.worker_host_name
        processed_ht_task_slices_number = 0

        msg_text = "task worker %s start!" % (worker_name)
        logging.info(msg_text)
        isc_tasks = ITaskConfig(isc_config_file)
        isc_tasks = isc_tasks.isc_tasks

        is_process_ht_task_queue = False

        while True:
            if not pathlib.Path(config.task_host_lock_file).exists():
                break

            is_task_finish = cls.check_itasks_finish_status(isc_tasks)
            if is_task_finish:
                logging.info("%s:%s all itasks terminate ..." % (worker_host_name, worker_name))
                break

            if ht_task_queue.empty():
                if is_process_ht_task_queue:
                    is_process_ht_task_queue = False
                    logging.info("%s:%s waiting for ht task queue ..." % (worker_host_name, worker_name))
                time.sleep(1)
                continue

            is_process_ht_task_queue = True
            processed_ht_task_slices_number += 1

            ts = ht_task_queue.get()
            start_time = datetime.now()
            itask_id = ts[0]
            itask = isc_tasks[itask_id]

            ne_iset_number, task_check_number, se_conditions_cache, nse_conditions_cache = \
                cls.search_ht_task_slice(cls, itask, ts[1])
            end_time = datetime.now()

            if ne_iset_number > itask.rule_number and len(nse_conditions_cache) > 0:
                print("debug info (wrong nse conditions): ", ts)

            if len(se_conditions_cache) > 0:
                result_queue.put((ITaskSignal.se_condition_signal, itask_id, se_conditions_cache))

            if len(nse_conditions_cache) > 0:
                result_queue.put((ITaskSignal.nse_condition_signal, itask_id, nse_conditions_cache))

            result_tuple = (ITaskSignal.stat_signal, itask_id, ne_iset_number, task_check_number,
                            task_check_number, 0, (start_time, end_time))
            result_queue.put(result_tuple)

        logging.info("%s processes  %d ht itask slices" % (worker_name, processed_ht_task_slices_number))

    @staticmethod
    def init_kmn_isc_task_workers(cls, isc_config_file="isets-tasks.json", is_check_valid_rules=True, result_queue=None):
        worker_pool, result_queue, host_ip = super().init_kmn_isc_task_workers(cls, isc_config_file, is_check_valid_rules, result_queue)
        worker_pool.join()
        cls.send_worker_terminate_info(cls, host_ip, result_queue)

    @staticmethod
    def inner_init_kmn_isc_task_workers(cls,  isc_config_file="isets-tasks.json", is_check_valid_rules=True, result_queue=None):
        worker_pool, result_queue, host_ip = super().init_kmn_isc_task_workers(cls, isc_config_file,
                                                                               is_check_valid_rules, result_queue)
        return worker_pool, result_queue, host_ip


if __name__ == '__main__':
    pass
    