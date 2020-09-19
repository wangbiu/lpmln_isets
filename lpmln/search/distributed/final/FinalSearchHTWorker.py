
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-19 14:45
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : FinalSearchHTWorker.py
"""

import logging
from datetime import datetime
import time
import pathlib

from lpmln.iset.ISetConditionValidator import ISetConditionValidator
import lpmln.config.GlobalConfig as cfg
from lpmln.itask.ITask import ITaskConfig
import itertools
from lpmln.search.distributed.final.FinalSearchBase import FinalIConditionsSearchBaseWorker, ITaskSignal, SearchQueueManager
config = cfg.load_configuration()


class FinalIConditionsSearchHTWorker(FinalIConditionsSearchBaseWorker):

    @staticmethod
    def get_and_process_ht_task_queue(cls, itasks, worker_info, manager_tuple):
        ht_task_queue = manager_tuple[2]
        result_queue = manager_tuple[3]
        processed_ht_task_slices_number = worker_info[3]
        is_process_ht_task_queue = False

        while True:
            if ht_task_queue.empty():
                break

            is_process_ht_task_queue = True
            ts = ht_task_queue.get()
            start_time = datetime.now()
            itask_id = ts[0]
            itask = itasks[itask_id]
            processed_ht_task_slices_number += 1
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

        return is_process_ht_task_queue, processed_ht_task_slices_number

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

        worker_name = "worker-%d" % worker_id
        worker_host_name = config.worker_host_name
        processed_task_slices_number = 0
        processed_ht_task_slices_number = 0
        worker_info = [worker_host_name, worker_name, processed_task_slices_number, processed_ht_task_slices_number]

        msg_text = "task worker %s start!" % (worker_name)
        logging.info(msg_text)
        isc_tasks = ITaskConfig(isc_config_file)
        isc_tasks = isc_tasks.isc_tasks

        for itask in isc_tasks:
            # itask.loaded_non_se_condition_files.add(1)
            itask.loaded_non_se_condition_files.add(0)

        task_slice_cache = None
        is_task_queue_finish = False
        is_process_task_queue = False
        is_process_ht_task_queue = False
        while True:
            if not pathlib.Path(config.task_host_lock_file).exists():
                break

            if not is_task_queue_finish:
                is_task_queue_finish, is_process_task_queue, processed_task_slices_number, task_slice_cache = \
                    cls.get_and_process_task_queue(cls, isc_tasks, task_slice_cache, worker_info, manager_tuple)

                if is_task_queue_finish:
                    msg_text = "%s:%s isc task queue finish, process %d task slices %d ht task slices ..." % (
                        worker_info[0], worker_info[1], worker_info[2], worker_info[3])
                    logging.info(msg_text)
                    is_process_task_queue = False
                else:
                    if is_process_task_queue and task_slice_cache is not None:
                        ne_iset_number = len(task_slice_cache[1][0]) + task_slice_cache[1][2] - 1
                        msg_text = "%s waiting for nse condition complete file: %d" % (worker_name, ne_iset_number)
                        logging.info((task_slice_cache, msg_text))

            is_process_ht_task_queue, processed_ht_task_slices_number = \
                cls.get_and_process_ht_task_queue(cls, isc_tasks, worker_info, manager_tuple)

            worker_info[2] = processed_task_slices_number
            worker_info[3] = processed_ht_task_slices_number

            if is_process_task_queue or is_process_ht_task_queue:
                msg_text = "%s:%s isc task worker process %d task slices %d ht task slices ..." % (
                    worker_info[0], worker_info[1], worker_info[2], worker_info[3])
                logging.info(msg_text)
                logging.info("%s waiting for task queue and ht task queue ..." % worker_name)

            is_task_finish = cls.check_itasks_finish_status(isc_tasks)
            if is_task_finish:
                break
            time.sleep(1)

        logging.info("%s processes %d isc task slices %d ht itask slices" % (worker_name, worker_info[2], worker_info[3]))



if __name__ == '__main__':
    pass
    