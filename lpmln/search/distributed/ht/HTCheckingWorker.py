
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-27 9:49
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : HTCheckingWorker.py
"""

import logging
from multiprocessing import Process, Manager
import linecache
from datetime import datetime
import time
import pathlib
from lpmln.iset.ISetCondition import ISetCondition
from lpmln.iset.ISetConditionValidator import ISetConditionValidator
import lpmln.iset.ISetUtils as isu
import lpmln.config.GlobalConfig as cfg
from lpmln.itask.ITask import ITaskConfig
import lpmln.iset.RawIConditionUtils as riu
import itertools
import copy
from lpmln.search.distributed.final.FinalSearchBase import FinalIConditionsSearchBaseWorker, ITaskSignal, SearchQueueManager
config = cfg.load_configuration()
from lpmln.search.distributed.final.FinalSearchHTWorker import FinalIConditionsSearchHTWorker


class HTCheckingWorker(FinalIConditionsSearchHTWorker):
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

        data_files = list()
        data_file_current_lines = list()

        for itask in isc_tasks:
            # data_file_current_lines.append(-1)
            file_path = riu.get_complete_raw_icondition_file(*itask.k_m_n, itask.lp_type, itask.is_use_extended_rules)
            # file = open(file_path, mode="r", encoding="utf-8")
            data_files.append(file_path)

        is_process_ht_task_queue = False

        while True:
            if not pathlib.Path(config.task_host_lock_file).exists():
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

            if itask_id == ITaskSignal.kill_signal:
                break

            itask = isc_tasks[itask_id]

            data_file = data_files[itask_id]

            task_check_number, se_conditions_cache, nse_conditions_cache = \
                cls.verify_ht_tasks_from_file_data(cls, itask, ts[1], data_file)

            end_time = datetime.now()

            if len(se_conditions_cache) > 0:
                result_queue.put((ITaskSignal.se_condition_signal, itask_id, se_conditions_cache))

            if len(nse_conditions_cache) > 0:
                result_queue.put((ITaskSignal.nse_condition_signal, itask_id, nse_conditions_cache))

            result_tuple = (ITaskSignal.stat_signal, itask_id, task_check_number, (start_time, end_time))
            result_queue.put(result_tuple)

        logging.info("%s processes  %d ht itask slices" % (worker_name, processed_ht_task_slices_number))

    @staticmethod
    def verify_ht_tasks_from_file_data(cls, itask, task_slice, data_file):
        se_conditions_cache = list()
        nse_conditions_cache = list()
        validator = ISetConditionValidator(lp_type=itask.lp_type, is_use_extended_rules=itask.is_use_extended_rules)
        task_check_number = 0

        for line in range(task_slice[0], task_slice[1]):
            f = linecache.getline(data_file, line + 1)
            task_check_number += 1
            f = f.strip("\r\n")
            ne_isets = f.split(",")
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
    