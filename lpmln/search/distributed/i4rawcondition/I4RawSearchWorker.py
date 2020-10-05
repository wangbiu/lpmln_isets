
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-30 11:03
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : I4RawSearchWorker.py
"""

from lpmln.search.distributed.rawicondition.RawIConditionSearchWorker import RawIConditionSearchWorker
from lpmln.search.distributed.final.FinalSearchBase import ITaskSignal, SearchQueueManager
from lpmln.search.distributed.final.FinalSearchPreWorker import FinalIConditionsSearchPreWorker
import logging
import time
import pathlib
from lpmln.iset.ISetConditionValidator import ISetConditionValidator
import lpmln.config.GlobalConfig as cfg
from lpmln.itask.ITask import ITaskConfig
import itertools
from datetime import datetime
import lpmln.iset.RawIConditionUtils as riu
import lpmln.iset.I4SetUtils as i4u
import copy
config = cfg.load_configuration()


class I4RawSearchWorker(RawIConditionSearchWorker):

    @staticmethod
    def kmn_isc_task_worker(cls, isc_config_file="isets-tasks.json", worker_id=1, is_check_valid_rules=True):
        manager_tuple = SearchQueueManager.init_task_worker_queue_manager()
        # manager_tuple = (manager, task_queue, ht_task_queue, result_queue)
        task_queue = manager_tuple[1]
        result_queue = manager_tuple[3]
        start_time = datetime.now()

        worker_name = "worker-%d" % worker_id
        worker_host_name = config.worker_host_name
        processed_task_slices_number = 0

        msg_text = "task worker %s start!" % (worker_name)
        logging.info(msg_text)
        isc_tasks = ITaskConfig(isc_config_file)
        isc_tasks = isc_tasks.isc_tasks

        raw_condition_files = list()
        for itask in isc_tasks:
            rule_number = sum(itask.k_m_n)
            search_space_iset_number = len(itask.meta_data.search_space_iset_ids)
            itask.loaded_non_se_condition_files.add(0)
            for i in range(rule_number + 1, search_space_iset_number + 1):
                itask.loaded_non_se_condition_files.add(i)

            logging.info(("itask %d-%d-%d loaded nse condition files " % (*itask.k_m_n, ), itask.loaded_non_se_condition_files))
            rf = riu.get_empty_raw_icondition_file(*itask.k_m_n, itask.lp_type, itask.is_use_extended_rules,
                                                   str(worker_id))
            pathlib.Path(rf).touch()
            raw_condition_files.append(rf)

        is_process_task_queue = False
        task_slice_cache = None
        last_nse_iset_number = 0
        result_queue_cache = list()
        single_round_processed_task_number = 0
        while True:
            if not pathlib.Path(config.task_host_lock_file).exists():
                break

            is_task_finish = cls.check_itasks_finish_status(isc_tasks)
            if is_task_finish:
                logging.info("%s:%s all itasks terminate ..." % (worker_host_name, worker_name))
                break

            if task_slice_cache is None:
                if task_queue.empty():
                    if is_process_task_queue:
                        logging.info("%s:%s waiting for task queue ... " % (worker_host_name, worker_name))
                        is_process_task_queue = False
                    time.sleep(1)
                    continue
                else:
                    task_slice_cache = task_queue.get()
                    processed_task_slices_number += 1
                    single_round_processed_task_number += 1
                    is_process_task_queue = True

            itask_id = task_slice_cache[0]
            if itask_id == ITaskSignal.kill_signal:
                break

            itask = isc_tasks[itask_id]

            if itask.is_task_finish:
                task_slice_cache = None
                continue

            """
            task_slice_tuple(itask_id,  (left_choice_number, left_i4_tuple_from, left_i4_tuple_end, right_choice_number))
            """

            task_slice = task_slice_cache[1]

            rule_number = sum(itask.k_m_n)
            ne_iset_number = task_slice[0] + task_slice[3]
            nse_ne_iset_number = ne_iset_number - 1

            load_nse_complete = cls.task_worker_load_nse_conditions(itask, ne_iset_number)
            if not load_nse_complete:
                if last_nse_iset_number != nse_ne_iset_number:
                    last_nse_iset_number = nse_ne_iset_number
                    logging.info((task_slice,
                                  "%s:%s waiting for %d-%d-%d nse complete file %d" % (
                                      worker_host_name, worker_name, *itask.k_m_n, nse_ne_iset_number)
                                  ))
                time.sleep(1)
                continue

            raw_data_file = raw_condition_files[itask_id]
            rq_cache = cls.process_task_slice_saving_mem(cls, itask_id, itask, task_slice, raw_data_file, result_queue)
            result_queue_cache.append(rq_cache)
            task_slice_cache = None
            result_queue_cache = cls.batch_send_stat_info_2_result_queue(cls, result_queue_cache, result_queue,
                                                                         start_time)

            # if ne_iset_number <= rule_number:
            #     result_queue_cache = cls.batch_send_stat_info_2_result_queue(cls, result_queue_cache, result_queue,
            #                                                                  start_time)
            # else:
            #     if task_queue.qsize() < 1000 or len(result_queue_cache) > 1000:
            #         result_queue_cache = cls.batch_send_stat_info_2_result_queue(cls, result_queue_cache, result_queue,
            #                                                                      start_time)

            if single_round_processed_task_number == 10:
                msg_text = "%s:%s processes %d isc task slices, new round process %d task slices ... " % (
                    worker_host_name, worker_name, processed_task_slices_number, single_round_processed_task_number)
                single_round_processed_task_number = 0
                logging.info(msg_text)

        msg_text = "%s:%s processes %d isc task slices, new round process %d task slices ... " % (
            worker_host_name, worker_name, processed_task_slices_number, single_round_processed_task_number)
        logging.info(msg_text)

    @staticmethod
    def process_task_slice_saving_mem(cls, itask_id, itask, task_slice, raw_data_file, result_queue):
        left_choice_number = task_slice[0]
        i4_data_from = task_slice[1]
        i4_data_end = task_slice[2]
        right_choice_number = task_slice[3]
        ne_iset_number = left_choice_number + right_choice_number
        result_queue_cache = (itask_id, ne_iset_number, 0, 0, 0)

        right_zone_isets = set(itask.meta_data.search_space_iset_ids)
        search_i4_isets = set(itask.meta_data.search_i4_composed_iset_ids)
        right_zone_isets = right_zone_isets.difference(search_i4_isets)

        i4_data_file = i4u.get_kmn_i4_result_file_by_ne_iset_number(*itask.k_m_n, left_choice_number)

        data_file = open(i4_data_file, encoding="utf-8", mode="r")
        current_line = 0
        for ls in data_file:
            if current_line < i4_data_from:
                continue

            if current_line >= i4_data_end:
                break

            left_isets = ls.strip("\r\n ").split(",")
            left_isets = [int(s) for s in left_isets]
            ts = (set(left_isets), copy.deepcopy(right_zone_isets), right_choice_number)

            ht_slices, nse_skip_result = cls.process_nse_subparts_task_slices(cls, itask_id, itask, ts)
            if nse_skip_result is not None:
                result_queue_cache = cls.merge_result_stat(result_queue_cache, nse_skip_result)

            ht_check_items = cls.single_split_ht_tasks(cls, itask_id, ht_slices, None)
            ht_stat = cls.process_ht_tasks(cls, ht_check_items, itask_id, itask, ne_iset_number, result_queue,
                                           raw_data_file)
            if ht_stat is not None:
                result_queue_cache = cls.merge_result_stat(result_queue_cache, ht_stat)

        data_file.close()

        return result_queue_cache

    @staticmethod
    def merge_result_stat(stat1, stat2):
        if stat1[0] != stat2[0] or stat1[1] != stat2[1]:
            raise RuntimeError(("cannot merge stat info ", stat1, stat2))

        stat = [stat1[0], stat1[1]]
        for i in range(2, len(stat1)):
            stat.append(stat1[i] + stat2[i])
        return tuple(stat)



if __name__ == '__main__':
    pass
    