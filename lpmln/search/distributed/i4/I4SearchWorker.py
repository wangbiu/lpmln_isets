
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-30 9:12
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : I4SearchWorker.py
"""

from lpmln.search.distributed.final.FinalSearchPreWorker import FinalIConditionsSearchPreWorker
import logging
import time
import pathlib

import lpmln.config.GlobalConfig as cfg
from lpmln.itask.ITask import ITaskConfig
import lpmln.iset.ISetNonSEUtils as isnse
import copy
from lpmln.utils.CombinationSpaceUtils import CombinationSearchingSpaceSplitter
from lpmln.utils.counter.CombinaryCounter import CombinaryCounter
import lpmln.iset.ISetCompositionUtils as iscm
from lpmln.search.distributed.final.FinalSearchBase import ITaskSignal, SearchQueueManager
import itertools
from datetime import datetime
config = cfg.load_configuration()


class I4SearchWorker(FinalIConditionsSearchPreWorker):

    @staticmethod
    def process_semi_valid_task_slices(cls, itask_id, itask, task_slice):
        left_isets = task_slice[0]
        right_zone_isets = task_slice[1]
        right_zone_choice_number = task_slice[2]
        task_cnt = 0

        i4_ne_isets = list()

        task_iter = itertools.combinations(right_zone_isets, right_zone_choice_number)
        for right_isets in task_iter:
            task_cnt += 1
            ne_isets = left_isets + list(right_isets)
            is_contain_semi_valid_rule = iscm.check_contain_rules_without_i_n_iset(
                4, ne_isets, itask.rule_number, itask.is_use_extended_rules)
            if not is_contain_semi_valid_rule:
                i4_ne_isets.append(ne_isets)

        return i4_ne_isets, task_cnt

    @staticmethod
    def kmn_isc_task_worker(cls, isc_config_file="isets-tasks.json", worker_id=1, is_check_valid_rules=True):
        manager_tuple = SearchQueueManager.init_task_worker_queue_manager()
        # manager_tuple = (manager, task_queue, ht_task_queue, result_queue)
        task_queue = manager_tuple[1]
        result_queue = manager_tuple[3]

        worker_name = "worker-%d" % worker_id
        worker_host_name = config.worker_host_name
        processed_task_slices_number = 0

        msg_text = "task worker %s start!" % (worker_name)
        logging.info(msg_text)
        isc_tasks = ITaskConfig(isc_config_file)
        isc_tasks = isc_tasks.isc_tasks

        is_process_task_queue = False
        while True:
            if not pathlib.Path(config.task_host_lock_file).exists():
                break

            if task_queue.empty():
                if is_process_task_queue:
                    logging.info("%s:%s waiting for task queue ... " % (worker_host_name, worker_name))
                    is_process_task_queue = False
                time.sleep(1)
                continue

            task_slice_cache = task_queue.get()
            processed_task_slices_number += 1
            is_process_task_queue = True

            itask_id = task_slice_cache[0]
            if itask_id == ITaskSignal.kill_signal:
                break

            itask = isc_tasks[itask_id]

            task_slice = task_slice_cache[1]

            i4_ne_isets, task_number = cls.process_semi_valid_task_slices(cls, itask_id, itask, task_slice)
            result_queue.put((ITaskSignal.se_condition_signal, itask_id, i4_ne_isets))
            result_queue.put((ITaskSignal.stat_signal, itask_id, task_number))

        logging.info(
            "%s processes %d isc task slices ... " % (worker_name, processed_task_slices_number))

    @staticmethod
    def batch_send_stat_info_2_result_queue(cls, result_queue_cache, result_queue, start_time):
        if len(result_queue_cache) == 0:
            return result_queue_cache

        key = "%d-%d"
        merge = dict()

        # logging.error("result queue has %d items ", len(result_queue_cache))
        # logging.error(result_queue_cache)

        for rq in result_queue_cache:
            data_key = key % (rq[0], rq[1])
            if data_key in merge:
                data_item = merge[data_key]
            else:
                data_item = [rq[0], rq[1], 0, 0, 0]
                merge[data_key] = data_item

            for i in range(2, len(rq)):
                data_item[i] += rq[i]

            # if data_item[2] == 0 and datetime[3] == 0 and data_item[4] == 0:
            #     print("wrong stat result item: ", rq)

        # logging.error(("stat merge result", merge))

        for data_key in merge:
            data_item = list()
            data_item.append(ITaskSignal.stat_signal)
            data_item.extend(merge[data_key])
            data_item.append((start_time, datetime.now()))
            data_item = tuple(data_item)

            # print("send stat info", data_item)
            # if data_item[3] == 0 and data_item[4] == 0 and data_item[5] == 0:
            #     print("wrong stat result item: ", data_item)

            # logging.error(("sended stat info", data_item))

            result_queue.put(data_item)

        result_queue_cache = list()
        return result_queue_cache

    @staticmethod
    def init_extra_kmn_pre_task_workers(cls, isc_config_file="isets-tasks.json", is_check_valid_rules=True,
                                        result_queue=None):
        worker_pool, result_queue, host_ip = cls.init_kmn_isc_task_workers(cls, isc_config_file, is_check_valid_rules,
                                                                           result_queue)
        worker_pool.join()
        cls.send_worker_terminate_info(cls, host_ip, result_queue)


if __name__ == '__main__':
    pass
    