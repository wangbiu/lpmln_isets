
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-27 10:08
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : RawIConditionSearchWorker.py
"""

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
config = cfg.load_configuration()


class RawIConditionSearchWorker(FinalIConditionsSearchPreWorker):
    @staticmethod
    def process_task_slice(cls, itask_id, itask, task_slice, manager_tuple):
        result_queue_cache = list()
        no_sv_task_slices, valid_skip_result = cls.process_semi_valid_task_slices(cls, itask_id, itask, task_slice)
        if valid_skip_result is not None:
            result_queue_cache.append(valid_skip_result)

        ht_check_task_slices = list()
        for ts in no_sv_task_slices:
            ht_slices, nse_skip_result = cls.process_nse_subparts_task_slices(cls, itask_id, itask, ts)
            ht_check_task_slices.extend(ht_slices)
            if nse_skip_result is not None:
                result_queue_cache.append(nse_skip_result)

        if len(ht_check_task_slices) == 0:
            return result_queue_cache, list()

        ht_check_items = cls.single_split_ht_tasks(cls, itask_id, ht_check_task_slices, None)

        return result_queue_cache, ht_check_items

    @staticmethod
    def single_split_ht_tasks(cls, itask_id, ht_check_task_slices, ht_task_queue):
        ht_check_items = list()
        for ts in ht_check_task_slices:
            left_isets = ts[0]
            right_zone_isets = ts[1]
            choice_number = ts[2]
            task_iter = itertools.combinations(right_zone_isets, choice_number)
            for right_iset in task_iter:
                ne_isets = left_isets.union(set(right_iset))
                ht_check_items.append(ne_isets)
        return ht_check_items

    @staticmethod
    def compute_ne_iset_number(task_slice):
        left_split = task_slice[0]
        if left_split:
            left_iset_size = len(task_slice[1])
        else:
            left_iset_size = task_slice[1]

        return left_iset_size + task_slice[3]

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

            rf = riu.get_empty_raw_icondition_file(*itask.k_m_n, itask.lp_type, itask.is_use_extended_rules, str(worker_id))
            raw_condition_files.append(rf)

        is_process_task_queue = False
        task_slice_cache = None
        last_nse_iset_number = 0
        result_queue_cache = list()
        single_round_processed_task_number = 0
        sleep_cnt = 0
        while True:

            # if sleep_cnt == 10:
            #     sleep_cnt = 0
            #     # logging.error(("result queue cache size ", len(result_queue_cache)))
            #     # logging.error("result queue cache has %d items, send sleep cnt 10", len(result_queue_cache))
            #     result_queue_cache = cls.batch_send_stat_info_2_result_queue(cls, result_queue_cache,
            #                                                                  result_queue, start_time)

            if not pathlib.Path(config.task_host_lock_file).exists():
                break

            is_task_finish = cls.check_itasks_finish_status(isc_tasks)
            if is_task_finish:
                logging.info("%s:%s all itasks terminate ..." % (worker_host_name, worker_name))
                break

            if task_slice_cache is None:
                if task_queue.empty():
                    # logging.error("result queue cache has %d items, send task queue empty",
                    #               len(result_queue_cache))
                    # result_queue_cache = cls.batch_send_stat_info_2_result_queue(cls, result_queue_cache,
                    #                                                              result_queue, start_time)
                    if is_process_task_queue:
                        logging.info("%s:%s waiting for task queue ... " % (worker_host_name, worker_name))
                        is_process_task_queue = False
                    time.sleep(1)
                    sleep_cnt += 1
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
            task_slice = (left_split, left, left_zone_length, right_choice_number)
            if left_split = True, left is left_isets
            else left is left_isets_size
            """

            task_slice = task_slice_cache[1]

            rule_number = sum(itask.k_m_n)
            ne_iset_number = cls.compute_ne_iset_number(task_slice)
            nse_ne_iset_number = ne_iset_number - 1

            load_nse_complete = cls.task_worker_load_nse_conditions(itask, ne_iset_number)
            if not load_nse_complete:
                # # logging.error("result queue cache has %d items, send load nse not complete", len(result_queue_cache))
                # result_queue_cache = cls.batch_send_stat_info_2_result_queue(cls, result_queue_cache, result_queue, start_time)
                if last_nse_iset_number != nse_ne_iset_number:
                    last_nse_iset_number = nse_ne_iset_number
                    logging.info((task_slice,
                                  "%s:%s waiting for %d-%d-%d nse complete file %d" % (
                                      worker_host_name, worker_name, *itask.k_m_n, nse_ne_iset_number)
                                  ))
                time.sleep(1)
                sleep_cnt += 1
                continue

            rq_cache, ht_check_items = cls.process_merge_small_task_slices(cls, itask_id, itask, task_slice, manager_tuple)
            result_queue_cache.extend(rq_cache)
            task_slice_cache = None

            raw_data_file = raw_condition_files[itask_id]
            ht_stat = cls.process_ht_tasks(cls, ht_check_items, itask_id, itask, ne_iset_number, result_queue, raw_data_file)
            if ht_stat is not None:
                result_queue_cache.append(ht_stat)

            # if len(result_queue_cache) > 2000:
                # logging.error("result queue cache has %d items, send cache size > 20000", len(result_queue_cache))

            if ne_iset_number <= rule_number:
                result_queue_cache = cls.batch_send_stat_info_2_result_queue(cls, result_queue_cache, result_queue, start_time)
            else:
                if task_queue.qsize() < 1000 or len(result_queue_cache) > 100000:
                    result_queue_cache = cls.batch_send_stat_info_2_result_queue(cls, result_queue_cache, result_queue,
                                                                                 start_time)

            if single_round_processed_task_number == 10000:
                msg_text = "%s:%s processes %d isc task slices, new round process %d task slices ... " % (
                    worker_host_name, worker_name, processed_task_slices_number, single_round_processed_task_number)
                single_round_processed_task_number = 0
                logging.info(msg_text)

        msg_text = "%s:%s processes %d isc task slices, new round process %d task slices ... " % (
            worker_host_name, worker_name, processed_task_slices_number, single_round_processed_task_number)
        logging.info(msg_text)

    @staticmethod
    def process_merge_small_task_slices(cls, itask_id, itask, task_slice, manager_tuple):
        left_split = task_slice[0]
        right_zone_isets = set(itask.meta_data.search_space_iset_ids[task_slice[2]:])
        total_rq_cache = list()
        total_ht_check_items = list()

        if left_split:
            new_task_slice = (set(task_slice[1]), right_zone_isets, task_slice[3])
            total_rq_cache, total_ht_check_items = cls.process_task_slice(cls, itask_id, itask, new_task_slice, manager_tuple)
        else:
            left_zone_isets = itask.meta_data.search_space_iset_ids[0:task_slice[2]]
            left_choice_number = task_slice[1]
            task_iter = itertools.combinations(left_zone_isets, left_choice_number)
            for left_isets in task_iter:
                new_task_slice = (set(left_isets), right_zone_isets, task_slice[3])
                rq_cache, ht_check_items = cls.process_task_slice(cls, itask_id, itask, new_task_slice,
                                                                               manager_tuple)
                total_rq_cache.extend(rq_cache)
                total_ht_check_items.extend(ht_check_items)

        return total_rq_cache, total_ht_check_items




    @staticmethod
    def process_ht_tasks(cls, ht_task_items, itask_id, itask, ne_iset_number, result_queue, raw_data_file):
        ht_task_number = len(ht_task_items)
        # logging.error("ht task number %d", ht_task_number)
        if ht_task_number <= 0:
            return None

        rule_number = sum(itask.k_m_n)
        if ne_iset_number <= rule_number:
            ht_task_items = cls.check_ht_task_items(ht_task_items, itask_id, itask, result_queue)

        ht_task_stat = (itask_id, ne_iset_number, ht_task_number, ht_task_number, 0)
        # result_queue.put(ht_task_stat)
        # logging.error(("send ht stat ", ht_task_stat))

        with open(raw_data_file, encoding="utf-8", mode="a") as rf:
            for ht in ht_task_items:
                data = [str(s) for s in ht]
                rf.write(",".join(data))
                rf.write("\n")

        return ht_task_stat

    @staticmethod
    def check_ht_task_items(ht_task_items, task_id, itask, result_queue):
        validator = ISetConditionValidator(lp_type=itask.lp_type, is_use_extended_rules=itask.is_use_extended_rules)
        se_conditions = list()
        nse_conditions = list()
        for ht in ht_task_items:
            is_contain_valid_rule, is_strongly_equivalent, condition = \
                validator.validate_isets_kmn_program_from_non_empty_ids_return_obj(
                    ht, *itask.k_m_n, is_check_valid_rule=False)

            if is_strongly_equivalent:
                se_conditions.append(condition)
            else:
                nse_conditions.append(condition)

        result_queue.put((ITaskSignal.se_condition_signal, task_id, se_conditions))
        result_queue.put((ITaskSignal.nse_condition_signal, task_id, nse_conditions))

        se_conditions = [obj.ne_iset_ids for obj in se_conditions]
        return se_conditions


if __name__ == '__main__':
    pass
    