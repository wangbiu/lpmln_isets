
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-19 14:45
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : FinalSearchPreWorker.py
"""

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
from lpmln.search.distributed.final.FinalSearchBase import FinalIConditionsSearchBaseWorker, ITaskSignal, SearchQueueManager
import itertools
config = cfg.load_configuration()


class FinalIConditionsSearchPreWorker(FinalIConditionsSearchBaseWorker):
    @staticmethod
    def process_task_slice(cls, itask_id, itask, task_slice, manager_tuple):
        ht_task_queue = manager_tuple[2]
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
            return result_queue_cache

        ts = ht_check_task_slices[0]
        ne_iset_number = len(ts[0]) + ts[2]

        if ne_iset_number < 8:
            cls.vandermonde_split_ht_task(cls, itask_id, ht_check_task_slices, ht_task_queue)
        else:
            cls.single_split_ht_tasks(cls, itask_id, ht_check_task_slices, ht_task_queue)

        return result_queue_cache

    @staticmethod
    def vandermonde_split_ht_task(cls, itask_id, ht_check_task_slices, ht_task_queue):
        for ts in ht_check_task_slices:
            left_isets = ts[0]
            right_zone_isets = list(ts[1])
            choice_number = ts[2]

            split_left_length = len(right_zone_isets) // 2
            if split_left_length > 14:
                split_left_length = 14

            split_left_zone = set(right_zone_isets[0:split_left_length])
            split_right_zone = set(right_zone_isets[split_left_length:])
            v_generator = CombinationSearchingSpaceSplitter.vandermonde_generator(
                split_left_zone, split_right_zone, choice_number)

            for vts in v_generator:
                for iset in left_isets:
                    vts[0].add(iset)
                ht_task_queue.put((itask_id, vts))

    @staticmethod
    def single_split_ht_tasks(cls, itask_id, ht_check_task_slices, ht_task_queue):
        for ts in ht_check_task_slices:
            left_isets = ts[0]
            right_zone_isets = ts[1]
            choice_number = ts[2]
            task_iter = itertools.combinations(right_zone_isets, choice_number)
            for right_iset in task_iter:
                ne_isets = left_isets.union(set(right_iset))
                task_item = (itask_id, (ne_isets, set(), 0))
                ht_task_queue.put(task_item)

    @staticmethod
    def process_one_nse_subpart_task_slice(cls, nse_isets, task_slice):
        """
        :param cls:
        :param nse_isets:
        :param task_slice: (left_iset_ids, right_zone_iset_ids, right_zone_choice_number)
        :return:
        """
        original_left_isets = set(task_slice[0])
        remained_nse_isets = nse_isets.difference(original_left_isets)

        is_yang_split, yang_task_slices = CombinationSearchingSpaceSplitter.yanghui_split(
            task_slice[1], task_slice[2], remained_nse_isets)

        if is_yang_split:
            nse_slice = yang_task_slices[-1]
            yang_task_slices = yang_task_slices[0:-1]
            skip_number = CombinaryCounter.compute_comb(len(nse_slice[1]), nse_slice[2])
        else:
            skip_number = 0

        for yts in yang_task_slices:
            for iset in original_left_isets:
                yts[0].add(iset)

        return skip_number, yang_task_slices

    @staticmethod
    def process_one_nse_subpart_task_slice2(cls, nse_isets, task_slice):
        """
        :param cls:
        :param nse_isets:
        :param task_slice: (left_iset_ids, right_zone_iset_ids, right_zone_choice_number)
        :return:
        """
        original_left_isets = set(task_slice[0])
        remained_nse_isets = nse_isets.difference(original_left_isets)
        yang_task_slices = list()

        if len(remained_nse_isets) == 0:
            skip_number = CombinaryCounter.compute_comb(len(task_slice[1]), task_slice[2])
            return skip_number, yang_task_slices

        if not remained_nse_isets.issubset(task_slice[1]):
            skip_number = 0
            yang_task_slices.append(task_slice)
            return skip_number, yang_task_slices

        nse_isets_size = len(remained_nse_isets)
        right_zone_isets = task_slice[1].difference(remained_nse_isets)
        v_generator = CombinationSearchingSpaceSplitter.vandermonde_generator(
            remained_nse_isets, right_zone_isets, task_slice[2])

        skip_number = 0
        for slice in v_generator:
            if len(slice[0]) == nse_isets_size:
                skip_number += CombinaryCounter.compute_comb(len(slice[1]), slice[2])
                continue

            for a in original_left_isets:
                slice[0].add(a)

            yang_task_slices.append(slice)

        return skip_number, yang_task_slices

    @staticmethod
    def process_nse_subparts_task_slices(cls, itask_id, itask, task_slice):
        skip_number = 0
        processed_task_slices = [task_slice]
        original_left_isets = set(task_slice[0])
        ne_iset_number = len(original_left_isets) + task_slice[2]

        for nse in itask.non_se_conditions:
            nse_new_task_slices = list()
            for ts in processed_task_slices:
                ts_skip_number, ts_new_task_slices = cls.process_one_nse_subpart_task_slice2(cls, nse, ts)
                # print("nse: ", nse, ", task slice: ", ts, ", skip ", ts_skip_number)
                skip_number += ts_skip_number
                nse_new_task_slices.extend(ts_new_task_slices)

            processed_task_slices = nse_new_task_slices

        # remain_task = 0
        # for ts in processed_task_slices:
        #     remain_task += CombinaryCounter.compute_comb(len(ts[1]), ts[2])
        # total_task = CombinaryCounter.compute_comb(len(task_slice[1]), task_slice[2])
        # print("task slice ", task_slice, "has task ", total_task, "total skip ", skip_number, "remian ", remain_task)
        # print("total = skip + remain ", remain_task + skip_number == total_task, "\n")

        nse_skip_result = None

        if skip_number > 0:
            nse_skip_result = (itask_id, ne_iset_number, 0, skip_number, 0)
            # result_queue.put(result_item)
            # print("nse skip", nse_skip_result)

        return processed_task_slices, nse_skip_result

    @staticmethod
    def process_semi_valid_task_slices(cls, itask_id, itask, task_slice):
        left_isets = task_slice[0]
        right_zone_isets = task_slice[1]
        right_zone_choice_number = task_slice[2]
        ne_iset_number = len(left_isets) + right_zone_choice_number
        search_i4_isets = set(itask.meta_data.search_i4_composed_iset_ids)
        skip_number = 0
        new_task_slices = list()

        right_zone_i4_isets = right_zone_isets.intersection(search_i4_isets)
        if len(right_zone_i4_isets) == 0:
            v_generator = [task_slice]
        else:
            right_zone_non_i4_isets = right_zone_isets.difference(right_zone_i4_isets)
            v_generator = CombinationSearchingSpaceSplitter.vandermonde_generator(
                right_zone_i4_isets, right_zone_non_i4_isets, right_zone_choice_number)

        for ts in v_generator:
            new_left_ids = left_isets.union(ts[0])
            is_contain_semi_valid_rule = iscm.check_contain_rules_without_i_n_iset(
                4, new_left_ids, itask.rule_number, itask.is_use_extended_rules)
            if is_contain_semi_valid_rule:
                skip_number += CombinaryCounter.compute_comb(len(ts[1]), ts[2])
            else:
                new_task_slices.append((new_left_ids, ts[1], ts[2]))

        valid_skip_result = None
        if skip_number > 0:
            valid_skip_result = (itask_id, ne_iset_number, 0, skip_number, skip_number)
            # print("valid skip ", valid_skip_result)
            # result_queue.put(result_tuple)

        return new_task_slices, valid_skip_result

    @staticmethod
    def task_worker_load_nse_conditions(itask, task_slice):
        ne_iset_number = task_slice[2] + len(task_slice[0])
        load_complete = True
        for i in range(1, ne_iset_number):
            if i not in itask.loaded_non_se_condition_files:
                complete_flag = isnse.get_transport_complete_flag_file(*itask.k_m_n, i)
                if pathlib.Path(complete_flag).exists():
                    nse_conditions = isnse.load_kmn_non_se_results(*itask.k_m_n, i, itask.lp_type,
                                                                   itask.is_use_extended_rules)
                    itask.non_se_conditions.extend(nse_conditions)
                    itask.loaded_non_se_condition_files.add(i)
                else:
                    load_complete = False
                    break
        return load_complete

    @staticmethod
    def kmn_isc_task_worker(cls, isc_config_file="isets-tasks.json", worker_id=1, is_check_valid_rules=True):
        manager_tuple = SearchQueueManager.init_task_worker_queue_manager()
        # manager_tuple = (manager, task_queue, ht_task_queue, result_queue)
        task_queue = manager_tuple[1]

        worker_name = "worker-%d" % worker_id
        worker_host_name = config.worker_host_name
        processed_task_slices_number = 0

        msg_text = "task worker %s start!" % (worker_name)
        logging.info(msg_text)
        isc_tasks = ITaskConfig(isc_config_file)
        isc_tasks = isc_tasks.isc_tasks

        for itask in isc_tasks:
            # itask.loaded_non_se_condition_files.add(1)
            itask.loaded_non_se_condition_files.add(0)

        is_process_task_queue = False
        task_slice_cache = None
        last_nse_iset_number = 0
        result_queue_cache = list()
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
                    is_process_task_queue = True


            itask_id = task_slice_cache[0]
            if itask_id == ITaskSignal.kill_signal:
                break

            itask = isc_tasks[itask_id]

            if itask.is_task_finish:
                task_slice_cache = None
                continue

            task_slice = task_slice_cache[1]
            nse_iset_number = task_slice[2] + len(task_slice[0]) - 1

            load_nse_complete = cls.task_worker_load_nse_conditions(itask, task_slice)
            if not load_nse_complete:
                if last_nse_iset_number != nse_iset_number:
                    last_nse_iset_number = nse_iset_number
                    logging.info((task_slice,
                                  "%s:%s waiting for %d-%d-%d nse complete file %d" % (
                                      worker_host_name, worker_name, *itask.k_m_n, nse_iset_number)
                                  ))
                result_queue_cache = cls.batch_send_stat_info_2_result_queue(cls, result_queue_cache, manager_tuple[3])
                time.sleep(1)
                continue

            rq_cache = cls.process_task_slice(cls, itask_id, itask, task_slice, manager_tuple)
            result_queue_cache.extend(rq_cache)
            task_slice_cache = None

            if len(result_queue_cache) > 2000:
                result_queue_cache = cls.batch_send_stat_info_2_result_queue(cls, result_queue_cache, manager_tuple[3])

        logging.info(
            "%s processes %d isc task slices ... " % (worker_name, processed_task_slices_number))

    @staticmethod
    def batch_send_stat_info_2_result_queue(cls, result_queue_cache, result_queue):
        if len(result_queue_cache) == 0:
            return result_queue_cache

        key = "%d-%d"
        results = dict()
        for rq in result_queue_cache:
            data_key = key % (rq[0], rq[1])
            if data_key in results:
                data_item = results[data_key]
            else:
                data_item = [rq[0], rq[1], 0, 0, 0]
                results[data_key] = data_item

            for i in range(2, len(rq)):
                data_item[i] += rq[i]

        for data_key in results:
            data_item = [ITaskSignal.stat_signal]
            data_item.extend(results[data_key])
            data_item.append(None)
            data_item = tuple(data_item)
            result_queue.put(data_item)

        result_queue_cache = list()
        return result_queue_cache

    @staticmethod
    def init_extra_kmn_pre_task_workers(cls, isc_config_file="isets-tasks.json", is_check_valid_rules=True,
                                  result_queue=None):
        worker_pool, result_queue, host_ip = cls.init_kmn_isc_task_workers(cls, isc_config_file, is_check_valid_rules, result_queue)
        worker_pool.join()
        cls.send_worker_terminate_info(cls, host_ip, result_queue)



if __name__ == '__main__':
    pass
    