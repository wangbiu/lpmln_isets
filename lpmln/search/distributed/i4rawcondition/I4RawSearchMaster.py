
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-30 11:03
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : I4RawSearchMaster.py
"""

from lpmln.search.distributed.rawicondition.RawIConditionSearchMaster import RawIConditionSearchMaster
from lpmln.search.distributed.i4rawcondition.I4RawSearchWorker import I4RawSearchWorker
from lpmln.search.distributed.final.FinalSearchMaster import FinalIConditionsSearchMaster
from lpmln.search.distributed.i4.I4SearchWorker import I4SearchWorker
from lpmln.utils.counter.CombinaryCounter import CombinaryCounter
from multiprocessing import Pool
import logging
import time
import lpmln.message.Messager as msg
import lpmln.config.GlobalConfig as cfg
from lpmln.itask.ITask import ITaskConfig
import lpmln.iset.ISetNonSEUtils as isnse
import lpmln.utils.SSHClient as ssh
import copy
import pathlib
import os
from lpmln.utils.CombinationSpaceUtils import CombinationSearchingSpaceSplitter
from lpmln.search.distributed.final.FinalSearchBase import SearchQueueManager, ITaskSignal
import lpmln.iset.I4SetUtils as i4u

config = cfg.load_configuration()


class I4RawSearchMaster(RawIConditionSearchMaster):
    i4_meta = {
        "0-1-1": [0, 1],
        "1-1-0": [0, 1, 4, 6, 4, 1],
        "0-2-1": [0, 1, 15, 33, 35, 21, 7, 1],
        "1-2-0": [0, 1, 24, 204, 956, 2910, 6244, 9892, 11889, 11001, 7876, 4344, 1818, 560, 120, 16, 1],
        "1-1-1": [0, 1, 72, 612, 3012, 10170, 25368, 48840, 74601, 91939, 92246, 75558, 50386, 27132, 11628, 3876, 969,
                  171, 19, 1],
        "2-1-0": [0, 1, 108, 1674, 13752, 76599, 320796, 1066248, 2903643, 6612532, 12770262, 21121440, 30127823,
                  37238670, 40000320, 37387896, 30401406, 21468195, 13121780, 6906690, 3108084, 1184039, 376740, 98280,
                  20475, 3276, 378, 28, 1],
    }

    @staticmethod
    def get_kmn_meta_key(k_size, m_size, n_size):
        return "%d-%d-%d" % (k_size, m_size, n_size)

    @staticmethod
    def meta_data_checker(k_size, m_size, n_size):
        key = I4RawSearchMaster.get_kmn_meta_key(k_size, m_size, n_size)
        meta = I4RawSearchMaster.i4_meta[key]
        i4_iset_size = len(meta)
        for i in range(1, i4_iset_size):
            real_tuple_size = CombinaryCounter.compute_comb(i4_iset_size - 1, i)
            non_semi_valid_tuple_size = meta[i]
            left = real_tuple_size - non_semi_valid_tuple_size
            print("choose %d elements, has %d tuples, %d non-semi-valid tuples, remain %d tuples" % (
                i, real_tuple_size, non_semi_valid_tuple_size, left
            ))

    @staticmethod
    def itask_slice_generator_by_i4_meta(ne_iset_number, itask_id, itask, max_space_size, manager_tuple):
        task_queue = manager_tuple[1]
        result_queue = manager_tuple[3]

        kmn_key = I4RawSearchMaster.get_kmn_meta_key(*itask.k_m_n)
        i4_meta = I4RawSearchMaster.i4_meta[kmn_key]

        left_zone_length = len(itask.meta_data.search_i4_composed_iset_ids)
        search_isets_length = len(itask.meta_data.search_space_iset_ids)
        right_zone_length = search_isets_length - left_zone_length


        if ne_iset_number <= right_zone_length:
            semi_valid_i4_slices_size = CombinaryCounter.compute_comb(right_zone_length, ne_iset_number)
            valid_skip_number = CombinaryCounter.compute_comb(right_zone_length, ne_iset_number)
            result_tuple = (
            ITaskSignal.stat_signal, itask_id, ne_iset_number, 0, valid_skip_number, valid_skip_number, None)
            result_queue.put(result_tuple)


        for left_choice in range(1, left_zone_length + 1):
            right_choice = ne_iset_number - left_choice
            if right_choice > right_zone_length or left_choice > ne_iset_number:
                continue

            single_slice_right_task_number = CombinaryCounter.compute_comb(right_zone_length, right_choice)
            task_i4_slice_number = max_space_size // single_slice_right_task_number + 1
            non_semi_valid_i4_slices_size = i4_meta[left_choice]
            itask_sizes = non_semi_valid_i4_slices_size // task_i4_slice_number
            itask_splitting_points = [i * task_i4_slice_number for i in range(itask_sizes)]
            if len(itask_splitting_points) == 0:
                itask_splitting_points.append(0)

            if itask_splitting_points[-1] < non_semi_valid_i4_slices_size:
                itask_splitting_points.append(non_semi_valid_i4_slices_size)

            for i in range(1, len(itask_splitting_points)):
                itask_slice_tuple = (left_choice, itask_splitting_points[i-1], itask_splitting_points[i], right_choice)
                itask_slice_tuple = (itask_id, itask_slice_tuple)
                task_queue.put(itask_slice_tuple)

            total_i4_silces_size = CombinaryCounter.compute_comb(left_zone_length, left_choice)
            semi_valid_i4_slices_size = total_i4_silces_size - non_semi_valid_i4_slices_size
            if semi_valid_i4_slices_size > 0:
                valid_skip_number = semi_valid_i4_slices_size * single_slice_right_task_number
                result_tuple = (ITaskSignal.stat_signal, itask_id, ne_iset_number, 0, valid_skip_number, valid_skip_number, None)
                result_queue.put(result_tuple)

    @staticmethod
    def itask_slices_generator(cls, isc_config_file):
        max_space_size = 100000000000
        msg_text = "%s init task slices generator ..." % str(cls)
        logging.info(msg_text)
        msg.send_message(msg_text)

        manager_tuple = SearchQueueManager.init_task_worker_queue_manager()
        task_queue = manager_tuple[1]

        isc_tasks_cfg = ITaskConfig(isc_config_file)
        isc_tasks = isc_tasks_cfg.isc_tasks

        for tid in range(len(isc_tasks)):
            it = isc_tasks[tid]
            min_ne = it.min_ne
            max_ne = it.max_ne
            rule_number = sum(it.k_m_n)
            isnse.clear_task_space_layer_finish_flag_files(*it.k_m_n, min_ne, max_ne)

            for ne_iset_number in range(min_ne, max_ne + 1):
                msg_text = "generating %d-%d-%d %d layer task slices" % (*it.k_m_n, ne_iset_number)
                logging.info(msg_text)

                if ne_iset_number <= rule_number:
                    cls.itask_slice_generator_by_i4_meta(ne_iset_number, tid, it, max_space_size, manager_tuple)
                else:
                    if not cls.check_itask_terminate_status(it):
                        flag_file = isnse.get_task_space_layer_finish_flag_file(*it.k_m_n, ne_iset_number - 2)
                        while not pathlib.Path(flag_file).exists():
                            if cls.check_itask_terminate_status(it):
                                break
                            time.sleep(1)

                        cls.itask_slice_generator_by_i4_meta(ne_iset_number, tid, it, max_space_size, manager_tuple)

        working_hosts_number = 5
        for i in range(working_hosts_number * 200):
            task_queue.put((ITaskSignal.kill_signal, -1))
        logging.info("all itasks has been dispatched")

    @staticmethod
    def init_pre_task_worker_pool(cls, isc_config_file, result_queue):
        worker_pool, result_queue, host_ip = I4RawSearchWorker.init_kmn_isc_task_workers(
            I4RawSearchWorker, isc_config_file, is_check_valid_rules=True, result_queue=result_queue)
        return worker_pool


if __name__ == '__main__':
    kmns = [[0, 1, 1], [1, 1, 0], [0, 2, 1], [1, 1, 1], [1, 2, 0]]
    for kmn in kmns:
        I4RawSearchMaster.meta_data_checker(*kmn)
    pass
    