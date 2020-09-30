
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-30 9:11
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : I4SearchMaster.py
"""

from lpmln.search.distributed.final.FinalSearchMaster import FinalIConditionsSearchMaster
from lpmln.search.distributed.i4.I4SearchWorker import I4SearchWorker
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


class I4SearchMaster(FinalIConditionsSearchMaster):
    @staticmethod
    def dump_isc_task_results(result_records):
        for it in result_records:
            file = it[3]
            with open(file, encoding="utf-8", mode="a") as outf:
                for data in it[2]:
                    data = [str(s) for s in data]
                    outf.write(",".join(data))
                    outf.write("\n")
            it[2] = list()

    @staticmethod
    def insert_i4_conditions(iconditions, result_record):
        for ic in iconditions:
            result_record[2].append(ic)

    @staticmethod
    def update_i4_result_record(result_record, result):
        result_record[1] += result[2]

    @staticmethod
    def process_i4_result_queue(cls, result_queue, result_record):
        result_cnt = 10000000
        working_hosts_diff = (0, 0)

        while not result_queue.empty() and result_cnt > 0:
            result_cnt -= 1
            result = result_queue.get()
            result_state = result[0]
            isc_task_id = result[1]

            if result_state == ITaskSignal.kill_signal:
                working_hosts_diff = cls.process_working_host_change(result, False)
                break
            elif result_state == ITaskSignal.add_worker_signal:
                working_hosts_diff = cls.process_working_host_change(result, True)
                break
            elif result_state == ITaskSignal.stat_signal:
                cls.update_i4_result_record(result_record[isc_task_id], result)
            elif result_state == ITaskSignal.se_condition_signal:
                cls.insert_i4_conditions(result[2], result_record[isc_task_id])

        cls.dump_isc_task_results(result_record)
        return working_hosts_diff

    @staticmethod
    def check_i4_tasks_status(cls, result_records):
        is_finish = True
        for tid in range(len(result_records)):
            it = result_records[tid]
            if it[0] > it[1]:
                is_finish = False
        return is_finish

    @staticmethod
    def itask_slices_generator(cls, isc_config_file):
        msg_text = "%s init task slices generator ..." % str(cls)
        logging.info(msg_text)
        msg.send_message(msg_text)

        manager, task_queue, ht_task_queue, result_queue = \
            SearchQueueManager.init_task_worker_queue_manager()

        isc_tasks_cfg = ITaskConfig(isc_config_file)
        isc_tasks = isc_tasks_cfg.isc_tasks

        for tid in range(len(isc_tasks)):
            it = isc_tasks[tid]
            i4_iset_ids = list(it.meta_data.search_i4_composed_iset_ids)
            i4_iset_size = len(i4_iset_ids)
            min_ne = 1
            max_ne = len(i4_iset_ids)
            max_left_zone_length = 10
            left_zone_size = max_left_zone_length

            if i4_iset_size < left_zone_size:
                left_zone_size = i4_iset_size // 2

            left_zone_iset_ids = i4_iset_ids[0:left_zone_size]
            right_zone_iset_ids = i4_iset_ids[left_zone_size:]

            for ne_iset_number in range(min_ne, max_ne + 1):

                task_slices = CombinationSearchingSpaceSplitter.vandermonde_generator(
                    left_zone_iset_ids, right_zone_iset_ids, ne_iset_number)
                for ts in task_slices:
                    task_queue.put((tid, ts))

        working_hosts_number = 5
        for i in range(working_hosts_number * 200):
            task_queue.put((ITaskSignal.kill_signal, -1))
        logging.info("all itasks has been dispatched")

    @staticmethod
    def init_pre_task_worker_pool(cls, isc_config_file, result_queue):
        worker_pool, result_queue, host_ip = I4SearchWorker.init_kmn_isc_task_workers(
            I4SearchWorker, isc_config_file, is_check_valid_rules=True, result_queue=result_queue)
        return worker_pool

    @staticmethod
    def send_itasks_progress_info(cls, result_records, manager_tuple, working_hosts_number, is_all_task_dispatched=False):
        task_queue = manager_tuple[1]
        ht_task_queue = manager_tuple[2]
        result_queue = manager_tuple[3]

        msg_texts = list()
        for i in range(len(result_records)):
            msg_text = "task %d has %d tasks, complete %d tasks" % (i, result_records[i][0], result_records[i][1])
            msg_texts.append(msg_text)

        msg_text = "isc tasks progress info, remain %d task hosts, %d task slices, %d ht task slices, %d results items:  \n\t\t%s" % (
            working_hosts_number, task_queue.qsize(), ht_task_queue.qsize(), result_queue.qsize(),
            "\n\t\t".join(msg_texts))

        if is_all_task_dispatched:
            msg_text = "all isc tasks are discatched, DO NOT add new worker! " + msg_text

        logging.info(msg_text)
        msg.send_message(msg_text)

    @staticmethod
    def init_kmn_isc_task_master_from_config(cls, isc_config_file="isets-tasks.json", sleep_time=30):
        manager, task_queue, ht_task_queue, result_queue = \
            SearchQueueManager.init_task_master_queue_manager()
        manager_tuple = (manager, task_queue, ht_task_queue, result_queue)
        localhost_ip = ssh.get_host_ip()

        isc_tasks_cfg = ITaskConfig(isc_config_file)
        isc_tasks = isc_tasks_cfg.isc_tasks
        result_record = list()
        for itask in isc_tasks:
            isnse.clear_task_terminate_flag_files(*itask.k_m_n)
            i4_iset_size = len(itask.meta_data.search_i4_composed_iset_ids)
            file = i4u.get_kmn_i4_all_result_file(*itask.k_m_n)
            if os.path.exists(file):
                os.remove(file)
            record = [2 ** i4_iset_size - 1, 0, list(), file]
            result_record.append(record)

        ts_generator_pool = cls.init_task_slices_generator_pool(cls, isc_config_file)

        pre_task_pool = cls.init_pre_task_worker_pool(cls, isc_config_file, result_queue)

        working_hosts_number = 0

        msg_text = "isc task master start, load %d isc tasks from %s" % (len(isc_tasks), isc_config_file)
        logging.info(msg_text)
        msg.send_message(msg_text)

        sleep_cnt = 0
        online_hosts = set()

        progress_msg_cnt = 10
        task_finish = False
        print_loop = 10
        print_cnt = 0
        while not task_finish:
            print_cnt += 1

            if print_cnt == print_loop:
                cls.send_itasks_progress_info(cls, result_record, manager_tuple, working_hosts_number, False)
                sleep_cnt = 0
                print_cnt = 0

            if sleep_cnt == progress_msg_cnt:
                cls.send_itasks_progress_info(cls, result_record, manager_tuple, working_hosts_number, False)
                sleep_cnt = 0
                print_cnt = 0

            task_finish = cls.check_i4_tasks_status(cls, result_record)
            if result_queue.empty():
                time.sleep(sleep_time)
                sleep_cnt += 1
                continue

            whn_diff = cls.process_i4_result_queue(cls, result_queue, result_record)

            whn_number = whn_diff[0]
            host_ip = whn_diff[1]
            working_hosts_number += whn_number

            if whn_number == 1:
                if host_ip != localhost_ip:
                    online_hosts.add(host_ip)
            elif whn_number == -1:
                if host_ip != localhost_ip:
                    online_hosts.remove(host_ip)

        ts_generator_pool.join()
        pre_task_pool.join()

        I4SearchWorker.send_worker_terminate_info(I4SearchWorker, localhost_ip, result_queue)

        while working_hosts_number > 0:
            if sleep_cnt == 10:
                cls.send_itasks_progress_info(cls, result_record, manager_tuple, working_hosts_number, True)
                sleep_cnt = 0

            if result_queue.empty():
                time.sleep(sleep_time)
                sleep_cnt += 1
                continue

            whn_diff = cls.process_i4_result_queue(cls, result_queue, result_record)
            working_hosts_number += whn_diff[0]

        msg_text = "isc tasks finish!"
        logging.info(msg_text)
        msg.send_message(msg=msg_text)
        cls.send_itasks_progress_info(cls, result_record, manager_tuple, working_hosts_number, True)

        return isc_tasks



if __name__ == '__main__':
    pass
    