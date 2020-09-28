
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-27 10:07
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : RawIConditionSearchMaster.py
"""

from lpmln.search.distributed.final.FinalSearchBase import ITaskSignal, SearchQueueManager
from lpmln.search.distributed.final.FinalSearchMaster import FinalIConditionsSearchMaster
from lpmln.search.distributed.rawicondition.RawIConditionSearchWorker import RawIConditionSearchWorker
import logging
import time
from datetime import datetime
import lpmln.message.Messager as msg
import lpmln.config.GlobalConfig as cfg
from lpmln.itask.ITask import ITaskConfig
import lpmln.iset.ISetNonSEUtils as isnse
import lpmln.utils.SSHClient as ssh
import copy
from lpmln.utils.CombinationSpaceUtils import CombinationSearchingSpaceSplitter
import pathlib
config = cfg.load_configuration()


class RawIConditionSearchMaster(FinalIConditionsSearchMaster):

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
            min_ne = it.min_ne
            max_ne = it.max_ne
            isnse.clear_task_space_layer_finish_flag_files(*it.k_m_n, min_ne, max_ne)

            left_zone_length = len(it.meta_data.search_i4_composed_iset_ids)
            search_isets = copy.deepcopy(it.meta_data.search_space_iset_ids)
            search_isets_length = len(search_isets)

            max_left_zone_length = 12
            if left_zone_length > max_left_zone_length:
                left_zone_length = 12

            rule_number = sum(it.k_m_n)
            left_zone_iset_ids = search_isets[0:left_zone_length]
            right_zone_iset_ids = search_isets[left_zone_length:]

            for ne_iset_number in range(min_ne, max_ne + 1):
                msg_text = "generating %d-%d-%d %d layer task slices" % (*it.k_m_n, ne_iset_number)
                logging.info(msg_text)
                if ne_iset_number <= rule_number:
                    task_slices = CombinationSearchingSpaceSplitter.vandermonde_generator(
                        left_zone_iset_ids, right_zone_iset_ids, ne_iset_number)
                    for ts in task_slices:
                        new_ts = (set(ts[0]), left_zone_length, ts[2])
                        task_queue.put((tid, new_ts))
                else:
                    if not cls.check_itask_terminate_status(it):
                        flag_file = isnse.get_task_space_layer_finish_flag_file(*it.k_m_n, ne_iset_number - 2)
                        while not pathlib.Path(flag_file).exists():
                            if cls.check_itask_terminate_status(it):
                                break
                            time.sleep(1)

                        task_slices = CombinationSearchingSpaceSplitter.near_uniform_vandermonde_generator(
                            left_zone_iset_ids, right_zone_iset_ids, ne_iset_number)
                        ts_cnt = 0
                        for ts in task_slices:
                            new_ts = (set(ts[0]), search_isets_length - len(ts[1]), ts[2])
                            task_queue.put((tid, new_ts))

                            ts_cnt += 1
                            if ts_cnt % 10000 == 0 and cls.check_itask_terminate_status(it):
                                break

        working_hosts_number = 5
        for i in range(working_hosts_number * 200):
            task_queue.put((ITaskSignal.kill_signal, -1))
        logging.info("all itasks has been dispatched")

    @staticmethod
    def check_itasks_status(cls, itasks, host_ips, manager_tuple, working_host_number):
        is_finish = True
        for tid in range(len(itasks)):
            it = itasks[tid]
            if not it.is_task_finish:
                rule_number = sum(it.k_m_n)
                current_ne_number = it.working_ne_iset_numbers

                if it.is_no_new_ht_check_task() and current_ne_number > 1:
                    isnse.create_and_send_task_early_terminate_flag_file(*it.k_m_n, current_ne_number, host_ips)
                    it.is_task_finish = True
                    it.save_progress_info()
                    continue

                task_complete = it.hierarchical_task_complete_number[current_ne_number]
                task_total = it.hierarchical_task_number[current_ne_number]
                isnse.create_task_space_layer_finish_flag_file(*it.k_m_n, 0)
                if task_complete == task_total:
                    it.save_progress_info()
                    isnse.create_task_space_layer_finish_flag_file(*it.k_m_n, current_ne_number)

                    if current_ne_number <= rule_number:
                        nse_file = it.flush_non_se_condition()
                        isnse.transport_non_se_results([nse_file], host_ips)
                        isnse.create_and_send_transport_complete_flag_file(*it.k_m_n, current_ne_number, host_ips)

                    cls.send_itasks_progress_info(cls, itasks, manager_tuple, working_host_number, False)

                    if current_ne_number < it.max_ne:
                        it.working_ne_iset_numbers += 1
                        is_finish = False
                    else:
                        it.is_task_finish = True
                        isnse.create_and_send_task_early_terminate_flag_file(*it.k_m_n, current_ne_number, host_ips)
                        it.save_progress_info()
                else:
                    is_finish = False
        return is_finish

    @staticmethod
    def init_pre_task_worker_pool(cls, isc_config_file, result_queue):
        worker_pool, result_queue, host_ip = RawIConditionSearchWorker.init_kmn_isc_task_workers(
            RawIConditionSearchWorker, isc_config_file, is_check_valid_rules=True, result_queue=result_queue)
        return worker_pool

    @staticmethod
    def init_kmn_isc_task_master_from_config(cls, isc_config_file="isets-tasks.json", sleep_time=30):
        start_time = datetime.now()
        manager, task_queue, ht_task_queue, result_queue = \
            SearchQueueManager.init_task_master_queue_manager()
        manager_tuple = (manager, task_queue, ht_task_queue, result_queue)
        localhost_ip = ssh.get_host_ip()

        isc_tasks_cfg = ITaskConfig(isc_config_file)
        isc_tasks = isc_tasks_cfg.isc_tasks
        for itask in isc_tasks:
            itask.init_task_numbers()
            isnse.clear_task_terminate_flag_files(*itask.k_m_n)

        ts_generator_pool = cls.init_task_slices_generator_pool(cls, isc_config_file)
        pre_pool = cls.init_pre_task_worker_pool(cls, isc_config_file, result_queue)
        working_hosts_number = 0

        msg_text = "isc task master start, load %d isc tasks from %s" % (len(isc_tasks), isc_config_file)
        logging.info(msg_text)
        msg.send_message(msg_text)

        sleep_cnt = 0
        online_hosts = set()

        progress_msg_cnt = 10
        task_finish = False
        print_loop = 100000
        print_cnt = 0
        while not task_finish:
            print_cnt += 1

            if print_cnt == print_loop:
                cls.send_itasks_progress_info(cls, isc_tasks, manager_tuple, working_hosts_number, False)
                sleep_cnt = 0
                print_cnt = 0

            if sleep_cnt == progress_msg_cnt:
                cls.send_itasks_progress_info(cls, isc_tasks, manager_tuple, working_hosts_number, False)
                sleep_cnt = 0
                print_cnt = 0

            task_finish = cls.check_itasks_status(cls, isc_tasks, online_hosts, manager_tuple, working_hosts_number)
            if result_queue.empty():
                time.sleep(sleep_time)
                sleep_cnt += 1
                continue

            whn_diff = cls.process_result_queue(cls, result_queue, isc_tasks)

            whn_number = whn_diff[0]
            host_ip = whn_diff[1]
            working_hosts_number += whn_number

            if whn_number == 1:
                if host_ip != localhost_ip:
                    online_hosts.add(host_ip)
                    cls.update_nse_files_to_new_host(host_ip, isc_tasks)
            elif whn_number == -1:
                if host_ip != localhost_ip:
                    online_hosts.remove(host_ip)

        ts_generator_pool.join()
        pre_pool.join()
        RawIConditionSearchWorker.send_worker_terminate_info(RawIConditionSearchWorker, localhost_ip, result_queue)

        while working_hosts_number > 0:
            if sleep_cnt == 10:
                cls.send_itasks_progress_info(cls, isc_tasks, manager_tuple, working_hosts_number, True)
                sleep_cnt = 0

            if result_queue.empty():
                time.sleep(sleep_time)
                sleep_cnt += 1
                continue

            whn_diff = cls.process_result_queue(cls, result_queue, isc_tasks)
            working_hosts_number += whn_diff[0]

        end_time = datetime.now()
        msg_text = "isc tasks finish, running time: %s" % str(end_time - start_time)
        logging.info(msg_text)
        msg.send_message(msg=msg_text)

        for it in isc_tasks:
            it.task_finish()
            msg_text = it.get_final_detail_progress_info()
            logging.info(msg_text)
            msg.send_message(msg=msg_text)
            # attached_files.append(it.result_file)

        return isc_tasks


if __name__ == '__main__':
    pass
    