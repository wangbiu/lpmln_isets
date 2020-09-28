
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-19 14:45
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : FinalSearchMaster.py
"""

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
from lpmln.utils.CombinationSpaceUtils import CombinationSearchingSpaceSplitter
from lpmln.search.distributed.final.FinalSearchBase import SearchQueueManager, ITaskSignal
from lpmln.search.distributed.final.FinalSearchPreWorker import FinalIConditionsSearchPreWorker
config = cfg.load_configuration()


class FinalIConditionsSearchMaster:
    @staticmethod
    def dump_isc_task_results(itasks):
        msg_texts = []
        for it in itasks:
            it.dump_tmp_se_condition()
            msg_texts.append(it.get_progress_info())
        return msg_texts

    @staticmethod
    def send_itasks_progress_info(cls, isc_tasks, manager_tuple, working_hosts_number, is_all_task_dispatched=False):

        task_queue = manager_tuple[1]
        ht_task_queue = manager_tuple[2]
        result_queue = manager_tuple[3]
        msg_texts = cls.dump_isc_task_results(isc_tasks)

        msg_text = "isc tasks progress info, remain %d task hosts, %d task slices, %d ht task slices, %d results items:  \n\t\t%s" % (
            working_hosts_number, task_queue.qsize(), ht_task_queue.qsize(), result_queue.qsize(), "\n\t\t".join(msg_texts))

        if is_all_task_dispatched:
            msg_text = "all isc tasks are discatched, DO NOT add new worker! " + msg_text

        logging.info(msg_text)
        msg.send_message(msg_text)

    @staticmethod
    def check_itasks_status(cls, itasks, host_ips, manager_tuple, working_host_number):
        is_finish = True
        for tid in range(len(itasks)):
            it = itasks[tid]
            if not it.is_task_finish:

                current_ne_number = it.working_ne_iset_numbers
                if it.is_no_new_se_condition() and current_ne_number > 1:
                    isnse.create_and_send_task_early_terminate_flag_file(*it.k_m_n, current_ne_number, host_ips)
                    it.is_task_finish = True
                    it.save_progress_info()
                    continue

                task_complete = it.hierarchical_task_complete_number[current_ne_number]
                task_total = it.hierarchical_task_number[current_ne_number]
                if task_complete == task_total:
                    it.save_progress_info()
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
    def update_nse_files_to_new_host(host_ip, itasks):
        nse_files = list()
        flag_files = list()
        for it in itasks:
            for i in it.non_se_condition_files:
                nse_files.append(isnse.get_nse_condition_file_path(*it.k_m_n, i, it.lp_type, it.is_use_extended_rules))
                flag_files.append(isnse.get_transport_complete_flag_file(*it.k_m_n, i))
            if it.is_task_finish:
                isnse.get_task_early_terminate_flag_file(*it.k_m_n)
        isnse.transport_non_se_results(nse_files, [host_ip])
        isnse.transport_non_se_results(flag_files, [host_ip])

    @staticmethod
    def update_itask_running_info(itask, info):

        logging.error(("received stat info ", info))

        ne_iset_number = info[2]
        task_check_number = info[3]
        task_complete_number = info[4]
        valid_skip_number = info[5]
        task_running_time = info[6]
        nse_task_number = task_complete_number - (task_check_number + valid_skip_number)

        itask.set_task_complete_number(task_complete_number, ne_iset_number)
        itask.hierarchical_nse_condition_number[ne_iset_number] += nse_task_number
        itask.hierarchical_task_check_number[ne_iset_number] += task_check_number
        itask.hierarchical_task_valid_rule_skip_number[ne_iset_number] += valid_skip_number

        if task_running_time is not None:
            itask.set_task_running_time(task_running_time, ne_iset_number)

    @staticmethod
    def insert_found_conditions(itask, iconditions, is_se_condition=True):
        if is_se_condition:
            for ic in iconditions:
                itask.insert_se_condition(ic)
        else:
            for ic in iconditions:
                itask.insert_nse_condition(ic)

    @staticmethod
    def process_working_host_change(info, is_add=True):
        host_name = info[1]
        host_ip = info[2]
        if is_add:
            diff = 1
            msg_text = "task host %s:%s is online" % (host_name, host_ip)
        else:
            diff = -1
            msg_text = "task host %s:%s exit" % (host_name, host_ip)
        working_hosts_number_diff = (diff, host_ip)
        logging.info(msg_text)
        msg.send_message(msg_text)
        return working_hosts_number_diff

    @staticmethod
    def process_result_queue(cls, result_queue, isc_tasks):
        working_hosts_diff = (0, 0)
        result = result_queue.get()
        result_state = result[0]
        isc_task_id = result[1]

        if result_state == ITaskSignal.kill_signal:
            working_hosts_diff = cls.process_working_host_change(result, False)
        elif result_state == ITaskSignal.add_worker_signal:
            working_hosts_diff = cls.process_working_host_change(result, True)
        elif result_state == ITaskSignal.stat_signal:
            cls.update_itask_running_info(isc_tasks[isc_task_id], result)
        elif result_state == ITaskSignal.se_condition_signal:
            cls.insert_found_conditions(isc_tasks[isc_task_id], result[2], True)
        elif result_state == ITaskSignal.nse_condition_signal:
            cls.insert_found_conditions(isc_tasks[isc_task_id], result[2], False)

        return working_hosts_diff

    @staticmethod
    def check_itask_terminate_status(itask):
        if not itask.is_task_finish:
            terminate_flag = isnse.get_task_early_terminate_flag_file(*itask.k_m_n)
            if pathlib.Path(terminate_flag).exists():
                itask.is_task_finish = True
            else:
                itask.is_task_finish = False
        return itask.is_task_finish

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

            right_zone_iset_ids = set(copy.deepcopy(it.meta_data.search_space_iset_ids))
            left_zone_iset_ids = set(it.meta_data.search_i4_composed_iset_ids)

            max_left_zone_length = 12
            if len(left_zone_iset_ids) > max_left_zone_length:
                left_zone_iset_ids = list(left_zone_iset_ids)[0:max_left_zone_length]
                left_zone_iset_ids = set(left_zone_iset_ids)

            right_zone_iset_ids = right_zone_iset_ids.difference(left_zone_iset_ids)

            for ne_iset_number in range(min_ne, max_ne+1):
                if not cls.check_itask_terminate_status(it):
                    task_slices = CombinationSearchingSpaceSplitter.near_uniform_vandermonde_generator(
                        left_zone_iset_ids, right_zone_iset_ids, ne_iset_number)
                    ts_cnt = 0
                    for ts in task_slices:
                        new_ts = (set(ts[0]), set(ts[1]), ts[2])
                        task_queue.put((tid, new_ts))

                        ts_cnt += 1
                        if ts_cnt % 10000 == 0 and cls.check_itask_terminate_status(it):
                            break

        working_hosts_number = 5
        for i in range(working_hosts_number * 200):
            task_queue.put((ITaskSignal.kill_signal, -1))
        logging.info("all itasks has been dispatched")

    @staticmethod
    def init_task_slices_generator_pool(cls, isc_config_file):
        task_generator_pool = Pool(1)
        task_generator_pool.apply_async(cls.itask_slices_generator, args=(cls, isc_config_file))
        task_generator_pool.close()
        return task_generator_pool

    @staticmethod
    def init_pre_task_worker_pool(cls, isc_config_file, result_queue):
        worker_pool, result_queue, host_ip = FinalIConditionsSearchPreWorker.init_kmn_isc_task_workers(
            FinalIConditionsSearchPreWorker, isc_config_file, is_check_valid_rules=True, result_queue=result_queue)
        return worker_pool

    @staticmethod
    def init_kmn_isc_task_master_from_config(cls, isc_config_file="isets-tasks.json", sleep_time=30):
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

        pre_task_pool = cls.init_pre_task_worker_pool(cls, isc_config_file, result_queue)

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
        pre_task_pool.join()

        FinalIConditionsSearchPreWorker.send_worker_terminate_info(FinalIConditionsSearchPreWorker, localhost_ip, result_queue)

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

        msg_text = "isc tasks finish!"
        logging.info(msg_text)
        msg.send_message(msg=msg_text)

        for it in isc_tasks:
            it.task_finish()
            msg_text = it.get_final_detail_progress_info()
            logging.info(msg_text)
            msg.send_message(msg=msg_text)

        return isc_tasks


if __name__ == '__main__':
    pass
    