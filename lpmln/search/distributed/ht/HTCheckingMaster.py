
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-27 9:46
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : HTCheckingMaster.py
"""

from lpmln.search.distributed.final.FinalSearchMaster import FinalIConditionsSearchMaster
from lpmln.search.distributed.ht.HTCheckingWorker import HTCheckingWorker
import logging
import time
import lpmln.message.Messager as msg
import lpmln.config.GlobalConfig as cfg
from lpmln.itask.ITask import ITaskConfig
import lpmln.utils.SSHClient as ssh
import os
from lpmln.search.distributed.final.FinalSearchBase import SearchQueueManager, ITaskSignal
config = cfg.load_configuration()


class HTCheckingMaster(FinalIConditionsSearchMaster):
    @staticmethod
    def process_result_queue(cls, result_queue, isc_tasks):
        working_hosts_diff = (0, 0)
        result_cnt = 1000000

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
                cls.update_itask_running_info(isc_tasks[isc_task_id], result)
            elif result_state == ITaskSignal.se_condition_signal:
                cls.insert_found_conditions(isc_tasks[isc_task_id], result[2], True)
            elif result_state == ITaskSignal.nse_condition_signal:
                cls.insert_found_conditions(isc_tasks[isc_task_id], result[2], False)

        return working_hosts_diff

    @staticmethod
    def update_itask_running_info(itask, info):
        task_complete_number = info[2]
        task_running_time = info[3]

        itask.set_task_complete_number(task_complete_number, 1)

        if task_running_time is not None:
            itask.set_task_running_time(task_running_time, 1)

    @staticmethod
    def dump_isc_task_results(itasks):
        msg_texts = []
        for it in itasks:
            it.dump_tmp_se_condition_saving_mem()
            total_number = it.ht_task_number
            task_complete_number = it.task_complete_number
            se_number = it.se_condition_number
            nse_number = task_complete_number - se_number
            if it.task_end_time is None or it.task_start_time is None:
                running_time = "unknown"
            else:
                running_time = it.task_end_time - it.task_start_time
            msg_text = ":rocket: %s (running time: %s): total tasks: %d, complete tasks: %d, find %d se conditions, %d nse condition" % (
                it.task_flag, str(running_time), total_number, task_complete_number, se_number, nse_number)
            msg_texts.append(msg_text)
        return msg_texts

    @staticmethod
    def insert_found_conditions(itask, iconditions, is_se_condition=True):
        if is_se_condition:
            for ic in iconditions:
                itask.insert_se_condition(ic)
        else:
            for ic in iconditions:
                msg_text = "%d-%d-%d nse condition: %s" % (*itask.k_m_n, str(ic))
                logging.error(msg_text)
                msg.send_message(msg_text)

    @staticmethod
    def check_itasks_status(cls, itasks, host_ips, manager_tuple, working_host_number):
        is_finish = True
        for tid in range(len(itasks)):
            it = itasks[tid]
            if not it.is_task_finish:
                task_complete = it.task_complete_number
                task_total_number = it.ht_task_number

                if task_complete == task_total_number:
                    it.is_task_finish = True
                else:
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
            ht_task_number = it.ht_task_number
            batch_size = 100
            ht_task_slices = [i for i in range(0, ht_task_number + 1, batch_size)]
            if ht_task_slices[-1] < ht_task_number:
                ht_task_slices.append(ht_task_number)

            for i in range(1, len(ht_task_slices)):
                ts = (tid, (ht_task_slices[i-1], ht_task_slices[i]))
                ht_task_queue.put(ts)

        working_hosts_number = 5
        for i in range(working_hosts_number * 200):
            ht_task_queue.put((ITaskSignal.kill_signal, -1))
        logging.info("all itasks has been dispatched")
        msg.send_message("all itasks has been dispatched")

    @staticmethod
    def init_pre_task_worker_pool(cls, isc_config_file, result_queue):
        worker_pool, result_queue, host_ip = HTCheckingWorker.inner_init_kmn_isc_task_workers(
            HTCheckingWorker, isc_config_file, is_check_valid_rules=False, result_queue=result_queue)
        return worker_pool

    @staticmethod
    def init_kmn_isc_task_master_from_config(cls, isc_config_file="isets-tasks.json", sleep_time=30):
        manager, task_queue, ht_task_queue, result_queue = \
            SearchQueueManager.init_task_master_queue_manager()
        manager_tuple = (manager, task_queue, ht_task_queue, result_queue)
        localhost_ip = ssh.get_host_ip()
        ts_generator_pool = cls.init_task_slices_generator_pool(cls, isc_config_file)
        ht_pool = cls.init_pre_task_worker_pool(cls, isc_config_file, result_queue)
        working_hosts_number = 0
        # ht_checking_results = list()

        isc_tasks_cfg = ITaskConfig(isc_config_file)
        isc_tasks = isc_tasks_cfg.isc_tasks
        for itask in isc_tasks:
            itask.init_task_numbers()
            if os.path.exists(itask.result_file):
                os.remove(itask.result_file)
        #     ht_checking_results.append(list())

        msg_text = "isc task master start, load %d isc tasks from %s" % (len(isc_tasks), isc_config_file)
        logging.info(msg_text)
        msg.send_message(msg_text)

        sleep_cnt = 0
        online_hosts = set()

        progress_msg_cnt = 10
        task_finish = False
        print_loop = 100
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
            elif whn_number == -1:
                if host_ip != localhost_ip:
                    online_hosts.remove(host_ip)

        ts_generator_pool.join()
        ht_pool.join()
        HTCheckingWorker.send_worker_terminate_info(HTCheckingWorker, localhost_ip, result_queue)

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
            it.dump_tmp_se_condition_saving_mem()
            msg_text = it.get_final_detail_progress_info()
            logging.info(msg_text)
            msg.send_message(msg=msg_text)

        return isc_tasks


if __name__ == '__main__':
    pass
    