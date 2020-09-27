
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-27 10:07
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : RawIConditionSearchMaster.py
"""

from lpmln.search.distributed.final.FinalSearchBase import ITaskSignal, SearchQueueManager
from lpmln.search.distributed.final.FinalSearchMaster import FinalIConditionsSearchMaster
from multiprocessing import Pool
import logging
import time
import lpmln.message.Messager as msg
import lpmln.config.GlobalConfig as cfg
from lpmln.itask.ITask import ITaskConfig
import lpmln.iset.ISetNonSEUtils as isnse
import lpmln.utils.SSHClient as ssh
import copy
from lpmln.utils.CombinationSpaceUtils import CombinationSearchingSpaceSplitter
from lpmln.utils.counter.CombinaryCounter import CombinaryCounter
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

            right_zone_iset_ids = set(copy.deepcopy(it.meta_data.search_space_iset_ids))
            left_zone_iset_ids = set(it.meta_data.search_i4_composed_iset_ids)

            max_left_zone_length = 12
            if len(left_zone_iset_ids) > max_left_zone_length:
                left_zone_iset_ids = list(left_zone_iset_ids)[0:max_left_zone_length]
                left_zone_iset_ids = set(left_zone_iset_ids)

            right_zone_iset_ids = right_zone_iset_ids.difference(left_zone_iset_ids)

            for ne_iset_number in range(min_ne, max_ne + 1):
                task_slices = CombinationSearchingSpaceSplitter.near_uniform_vandermonde_generator(
                    left_zone_iset_ids, right_zone_iset_ids, ne_iset_number)

                for ts in task_slices:
                    task_queue.put((tid, ts))

        working_hosts_number = 5
        for i in range(working_hosts_number * 200):
            task_queue.put((ITaskSignal.kill_signal, -1))
        logging.info("all itasks has been dispatched")




    @staticmethod
    def init_kmn_isc_task_master_from_config(cls, isc_config_file="isets-tasks.json", sleep_time=30):
        manager, task_queue, ht_task_queue, result_queue = \
            SearchQueueManager.init_task_master_queue_manager()
        manager_tuple = (manager, task_queue, ht_task_queue, result_queue)
        localhost_ip = ssh.get_host_ip()
        ts_generator_pool = cls.init_task_slices_generator_pool(cls, isc_config_file)
        working_hosts_number = 0

        isc_tasks_cfg = ITaskConfig(isc_config_file)
        isc_tasks = isc_tasks_cfg.isc_tasks
        for itask in isc_tasks:
            itask.init_task_numbers()

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

        msg_texts = []
        attached_files = []
        for it in isc_tasks:
            it.task_finish()
            msg_texts.append(it.get_final_detail_progress_info())
            attached_files.append(it.result_file)

        msg_text = "isc tasks finish! \n\t\t%s" % "\n\t\t".join(msg_texts)
        logging.info(msg_text)
        msg.send_message(msg=msg_text, attached_files=attached_files)
        return isc_tasks


if __name__ == '__main__':
    pass
    