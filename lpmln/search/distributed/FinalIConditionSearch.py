
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-17 21:05
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : FinalIConditionSearch.py
"""

from multiprocessing import Pool, Queue
from multiprocessing.managers import BaseManager
import logging
from datetime import datetime
import time
import pathlib

from lpmln.iset.ISetConditionValidator import ISetConditionValidator
import lpmln.message.Messager as msg
import lpmln.config.GlobalConfig as cfg
from lpmln.itask.ITask import ITaskConfig
import lpmln.iset.ISetNonSEUtils as isnse
import lpmln.utils.SSHClient as ssh
import itertools
import copy
from lpmln.utils.CombinationSpaceUtils import CombinationSearchingSpaceSplitter


config = cfg.load_configuration()


class ITaskSignal:
    add_worker_signal = "--a-worker--"
    kill_signal = "--over--"
    stat_signal = "--stat--"
    se_condition_signal = "--cdt--"
    nse_condition_signal = "--nse--"
    nse_condition_ready_signal = "--nse-ready--"


class SearchMasterQueueManger(BaseManager):
    pass


class SearchWorkerQueueManger(BaseManager):
    pass


class SearchQueueManager:
    global_task_queue = Queue()
    global_ht_task_queue = Queue()
    global_result_queue = Queue()

    task_queue_func = "get_task_queue"
    result_queue_func = "get_result_queue"
    ht_task_queue_func = "get_ht_task_queue"

    @staticmethod
    def get_global_task_queue():
        return FinalIConditionsSearchMaster.global_task_queue

    @staticmethod
    def get_global_result_queue():
        return FinalIConditionsSearchMaster.global_result_queue

    @staticmethod
    def get_global_ht_task_queue():
        return FinalIConditionsSearchMaster.global_ht_task_queue

    @staticmethod
    def init_task_master_queue_manager():
        SearchMasterQueueManger.register(SearchQueueManager.task_queue_func, callable=SearchQueueManager.get_global_task_queue)
        SearchMasterQueueManger.register(SearchQueueManager.result_queue_func, callable=SearchQueueManager.get_global_result_queue)
        SearchMasterQueueManger.register(SearchQueueManager.ht_task_queue_func, callable=SearchQueueManager.get_global_ht_task_queue)
        manager = SearchMasterQueueManger(address=(config.task_host, config.task_host_port),
                                          authkey=bytes(config.task_host_key, encoding="utf-8"))
        manager.start()
        task_queue = manager.get_task_queue()
        ht_task_queue = manager.get_ht_task_queue()
        result_queue = manager.get_result_queue()
        return manager, task_queue, ht_task_queue, result_queue

    @staticmethod
    def init_task_worker_queue_manager():
        SearchWorkerQueueManger.register(SearchQueueManager.task_queue_func)
        SearchWorkerQueueManger.register(SearchQueueManager.result_queue_func)
        SearchWorkerQueueManger.register(SearchQueueManager.ht_task_queue_func)
        manager = SearchWorkerQueueManger(address=(config.task_host, config.task_host_port),
                                          authkey=bytes(config.task_host_key, encoding="utf-8"))
        manager.connect()
        task_queue = manager.get_task_queue()
        ht_task_queue = manager.get_ht_task_queue()
        result_queue = manager.get_result_queue()
        return manager, task_queue, ht_task_queue, result_queue


class FinalIConditionsSearchMaster:
    @staticmethod
    def dump_isc_task_results(itasks):
        msg_texts = []
        for it in itasks:
            it.dump_tmp_se_condition()
            msg_texts.append(it.get_progress_info())
        return msg_texts

    @staticmethod
    def send_itasks_progress_info(cls, isc_tasks, task_queue, working_hosts_number, is_all_task_dispatched=False):
        msg_texts = cls.dump_isc_task_results(isc_tasks)
        if is_all_task_dispatched:
            msg_text = "all isc tasks are discatched, DO NOT add new worker! isc tasks progress info, remain %d task hosts:  \n\t\t%s" \
                       % (working_hosts_number, "\n\t\t".join(msg_texts))
        else:
            msg_text = "isc tasks progress info, remain %d task hosts, %d task slices:  \n\t\t%s" % (
                working_hosts_number, task_queue.qsize(), "\n\t\t".join(msg_texts))
        logging.info(msg_text)
        msg.send_message(msg_text)

    @staticmethod
    def check_itasks_status(cls, itasks, host_ips, task_queue, working_host_number):
        is_finish = True
        for tid in range(len(itasks)):
            it = itasks[tid]
            if not it.is_task_finish:

                current_ne_number = it.working_ne_iset_numbers
                if it.is_no_new_se_condition():
                    isnse.create_and_send_task_early_terminate_flag_file(*it.k_m_n, current_ne_number, host_ips)
                    it.is_task_finish = True
                    continue

                task_complete = it.hierarchical_task_complete_number[current_ne_number]
                task_total = it.hierarchical_task_number[current_ne_number]
                if task_complete == task_total:
                    it.save_progress_info()
                    nse_file = it.flush_non_se_condition()
                    isnse.transport_non_se_results([nse_file], host_ips)
                    isnse.create_and_send_transport_complete_flag_file(*it.k_m_n, current_ne_number, host_ips)
                    cls.send_itasks_progress_info(cls, itasks, task_queue, working_host_number, False)

                    if current_ne_number < it.max_ne:
                        it.working_ne_iset_numbers += 1
                        is_finish = False
                    else:
                        it.is_task_finish = True
                else:
                    is_finish = False
        return is_finish

    @staticmethod
    def update_nse_files_to_new_host(host_ip, itasks):
        nse_files = list()
        complete_flags = list()
        for it in itasks:
            for i in it.non_se_condition_files:
                nse_files.append(isnse.get_nse_condition_file_path(*it.k_m_n, i, it.lp_type, it.is_use_extended_rules))
                complete_flags.append(isnse.get_transport_complete_flag_file(*it.k_m_n, i))
        isnse.transport_non_se_results(nse_files, [host_ip])
        isnse.transport_non_se_results(complete_flags, [host_ip])

    @staticmethod
    def update_itask_running_info(itask, info):
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

            right_zone_iset_ids = copy.deepcopy(it.meta_data.search_space_iset_ids)
            left_zone_iset_ids = it.meta_data.search_i4_composed_iset_ids

            max_left_zone_length = 12
            if len(left_zone_iset_ids) > max_left_zone_length:
                left_zone_iset_ids = list(left_zone_iset_ids)[0:max_left_zone_length]
                left_zone_iset_ids = set(left_zone_iset_ids)

            right_zone_iset_ids = right_zone_iset_ids.difference(left_zone_iset_ids)

            for i in range(min_ne, max_ne+1):
                ne_iset_number = i
                for left_iset_number in range(ne_iset_number + 1):
                    task_slices = CombinationSearchingSpaceSplitter.vandermonde_generator(left_zone_iset_ids, right_zone_iset_ids, ne_iset_number)
                    for ts in task_slices:
                        task_queue.put((tid, ts))

        working_hosts_number = 5
        for i in range(working_hosts_number * 200):
            task_queue.put((ITaskSignal.kill_signal, -1))
        logging.info("all itasks has been dispatched")

    @staticmethod
    def init_task_slices_generator_pool(cls, isc_config_file):
        task_generator_pool = Pool(2)
        task_generator_pool.apply_async(cls.itask_slices_generator, args=(cls, isc_config_file))
        task_generator_pool.close()
        return task_generator_pool

    @staticmethod
    def init_kmn_isc_task_master_from_config(cls, isc_config_file="isets-tasks.json", sleep_time=30):
        manager, task_queue, ht_task_queue, result_queue = \
            SearchQueueManager.init_task_master_queue_manager()

        localhost_ip = ssh.get_host_ip()
        ts_generator_pool = cls.init_task_slices_generator_pool(cls, isc_config_file)

        working_hosts_number = 0
        msg_text = "isc task master start, load isc tasks from %s" % (isc_config_file)
        logging.info(msg_text)
        msg.send_message(msg_text)

        isc_tasks_cfg = ITaskConfig(isc_config_file)
        isc_tasks = isc_tasks_cfg.isc_tasks
        for itask in isc_tasks:
            itask.init_task_numbers()

        sleep_cnt = 0
        online_hosts = set()

        progress_msg_cnt = 10
        task_finish = False
        print_loop = 100000
        print_cnt = 0
        while not task_finish:
            print_cnt += 1

            if print_cnt == print_loop:
                cls.send_itasks_progress_info(cls, isc_tasks, task_queue, working_hosts_number, False)
                sleep_cnt = 0
                print_cnt = 0

            if sleep_cnt == progress_msg_cnt:
                cls.send_itasks_progress_info(cls, isc_tasks, task_queue, working_hosts_number, False)
                sleep_cnt = 0
                print_cnt = 0

            task_finish = cls.check_itasks_status(cls, isc_tasks, online_hosts, task_queue, working_hosts_number)
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
                cls.send_itasks_progress_info(cls, isc_tasks, task_queue, working_hosts_number, True)
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


class FinalIConditionsSearchWorker:
    @staticmethod
    def init_worker_host_nse_envs(isc_tasks):
        for it in isc_tasks:
            isnse.clear_transport_complete_flag_files(*it.k_m_n, it.min_ne, it.max_ne)
            isnse.create_transport_complete_flag_file(*it.k_m_n, 1)
            nse_1_path = isnse.get_nse_condition_file_path(*it.k_m_n, 1, it.lp_type,
                                                           it.is_use_extended_rules)
            pathlib.Path(nse_1_path).touch()
            isnse.clear_task_terminate_flag_files(*it.k_m_n)

    @staticmethod
    def check_contain_nse_subparts(iset_ids, itask):
        for nse in itask.non_se_conditions:
            if nse.issubset(iset_ids):
                return True
        return False

    @staticmethod
    def check_contain_i4_isets(iset_ids, itask):
        min_i4_iset_tuples = itask.meta_data.minmal_i4_isets_tuples
        for i4_isets in min_i4_iset_tuples:
            if i4_isets.issubset(iset_ids):
                return True
        return False

    @staticmethod
    def join_list_data(data):
        data = [str(s) for s in data]
        return ",".join(data)

    @staticmethod
    def task_worker_load_nse_conditions(itask, nse_iset_number):
        sleep_cnt = 0
        for i in range(1, nse_iset_number + 1):
            first_print_debug_log = True
            if i not in itask.loaded_non_se_condition_files:
                complete_flag = isnse.get_transport_complete_flag_file(*itask.k_m_n, i)
                transport_complete = False
                while not transport_complete:
                    if first_print_debug_log:
                        logging.info("waiting for transport complete file %s" % complete_flag)
                        first_print_debug_log = False
                    if sleep_cnt == 30:
                        return False

                    if pathlib.Path(complete_flag).exists():
                        transport_complete = True
                    else:
                        sleep_cnt += 1
                        time.sleep(2)
                nse_conditions = isnse.load_kmn_non_se_results(*itask.k_m_n, i, itask.lp_type,
                                                               itask.is_use_extended_rules)
                itask.non_se_conditions.extend(nse_conditions)
                itask.loaded_non_se_condition_files.add(i)
        return True

    @staticmethod
    def init_kmn_isc_task_workers(cls, isc_config_file="isets-tasks.json", is_check_valid_rules=True):
        payload = config.worker_payload
        worker_pool = Pool(payload)
        pathlib.Path(config.task_host_lock_file).touch()
        SearchWorkerQueueManger.register("get_task_queue")
        SearchWorkerQueueManger.register("get_result_queue")
        manager = SearchWorkerQueueManger(address=(config.task_host, config.task_host_port),
                                                           authkey=bytes(config.task_host_key, encoding="utf-8"))
        manager.connect()
        result_queue = manager.get_result_queue()
        host_ip = ssh.get_host_ip()

        result_queue.put((ITaskSignal.add_worker_signal, config.worker_host_name, host_ip))
        logging.info("task worker host %s start ..." % config.worker_host_name)

        # 初始化不等价条件目录文件
        isc_tasks = ITaskConfig(isc_config_file)
        isc_tasks = isc_tasks.isc_tasks
        cls.init_worker_host_nse_envs(isc_tasks)

        for i in range(payload):
            worker_pool.apply_async(cls.kmn_isc_task_worker,
                                    args=(cls, isc_config_file, i + 1, is_check_valid_rules))
        worker_pool.close()
        worker_pool.join()
        # if pathlib.Path(task_worker_host_lock_file).exists():
        result_queue.put((ITaskSignal.kill_signal, config.worker_host_name, host_ip))
        logging.info("task worker host %s send kill signal ..." % config.worker_host_name)
        logging.info("task worker host %s exit ..." % config.worker_host_name)

    @staticmethod
    def process_kmn_itask_slice(cls, itask, task_slice, task_name, result_queue, is_check_valid_rules):
        time_fmt = "%Y-%m-%d %H:%M:%S.%f"

        itask_id = task_slice[0]
        task_params = task_slice[1]

        ne_iset_number = task_params[0]
        left_zone_isets = task_params[1]
        left_iset_ids = task_params[2]

        task_terminate_flag = isnse.get_task_early_terminate_flag_file(*itask.k_m_n)
        nse_iset_number = ne_iset_number - 1

        if nse_iset_number not in itask.loaded_non_se_condition_files:
            load_complete = False
            while not load_complete:
                if pathlib.Path(task_terminate_flag).exists():
                    itask.is_task_finish = True
                    break
                load_complete = cls.task_worker_load_nse_conditions(itask, nse_iset_number)

        if itask.is_task_finish:
            return True

        start_time = datetime.now()
        start_time_str = start_time.strftime(time_fmt)[:-3]

        right_zone_isets = set(itask.meta_data.search_space_iset_ids)
        right_zone_isets = right_zone_isets.difference(left_zone_isets)

        right_iset_number = ne_iset_number - len(left_iset_ids)

        msg_text = "%s: %d-%d-%d isc task: nonempty iset number %d, left zone length %d, left isets {%s}" % (
            task_name, *itask.k_m_n, ne_iset_number, len(left_zone_isets), cls.join_list_data(left_iset_ids))
        logging.info(msg_text)

        nse_cdt_cnt, check_cdt_cnt, task_number, semi_valid_skip_cnt, se_conditions_cache, nse_conditions_cache = \
            cls.search_task_slice(cls, itask, left_iset_ids, right_zone_isets, right_iset_number, is_check_valid_rules)

        # for sec in se_conditions_cache:
        if len(se_conditions_cache) > 0:
            result_queue.put((ITaskSignal.se_condition_signal, itask_id, se_conditions_cache))

        if len(nse_conditions_cache) > 0:
            result_queue.put((ITaskSignal.nse_condition_signal, itask_id, nse_conditions_cache))

        end_time = datetime.now()
        end_time_str = end_time.strftime(time_fmt)[:-3]

        msg_text = "%s: %d-%d-%d isc task: nonempty iset number %d, left zone length %d, left isets {%s}, start time %s, end time %s, find %d se conditions (no semi-valid rules), find %d non-se conditions" % (
            task_name, *itask.k_m_n, ne_iset_number, len(left_zone_isets), cls.join_list_data(left_iset_ids),
            start_time_str, end_time_str, len(se_conditions_cache), nse_cdt_cnt)

        logging.info(msg_text)
        result_queue.put(
            (ITaskSignal.stat_signal, itask_id, ne_iset_number, check_cdt_cnt, task_number, semi_valid_skip_cnt,
             (start_time, end_time)))

        return True

    @staticmethod
    def search_task_slice(cls, itask, left_iset_ids, right_zone_isets, right_iset_number, is_check_valid_rules):
        semi_valid_skip_cnt = 0
        check_cdt_cnt = 0
        task_number = 0
        nse_cdt_cnt = 0

        se_conditions_cache = list()
        nse_conditions_cache = list()
        validator = ISetConditionValidator(lp_type=itask.lp_type, is_use_extended_rules=itask.is_use_extended_rules)

        task_iter = itertools.combinations(right_zone_isets, right_iset_number)

        for right_ti in task_iter:
            non_ne_ids = list()
            non_ne_ids.extend(left_iset_ids)
            non_ne_ids.extend(list(right_ti))
            non_ne_ids = set(non_ne_ids)
            task_number += 1

            if cls.check_contain_nse_subparts(non_ne_ids, itask):
                nse_cdt_cnt += 1
                continue

            if not cls.check_contain_i4_isets(non_ne_ids, itask):
                semi_valid_skip_cnt += 1
                continue

            check_cdt_cnt += 1
            is_contain_valid_rule, is_strongly_equivalent, condition = \
                validator.validate_kmn_extended_iset_condition_from_non_emtpy_iset_ids_return_icondition_obj(
                    non_ne_ids, *itask.k_m_n, is_check_valid_rule=is_check_valid_rules)

            # if not is_contain_valid_rule:
            if is_strongly_equivalent:
                se_conditions_cache.append(condition)
            else:
                nse_cdt_cnt += 1
                nse_conditions_cache.append(condition)

        return nse_cdt_cnt, check_cdt_cnt, task_number, semi_valid_skip_cnt, se_conditions_cache, nse_conditions_cache



    @staticmethod
    def kmn_isc_task_worker(cls, isc_config_file="isets-tasks.json", worker_id=1, is_check_valid_rules=True):

        SearchWorkerQueueManger.register("get_task_queue")
        SearchWorkerQueueManger.register("get_result_queue")
        manager = SearchWorkerQueueManger(address=(config.task_host, config.task_host_port),
                                                           authkey=bytes(config.task_host_key, encoding="utf-8"))

        is_check_valid_rules = False

        manager.connect()
        task_queue = manager.get_task_queue()
        result_queue = manager.get_result_queue()

        worker_name = "worker-%d" % worker_id

        worker_host_name = config.worker_host_name
        msg_text = "task worker %s start!" % (worker_name)
        logging.info(msg_text)
        isc_tasks = ITaskConfig(isc_config_file)
        isc_tasks = isc_tasks.isc_tasks
        processed_task_slices_number = 0

        for itask in isc_tasks:
            itask.loaded_non_se_condition_files.add(1)
            itask.loaded_non_se_condition_files.add(0)

        first_print_debug_log = True
        while True:
            if not pathlib.Path(config.task_host_lock_file).exists():
                break

            if task_queue.empty():
                if first_print_debug_log:
                    logging.info("waiting for isc task slices")
                    first_print_debug_log = False
                time.sleep(2)
                continue

            first_print_debug_log = True

            task_slice = task_queue.get()
            if task_slice[0] == ITaskSignal.kill_signal:
                msg_text = "%s:%s isc task worker terminate ..." % (worker_host_name, worker_name)
                logging.info(msg_text)
                break

            itask = isc_tasks[task_slice[0]]
            task_name = worker_name + ("-task-%d" % processed_task_slices_number)
            cls.process_kmn_itask_slice(cls, itask, task_slice, task_name, result_queue, is_check_valid_rules)
            processed_task_slices_number += 1

        logging.info("%s processes %d isc task slices" % (worker_name, processed_task_slices_number))


def init_task_master(isc_config_file="isets-tasks.json", sleep_time=30):
    FinalIConditionsSearchMaster.init_kmn_isc_task_master_from_config(FinalIConditionsSearchMaster, isc_config_file, sleep_time)


def init_task_worker(isc_config_file="isets-tasks.json", is_check_valid_rules=True):
    FinalIConditionsSearchWorker.init_kmn_isc_task_workers(FinalIConditionsSearchWorker, isc_config_file, is_check_valid_rules)


if __name__ == '__main__':
    pass
    