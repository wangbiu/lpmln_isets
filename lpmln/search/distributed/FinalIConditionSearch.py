
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
from lpmln.utils.counter.CombinaryCounter import CombinaryCounter
import lpmln.iset.ISetCompositionUtils as iscm



config = cfg.load_configuration()


class ITaskSignal:
    add_worker_signal = "--a-worker--"
    kill_signal = "--over--"
    stat_signal = "--stat--"
    se_condition_signal = "--se-cdt--"
    nse_condition_signal = "--nse-cdt--"
    update_semi_valid_skip_number = "--update-sv-skip--"
    update_nse_skip_number = "--update-nse-skip--"


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
        return SearchQueueManager.global_task_queue

    @staticmethod
    def get_global_result_queue():
        return SearchQueueManager.global_result_queue

    @staticmethod
    def get_global_ht_task_queue():
        return SearchQueueManager.global_ht_task_queue

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
                    cls.send_itasks_progress_info(cls, itasks, manager_tuple, working_host_number, False)

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

            right_zone_iset_ids = set(copy.deepcopy(it.meta_data.search_space_iset_ids))
            left_zone_iset_ids = set(it.meta_data.search_i4_composed_iset_ids)

            max_left_zone_length = 12
            if len(left_zone_iset_ids) > max_left_zone_length:
                left_zone_iset_ids = list(left_zone_iset_ids)[0:max_left_zone_length]
                left_zone_iset_ids = set(left_zone_iset_ids)

            right_zone_iset_ids = right_zone_iset_ids.difference(left_zone_iset_ids)

            for ne_iset_number in range(min_ne, max_ne+1):
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
    def join_list_data(data):
        data = [str(s) for s in data]
        return ",".join(data)

    @staticmethod
    def get_and_process_ht_task_queue(cls, itasks, worker_info, manager_tuple):
        ht_task_queue = manager_tuple[2]
        result_queue = manager_tuple[3]
        processed_ht_task_slices_number = worker_info[3]
        is_ht_task_queue_empty = True

        while True:
            if ht_task_queue.empty():
                break

            is_ht_task_queue_empty = False
            ts = ht_task_queue.get()
            start_time = datetime.now()
            itask_id = ts[0]
            processed_ht_task_slices_number += 1
            ne_iset_number, task_check_number, se_conditions_cache, nse_conditions_cache = \
                cls.search_ht_task_slice(cls, itasks[itask_id], ts[1])
            end_time = datetime.now()

            if len(se_conditions_cache) > 0:
                result_queue.put((ITaskSignal.se_condition_signal, itask_id, se_conditions_cache))

            if len(nse_conditions_cache) > 0:
                result_queue.put((ITaskSignal.nse_condition_signal, itask_id, nse_conditions_cache))

            result_tuple = (ITaskSignal.stat_signal, itask_id, ne_iset_number, task_check_number,
                            task_check_number, 0, (start_time, end_time))
            result_queue.put(result_tuple)

        return is_ht_task_queue_empty, processed_ht_task_slices_number


    @staticmethod
    def search_ht_task_slice(cls, itask, task_slice):
        task_check_number = 0
        nse_cdt_cnt = 0
        is_check_valid_rules = False
        left_iset_ids = list(task_slice[0])

        se_conditions_cache = list()
        nse_conditions_cache = list()
        validator = ISetConditionValidator(lp_type=itask.lp_type, is_use_extended_rules=itask.is_use_extended_rules)

        task_iter = itertools.combinations(task_slice[1], task_slice[2])
        ne_iset_number = 0

        for right_ti in task_iter:
            non_ne_ids = list()
            non_ne_ids.extend(left_iset_ids)
            non_ne_ids.extend(list(right_ti))
            non_ne_ids = set(non_ne_ids)
            ne_iset_number = len(non_ne_ids)

            task_check_number += 1
            is_contain_valid_rule, is_strongly_equivalent, condition = \
                validator.validate_kmn_extended_iset_condition_from_non_emtpy_iset_ids_return_icondition_obj(
                    non_ne_ids, *itask.k_m_n, is_check_valid_rule=is_check_valid_rules)

            if is_strongly_equivalent:
                se_conditions_cache.append(condition)
            else:
                nse_cdt_cnt += 1
                nse_conditions_cache.append(condition)

        return ne_iset_number, task_check_number, se_conditions_cache, nse_conditions_cache

    @staticmethod
    def get_and_process_task_queue(cls, itasks, task_slice_cache, worker_info, manager_tuple):
        """
        :param cls:
        :param worker_id:
        :param itasks:
        :param manager_tuple: manager_tuple = (manager, task_queue, ht_task_queue, result_queue)
        :return:
        """

        task_queue = manager_tuple[1]
        is_task_queue_complete = False
        is_task_queue_empty = True
        processed_task_slices_number = worker_info[2]

        while True:
            if task_slice_cache is None:
                if task_queue.empty():
                    break
                task_slice_cache = task_queue.get()
                is_task_queue_empty = False

            logging.debug(task_slice_cache)

            itask_id = task_slice_cache[0]


            if itask_id == ITaskSignal.kill_signal:
                is_task_queue_complete = True
                break

            processed_task_slices_number += 1
            itask = itasks[itask_id]

            if itask.is_task_finish:
                task_slice_cache = None
                continue

            load_nse_complete = cls.task_worker_load_nse_conditions(itask, task_slice_cache[1])
            if not load_nse_complete:
                break

            cls.process_task_slice(cls, itask_id, itask, task_slice_cache[1], manager_tuple)
            task_slice_cache = None

        return is_task_queue_complete, is_task_queue_empty, processed_task_slices_number, task_slice_cache


    @staticmethod
    def process_task_slice(cls, itask_id, itask, task_slice, manager_tuple):
        result_queue = manager_tuple[3]
        ht_task_queue = manager_tuple[2]
        no_sv_task_slices = cls.process_semi_valid_task_slices(cls, itask_id, itask, task_slice, result_queue)
        ht_check_task_slices = list()
        for ts in no_sv_task_slices:
            ht_slices = cls.process_nse_subpart_task_slices(cls, itask_id, itask, ts, result_queue)
            ht_check_task_slices.extend(ht_slices)

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
    def process_nse_subpart_task_slices(cls, itask_id, itask, task_slice, result_queue):
        skip_number = 0
        processed_task_slices = [task_slice]
        original_left_isets = set(task_slice[0])
        ne_iset_number = len(original_left_isets) + task_slice[2]

        for nse in itask.non_se_conditions:
            nse_new_task_slices = list()
            # print(nse)
            for ts in processed_task_slices:
                # print(ts)
                left_iset_ids = set(ts[0])
                right_zone_isets = set(ts[1])
                nse_remained_isets = nse.difference(left_iset_ids)

                yang_task_slices = CombinationSearchingSpaceSplitter.yanghui_split(
                    right_zone_isets, ts[2], nse_remained_isets)

                if nse_remained_isets.issubset(right_zone_isets):
                    skip_ts = yang_task_slices[-1]
                    yang_task_slices = yang_task_slices[0:-1]
                    skip_number += CombinaryCounter.compute_comb(len(skip_ts[1]), skip_ts[2])

                for yts in yang_task_slices:
                    for iset in left_iset_ids:
                        yts[0].add(iset)
                    # print("\t\t", yts)

                nse_new_task_slices.extend(yang_task_slices)
            processed_task_slices = nse_new_task_slices


        if skip_number > 0:
            result_item = (ITaskSignal.stat_signal, itask_id, ne_iset_number, 0, skip_number, 0, None)
            result_queue.put(result_item)

        return processed_task_slices

    @staticmethod
    def process_semi_valid_task_slices(cls, itask_id, itask, task_slice, result_queue):
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

        if skip_number > 0:
            result_tuple = (ITaskSignal.stat_signal, itask_id, ne_iset_number, 0, skip_number, skip_number, None)
            result_queue.put(result_tuple)

        return new_task_slices

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

        worker_name = "worker-%d" % worker_id
        worker_host_name = config.worker_host_name
        processed_task_slices_number = 0
        processed_ht_task_slices_number = 0
        worker_info = [worker_host_name, worker_name, processed_task_slices_number, processed_ht_task_slices_number]

        msg_text = "task worker %s start!" % (worker_name)
        logging.info(msg_text)
        isc_tasks = ITaskConfig(isc_config_file)
        isc_tasks = isc_tasks.isc_tasks

        for itask in isc_tasks:
            itask.loaded_non_se_condition_files.add(1)
            itask.loaded_non_se_condition_files.add(0)

        task_slice_cache = None
        is_task_queue_finish = False
        is_kill_task_worker = False
        is_task_queue_empty = False

        while True:
            if not pathlib.Path(config.task_host_lock_file).exists():
                break

            if not is_task_queue_finish:
                is_task_queue_finish, is_task_queue_empty, processed_task_slices_number, task_slice_cache = \
                    cls.get_and_process_task_queue(cls, isc_tasks, task_slice_cache, worker_info, manager_tuple)

                if is_task_queue_finish:
                    msg_text = "%s:%s isc task queue finish, process %d task slices %d ht task slices ..." % (
                        worker_info[0], worker_info[1], worker_info[2], worker_info[3])
                    logging.info(msg_text)
                    is_task_queue_empty = False
                else:
                    if not is_task_queue_empty and task_slice_cache is not None:
                        ne_iset_number = len(task_slice_cache[1][0]) + task_slice_cache[1][2] - 1
                        msg_text = "%s waiting for nse condition complete file: %d" % (worker_name, ne_iset_number)
                        logging.info(msg_text)

            is_ht_task_queue_empty, processed_ht_task_slices_number = \
                cls.get_and_process_ht_task_queue(cls, isc_tasks, worker_info, manager_tuple)

            worker_info[2] = processed_task_slices_number
            worker_info[3] = processed_ht_task_slices_number

            if not is_task_queue_empty or not is_ht_task_queue_empty:
                msg_text = "%s:%s isc task worker process %d task slices %d ht task slices ..." % (
                    worker_info[0], worker_info[1], worker_info[2], worker_info[3])
                logging.info(msg_text)

                logging.info("%s waiting for task queue and ht task queue ..." % worker_name)

            is_task_finish = cls.check_itasks_finish_status(isc_tasks)
            if is_task_finish:
                break

            time.sleep(1)

        logging.info("%s processes %d isc task slices %d ht itask slices" % (
            worker_name, processed_task_slices_number, processed_ht_task_slices_number))

    @staticmethod
    def check_itasks_finish_status(itasks):
        task_finish = True
        for itask in itasks:
            if not itask.is_task_finish:
                task_terminate_flag = isnse.get_task_early_terminate_flag_file(*itask.k_m_n)
                if pathlib.Path(task_terminate_flag).exists():
                    itask.is_task_finish = True
                else:
                    task_finish = False
                    break
        return task_finish

    @staticmethod
    def init_kmn_isc_task_workers(cls, isc_config_file="isets-tasks.json", is_check_valid_rules=True):
        payload = config.worker_payload
        worker_pool = Pool(payload)
        pathlib.Path(config.task_host_lock_file).touch()

        manager, task_queue, ht_task_queue, result_queue = \
            SearchQueueManager.init_task_worker_queue_manager()

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


def init_task_master(isc_config_file="isets-tasks.json", sleep_time=30):
    FinalIConditionsSearchMaster.init_kmn_isc_task_master_from_config(FinalIConditionsSearchMaster, isc_config_file, sleep_time)


def init_task_worker(isc_config_file="isets-tasks.json", is_check_valid_rules=True):
    FinalIConditionsSearchWorker.init_kmn_isc_task_workers(FinalIConditionsSearchWorker, isc_config_file, is_check_valid_rules)


if __name__ == '__main__':
    pass
    