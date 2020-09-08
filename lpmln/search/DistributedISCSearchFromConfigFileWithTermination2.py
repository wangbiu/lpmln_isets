
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-06 21:08
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : DistributedISCSearchFromConfigFileWithTermination.py
"""


from multiprocessing import Pool, Queue
from multiprocessing.managers import BaseManager
import logging
from datetime import datetime
import time
import pathlib
import copy
from lpmln.utils.counter.CombinaryCounter import CombinaryCounter

from lpmln.iset.ISetConditionValidator import ISetConditionValidator
import lpmln.message.Messager as msg
import lpmln.config.GlobalConfig as cfg
from lpmln.config.ITasksWithTerminationConfig import ISCTaskConfig
import lpmln.iset.ISetNonSEUtils as isnse
import lpmln.utils.SSHClient as ssh
import lpmln.search.misc.ISCSearchingSlicesGenerator as isg

config = cfg.load_configuration()

add_worker_signal = "--a-worker--"
kill_signal = "--over--"
stat_signal = "--stat--"
se_condition_signal = "--cdt--"
nse_condition_signal = "--nse--"
nse_condition_ready_signal = "--nse-ready--"



class ISCFileTaskTerminationMasterQueueManager(BaseManager):
    pass


class ISCFileTaskTerminationWorkerQueueManager(BaseManager):
    pass


global_task_queue = Queue()


def get_task_queue():
    return global_task_queue


global_result_queue = Queue()


def get_result_queue():
    return global_result_queue


def dump_isc_task_results(isc_tasks):
    msg_texts = []
    for it in isc_tasks:
        it.dump_tmp_se_condition()
        msg_texts.append(it.get_progress_info())
    return msg_texts


def process_result_queue(result_queue, isc_tasks):
    working_hosts_number_diff = (0, 0)
    result = result_queue.get()
    result_state = result[0]
    isc_task_id = result[1]

    if result_state == kill_signal:
        host_ip = result[2]
        working_hosts_number_diff = (-1, host_ip)
        msg_text = "task host %s:%s exit" % (isc_task_id, host_ip)
        logging.info(msg_text)
        msg.send_message(msg_text)
    elif result_state == add_worker_signal:
        host_ip = result[2]
        msg_text = "task host %s:%s is online" % (isc_task_id, host_ip)
        working_hosts_number_diff = (1, host_ip)
        logging.info(msg_text)
        msg.send_message(msg_text)
    elif result_state == stat_signal:
        ne_iset_number = result[2]
        task_check_number = result[3]
        task_complete_number = result[4]
        nse_task_number = task_complete_number - task_check_number
        task_running_time = result[5]
        isc_tasks[isc_task_id].set_task_complete_number(task_complete_number, ne_iset_number)
        isc_tasks[isc_task_id].incremental_nse_condition_number[ne_iset_number] += nse_task_number
        isc_tasks[isc_task_id].incremental_task_check_number[ne_iset_number] += task_check_number
        isc_tasks[isc_task_id].set_task_running_time(task_running_time)
    elif result_state == se_condition_signal:
        iconditions = result[2]
        for ic in iconditions:
            isc_tasks[isc_task_id].insert_se_condition(ic)
    elif result_state == nse_condition_signal:
        iconditions = result[2]
        for ic in iconditions:
            isc_tasks[isc_task_id].insert_nse_condition(ic)

    return working_hosts_number_diff


def check_has_new_itask_items(itasks):
    for it in itasks:
        if it.has_new_itask_items():
            return True
    return False


def check_itasks_status(itasks, host_ips, task_queue, working_host_number):
    is_finish = True
    for tid in range(len(itasks)):
        it = itasks[tid]
        if not it.is_task_finish:
            current_ne_number = it.working_ne_iset_numbers
            task_complete = it.incremental_task_complete_number[current_ne_number]
            task_total = it.incremental_task_number[current_ne_number]
            if task_complete == task_total:
                nse_file = it.flush_non_se_condition()
                isnse.transport_non_se_results([nse_file], host_ips)
                isnse.create_and_send_transport_complete_flag_file(*it.k_m_n, current_ne_number, host_ips)

                send_itasks_progress_info(itasks, task_queue, working_host_number)

                if it.is_early_terminate():
                    continue

                if current_ne_number < it.max_ne:
                    it.working_ne_iset_numbers += 1
                    is_finish = False
                else:
                    it.is_task_finish = True
            else:
                is_finish = False
    return is_finish


def update_nse_files_to_new_host(host_ip, itasks):
    nse_files = list()
    complete_flags = list()
    for it in itasks:
        for i in it.non_se_condition_files:
            nse_files.append(isnse.get_nse_condition_file_path(*it.k_m_n, i, it.lp_type, it.is_use_extended_rules))
            complete_flags.append(isnse.get_transport_complete_flag_file(*it.k_m_n, i))
    isnse.transport_non_se_results(nse_files, [host_ip])
    isnse.transport_non_se_results(complete_flags, [host_ip])


def send_itasks_progress_info(isc_tasks, task_queue, working_hosts_number):
    msg_texts = dump_isc_task_results(isc_tasks)
    msg_text = "isc tasks progress info, remain %d task hosts, %d task slices:  \n\t\t%s" % (
        working_hosts_number, task_queue.qsize(), "\n\t\t".join(msg_texts))
    logging.info(msg_text)
    msg.send_message(msg_text)


def itask_slices_generator(isc_config_file="isets-tasks.json", is_use_extended_rules=True):
    ISCFileTaskTerminationWorkerQueueManager.register("get_task_queue")
    ISCFileTaskTerminationWorkerQueueManager.register("get_result_queue")
    manager = ISCFileTaskTerminationWorkerQueueManager(address=(config.task_host, config.task_host_port),
                                                       authkey=bytes(config.task_host_key, encoding="utf-8"))
    manager.connect()
    task_queue = manager.get_task_queue()

    isc_tasks_cfg = ISCTaskConfig(isc_config_file, is_use_extended_rules)
    isc_tasks = isc_tasks_cfg.isc_tasks

    for tid in range(len(isc_tasks)):
        it = isc_tasks[tid]
        min_ne = it.min_ne
        max_ne = it.max_ne
        se_iset_ids = it.meta_data.se_iset_ids
        unknown_iset_number = len(se_iset_ids)
        max_task_slice_number = 10000
        for i in range(min_ne, max_ne+1):
            task_counter = CombinaryCounter(i, unknown_iset_number)
            task_start_idx = []
            task_idx_cnt = 0

            while True:
                task_end_idx = task_counter.get_current_indicator()
                if task_end_idx is None:
                    break

                if task_idx_cnt == 0:
                    task_start_idx = copy.deepcopy(task_end_idx)

                task_idx_cnt += 1

                if task_idx_cnt == max_task_slice_number:
                    task_queue.append((task_start_idx, task_idx_cnt))
                    task_idx_cnt = 0

            if task_idx_cnt != 0:
                task_queue.append((task_start_idx, task_idx_cnt))


    working_hosts_number = 5
    for i in range(working_hosts_number * 200):
        task_queue.put((kill_signal, -1))
    logging.info("all itasks has been dispatched")


def init_kmn_isc_task_master_from_config(isc_config_file="isets-tasks.json", sleep_time=30, is_use_extended_rules=True, is_frequent_log=False):
    start_time = datetime.now()
    ISCFileTaskTerminationMasterQueueManager.register("get_task_queue", callable=get_task_queue)
    ISCFileTaskTerminationMasterQueueManager.register("get_result_queue", callable=get_result_queue)
    manager = ISCFileTaskTerminationMasterQueueManager(address=(config.task_host, config.task_host_port),
                                                       authkey=bytes(config.task_host_key, encoding="utf-8"))
    manager.start()
    task_queue = manager.get_task_queue()
    result_queue = manager.get_result_queue()
    localhost_ip = ssh.get_host_ip()

    task_generator = Pool(2)
    task_generator.apply_async(itask_slices_generator, args=(isc_config_file, is_use_extended_rules))
    task_generator.close()

    working_hosts_number = 0
    msg_text = "isc task master start, load isc tasks from %s" % (isc_config_file)
    logging.info(msg_text)
    msg.send_message(msg_text)

    isc_tasks_cfg = ISCTaskConfig(isc_config_file, is_use_extended_rules)
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
            send_itasks_progress_info(isc_tasks, task_queue, working_hosts_number)
            sleep_cnt = 0
            print_cnt = 0

        if sleep_cnt == progress_msg_cnt:
            send_itasks_progress_info(isc_tasks, task_queue, working_hosts_number)
            sleep_cnt = 0
            print_cnt = 0

        task_finish = check_itasks_status(isc_tasks, online_hosts, task_queue, working_hosts_number)
        if result_queue.empty():
            time.sleep(sleep_time)
            sleep_cnt += 1
            continue

        whn_diff = process_result_queue(result_queue, isc_tasks)
        whn_number = whn_diff[0]
        host_ip = whn_diff[1]
        working_hosts_number += whn_number

        if whn_number == 1:
            if host_ip != localhost_ip:
                online_hosts.add(host_ip)
                update_nse_files_to_new_host(host_ip, isc_tasks)
        elif whn_number == -1:
            if host_ip != localhost_ip:
                online_hosts.remove(host_ip)

    msg_text = "all isc task slices are discatched!"
    logging.info(msg_text)
    msg.send_message(msg_text)

    task_generator.join()
    while working_hosts_number > 0:
        if sleep_cnt == 10:
            msg_texts = dump_isc_task_results(isc_tasks)

            msg_text = "all isc tasks are discatched, DO NOT add new worker! isc tasks progress info, remain %d task hosts:  \n\t\t%s" \
                       % (working_hosts_number, "\n\t\t".join(msg_texts))
            logging.info(msg_text)
            msg.send_message(msg_text)
            sleep_cnt = 0

        if result_queue.empty():
            time.sleep(sleep_time)
            sleep_cnt += 1
            continue

        whn_diff = process_result_queue(result_queue, isc_tasks)
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


def init_worker_host_nse_envs(isc_tasks):
    for it in isc_tasks:
        k_size = it.k_m_n[0]
        m_size = it.k_m_n[1]
        n_size = it.k_m_n[2]
        isnse.clear_transport_complete_flag_files(k_size, m_size, n_size, it.min_ne, it.max_ne)
        isnse.create_transport_complete_flag_file(k_size, m_size, n_size, 1)
        nse_1_path = isnse.get_nse_condition_file_path(k_size, m_size, n_size, 1, it.lp_type, it.is_use_extended_rules)
        pathlib.Path(nse_1_path).touch()


def init_kmn_isc_task_workers(isc_config_file="isets-tasks.json", lp_type="lpmln", is_check_valid_rules=True, is_use_extended_rules=True):
    payload = config.worker_payload
    worker_pool = Pool(payload)
    pathlib.Path(config.task_host_lock_file).touch()
    ISCFileTaskTerminationWorkerQueueManager.register("get_task_queue")
    ISCFileTaskTerminationWorkerQueueManager.register("get_result_queue")
    manager = ISCFileTaskTerminationWorkerQueueManager(address=(config.task_host, config.task_host_port),
                                                       authkey=bytes(config.task_host_key, encoding="utf-8"))
    manager.connect()
    result_queue = manager.get_result_queue()
    host_ip = ssh.get_host_ip()

    result_queue.put((add_worker_signal, config.worker_host_name, host_ip))
    logging.info("task worker host %s start ..." % config.worker_host_name)

    # 初始化不等价条件目录文件
    isc_tasks = ISCTaskConfig(isc_config_file, is_use_extended_rules)
    isc_tasks = isc_tasks.isc_tasks
    init_worker_host_nse_envs(isc_tasks)


    for i in range(payload):
        worker_pool.apply_async(kmn_isc_task_worker,
                                args=(isc_config_file, i+1, lp_type, is_check_valid_rules, is_use_extended_rules))
    worker_pool.close()
    worker_pool.join()
    # if pathlib.Path(task_worker_host_lock_file).exists():
    result_queue.put((kill_signal, config.worker_host_name, host_ip))
    logging.info("task worker host %s send kill signal ..." % config.worker_host_name)
    logging.info("task worker host %s exit ..." % config.worker_host_name)


def kmn_isc_task_worker(isc_config_file="isets-tasks.json", worker_id=1, lp_type="lpmln", is_check_valid_rules=True, is_use_extended_rules=True):

    ISCFileTaskTerminationWorkerQueueManager.register("get_task_queue")
    ISCFileTaskTerminationWorkerQueueManager.register("get_result_queue")
    manager = ISCFileTaskTerminationWorkerQueueManager(address=(config.task_host, config.task_host_port),
                                                       authkey=bytes(config.task_host_key, encoding="utf-8"))

    is_check_valid_rules = False

    manager.connect()
    task_queue = manager.get_task_queue()
    result_queue = manager.get_result_queue()

    worker_name = "worker-%d" % worker_id
    time_fmt = "%Y-%m-%d %H:%M:%S.%f"
    worker_host_name = config.worker_host_name
    msg_text = "task worker %s start!" % (worker_name)
    logging.info(msg_text)
    isc_tasks = ISCTaskConfig(isc_config_file, is_use_extended_rules)
    isc_tasks = isc_tasks.isc_tasks
    processed_task_slices_number = 0

    for it in isc_tasks:
        it.loaded_non_se_condition_files.add(1)

    while True:
        if not pathlib.Path(config.task_host_lock_file).exists():
            break

        if task_queue.empty():
            time.sleep(20)
            continue

        itask = task_queue.get()
        if itask[0] == kill_signal:
            msg_text = "%s:%s isc task worker terminate ..." % (worker_host_name, worker_name)
            logging.info(msg_text)
            break

        isc_task_id = itask[0]
        it = isc_tasks[isc_task_id]
        task_details = itask[1]
        isc_begin = copy.deepcopy(task_details[0])
        ne_iset_number = len(isc_begin)

        nse_iset_number = ne_iset_number - 1
        if nse_iset_number not in it.loaded_non_se_condition_files:
            task_worker_load_nse_conditions(it, nse_iset_number)

        start_time = datetime.now()
        start_time_str = start_time.strftime(time_fmt)[:-3]

        k_size = it.k_m_n[0]
        m_size = it.k_m_n[1]
        n_size = it.k_m_n[2]

        se_iset_ids = it.meta_data.se_iset_ids
        unknown_iset_number = len(se_iset_ids)

        task_start = task_details[0]
        task_start = [str(s) for s in task_start]
        task_start = ",".join(task_start)

        task_number = task_details[1]
        task_name = worker_name + ("-task-%d" % processed_task_slices_number)

        msg_text = "%s: %d-%d-%d isc task: from %s length %d, nonempty iset number %d" % (
            task_name, k_size, m_size, n_size, task_start, task_number, ne_iset_number)
        logging.info(msg_text)

        task_counter = CombinaryCounter(ne_iset_number, unknown_iset_number)
        task_counter.reset_current_indicator(isc_begin)

        se_cdt_cnt = 0
        nse_cdt_cnt = 0
        new_nse_cdt_cnt = 0
        check_cdt_cnt = 0

        se_conditions_cache = list()
        nse_conditions_cache = list()
        validator = ISetConditionValidator(lp_type=lp_type, is_use_extended_rules=is_use_extended_rules)

        for i in range(task_number):
            task_idx = task_counter.get_current_indicator()
            non_ne_ids = set()
            for t in task_idx:
                non_ne_ids.add(se_iset_ids[t])

            if check_contain_nse_subparts(non_ne_ids, it):
                nse_cdt_cnt += 1
                continue

            check_cdt_cnt += 1
            if not check_contain_i4_isets(non_ne_ids, it):
                continue

            is_contain_valid_rule, is_strongly_equivalent, condition = \
                validator.validate_kmn_extended_iset_condition_from_non_emtpy_iset_ids_return_icondition_obj(
                    non_ne_ids, k_size, m_size, n_size, is_check_valid_rule=is_check_valid_rules)

            # if not is_contain_valid_rule:
            if is_strongly_equivalent:
                se_conditions_cache.append(condition)
                se_cdt_cnt += 1
            else:
                nse_conditions_cache.append(condition)
                nse_cdt_cnt += 1
                new_nse_cdt_cnt += 1

        # for sec in se_conditions_cache:
        if se_cdt_cnt > 0:
            result_queue.put((se_condition_signal, isc_task_id, se_conditions_cache))

        if new_nse_cdt_cnt > 0:
            result_queue.put((nse_condition_signal, isc_task_id, nse_conditions_cache))


        end_time = datetime.now()
        end_time_str = end_time.strftime(time_fmt)[:-3]
        msg_text = "%s, end %d-%d-%d isc tasks from %s length %d, start time %s, end time %s, find %d se conditions (no semi-valid rules), find %d non-se conditions" % (
            task_name, k_size, m_size, n_size, task_start, task_number, start_time_str, end_time_str, se_cdt_cnt, nse_cdt_cnt)
        logging.info(msg_text)
        result_queue.put((stat_signal, isc_task_id, ne_iset_number, check_cdt_cnt, task_number, (start_time, end_time)))
        processed_task_slices_number += 1

    logging.info("%s processes %d isc task slices" % (worker_name, processed_task_slices_number))


def task_worker_load_nse_conditions(itask, nse_iset_number):
    for i in range(1, nse_iset_number + 1):
        if i not in itask.loaded_non_se_condition_files:
            complete_flag = isnse.get_transport_complete_flag_file(*itask.k_m_n, i)
            transport_complete = False
            while not transport_complete:
                if pathlib.Path(complete_flag).exists():
                    transport_complete = True
                else:
                    time.sleep(3)
            nse_conditions = isnse.load_kmn_non_se_results(*itask.k_m_n, i, itask.lp_type, itask.is_use_extended_rules)
            itask.non_se_conditions.extend(nse_conditions)
            itask.loaded_non_se_condition_files.add(i)


def check_contain_nse_subparts(iset_ids, itask):
    for nse in itask.non_se_conditions:
        if nse.issubset(iset_ids):
            return True
    return False

def check_contain_i4_isets(iset_ids, itask):
    for i4_isets in itask.min_i4_iset_tuples:
        if i4_isets.issubset(iset_ids):
            return True
    return False

if __name__ == '__main__':
    # init_kmn_isc_task_master_from_config(sleep_time=2, is_use_extended_rules=False)
    init_kmn_isc_task_workers(lp_type="lpmln", is_use_extended_rules=False)
