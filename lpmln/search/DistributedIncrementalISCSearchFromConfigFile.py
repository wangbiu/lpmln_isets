
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-08-29 15:05
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : DistributedIncrementalISCSearchFromConfigFile.py
"""

from multiprocessing import Pool, Queue
from multiprocessing.managers import BaseManager
import logging
from datetime import datetime
import time
import pathlib
import copy

from lpmln.iset.ISetConditionValidator import ISetConditionValidator
import lpmln.message.Messager as msg
import lpmln.config.GlobalConfig as cfg
from lpmln.config.IncrementalISCTaskConfig import IncrementalISCTaskConfig

config = cfg.load_configuration()

add_worker_signal = "--a-worker--"
kill_signal = "--over--"
stat_signal = "--stat--"
condition_signal = "--cdt--"


class IncrementalISCFileTaskMasterQueueManager(BaseManager):
    pass


class IncrementalISCFileTaskWorkerQueueManager(BaseManager):
    pass


global_task_queue = Queue()


def get_task_queue():
    return global_task_queue


global_result_queue = Queue()


def get_result_queue():
    return global_result_queue


def put_isc_task_items(task_id, items, msg_text, task_queue):
    for it in items:
        task_queue.put((task_id, it))
    # logging.info(msg_text)
    # msg.send_message(msg_text)


def dump_isc_task_results(isc_tasks):
    msg_texts = []
    for it in isc_tasks:
        it.dump_tmp_se_condition()
        msg_texts.append(it.get_progress_info())
    return msg_texts


def process_result_queue(task_queue, result_queue, isc_tasks):
    working_hosts_number_diff = 0
    result = result_queue.get()
    result_state = result[0]
    isc_task_id = result[1]

    if result_state == kill_signal:
        working_hosts_number_diff = -1
        msg_text = "task host %s exit" % isc_task_id
        logging.info(msg_text)
        msg.send_message(msg_text)
    elif result_state == add_worker_signal:
        msg_text = "task host %s is online" % isc_task_id
        working_hosts_number_diff = 1
        logging.info(msg_text)
        msg.send_message(msg_text)
    elif result_state == stat_signal:
        ne_iset_number = result[2]
        task_complete_number = result[3]
        task_running_time = result[4]
        isc_tasks[isc_task_id].set_task_complete_number(task_complete_number, ne_iset_number)
        isc_tasks[isc_task_id].set_task_running_time(task_running_time)
    elif result_state == condition_signal:
        icondition = result[2]
        if icondition.contain_se_valid_rules:
            itask_items, msg_text = isc_tasks[isc_task_id].generate_isc_task_items_by_base_icondition(icondition)
        else:
            itask_items, msg_text = isc_tasks[isc_task_id].insert_se_condition(icondition)
        put_isc_task_items(isc_task_id, itask_items, msg_text, task_queue)

    return working_hosts_number_diff


def check_has_new_itask_items(itasks):
    for it in itasks:
        if it.has_new_itask_items():
            return True
    return False


def init_kmn_isc_task_master_from_config(isc_config_file="isets-tasks.json", sleep_time=30, is_use_extended_rules=True):
    start_time = datetime.now()
    IncrementalISCFileTaskMasterQueueManager.register("get_task_queue", callable=get_task_queue)
    IncrementalISCFileTaskMasterQueueManager.register("get_result_queue", callable=get_result_queue)
    manager = IncrementalISCFileTaskMasterQueueManager(address=(config.task_host, config.task_host_port),
                                                       authkey=bytes(config.task_host_key, encoding="utf-8"))
    manager.start()
    task_queue = manager.get_task_queue()
    result_queue = manager.get_result_queue()

    working_hosts_number = 0
    msg_text = "isc task master start, load isc tasks from %s" % (isc_config_file)
    logging.info(msg_text)
    msg.send_message(msg_text)

    isc_tasks_cfg = IncrementalISCTaskConfig(isc_config_file, is_use_extended_rules)
    isc_tasks = isc_tasks_cfg.isc_tasks

    sleep_cnt = 0

    for task_id in range(len(isc_tasks)):
        itask = isc_tasks[task_id]
        itask_items, msg_text = itask.get_initial_isc_task_items()
        put_isc_task_items(task_id, itask_items, msg_text, task_queue)

    has_new_itask_items = True
    while not task_queue.empty() or has_new_itask_items:
        if sleep_cnt == 10:
            msg_texts = dump_isc_task_results(isc_tasks)
            msg_text = "isc tasks progress info, remain %d task hosts, %d task slices:  \n\t\t%s" % (
                working_hosts_number, task_queue.qsize(), "\n\t\t".join(msg_texts))
            logging.info(msg_text)
            msg.send_message(msg_text)
            sleep_cnt = 0

        if result_queue.empty():
            time.sleep(sleep_time)
            sleep_cnt += 1
            continue

        whn_diff = process_result_queue(task_queue, result_queue, isc_tasks)
        working_hosts_number += whn_diff
        has_new_itask_items = check_has_new_itask_items(isc_tasks)



    msg_text = "all isc task slices are discatched!"
    logging.info(msg_text)
    msg.send_message(msg_text)

    for i in range(working_hosts_number * 200):
        task_queue.put((kill_signal, -1))

    while working_hosts_number > 0:
        if sleep_cnt == 10:
            msg_texts = dump_isc_task_results(isc_tasks)

            msg_text = "all isc tasks have been discatched, DO NOT add new worker! isc tasks progress info, remain %d task hosts:  \n\t\t%s" \
                       % (working_hosts_number, "\n\t\t".join(msg_texts))
            logging.info(msg_text)
            msg.send_message(msg_text)
            sleep_cnt = 0

        if result_queue.empty():
            time.sleep(sleep_time)
            sleep_cnt += 1
            continue

        whn_diff = process_result_queue(task_queue, result_queue, isc_tasks)
        working_hosts_number += whn_diff

    msg_texts = []
    attached_files = []
    for it in isc_tasks:
        msg_texts.append(it.task_finish())
        attached_files.append(it.result_file)

    msg_text = "isc tasks finish! \n\t\t%s" % "\n\t\t".join(msg_texts)
    logging.info(msg_text)
    msg.send_message(msg=msg_text, attached_files=attached_files)


def init_kmn_isc_task_workers(isc_config_file="isets-tasks.json", lp_type="lpmln", is_check_valid_rules=True, is_use_extended_rules=True):
    payload = config.worker_payload
    worker_pool = Pool(payload)
    pathlib.Path(config.task_host_lock_file).touch()
    IncrementalISCFileTaskWorkerQueueManager.register("get_task_queue")
    IncrementalISCFileTaskWorkerQueueManager.register("get_result_queue")
    manager = IncrementalISCFileTaskWorkerQueueManager(address=(config.task_host, config.task_host_port),
                                                       authkey=bytes(config.task_host_key, encoding="utf-8"))
    manager.connect()
    result_queue = manager.get_result_queue()

    result_queue.put((add_worker_signal, config.worker_host_name))
    logging.info("task worker host %s start ..." % config.worker_host_name)

    for i in range(payload):
        worker_pool.apply_async(kmn_isc_task_worker,
                                args=(isc_config_file, "worker-%d" % (i + 1), lp_type, is_check_valid_rules, is_use_extended_rules))
    worker_pool.close()
    worker_pool.join()
    # if pathlib.Path(task_worker_host_lock_file).exists():
    result_queue.put((kill_signal, config.worker_host_name))
    logging.info("task worker host %s send kill signal ..." % config.worker_host_name)
    logging.info("task worker host %s exit ..." % config.worker_host_name)


def kmn_isc_task_worker(isc_config_file="isets-tasks.json", worker_name="", lp_type="lpmln", is_check_valid_rules=True, is_use_extended_rules=True):

    IncrementalISCFileTaskWorkerQueueManager.register("get_task_queue")
    IncrementalISCFileTaskWorkerQueueManager.register("get_result_queue")
    manager = IncrementalISCFileTaskWorkerQueueManager(address=(config.task_host, config.task_host_port),
                                                       authkey=bytes(config.task_host_key, encoding="utf-8"))
    manager.connect()
    task_queue = manager.get_task_queue()
    result_queue = manager.get_result_queue()

    time_fmt = "%Y-%m-%d %H:%M:%S.%f"
    worker_host_name = config.worker_host_name
    msg_text = "task worker %s start!" % (worker_name)
    logging.info(msg_text)

    isc_tasks = IncrementalISCTaskConfig(isc_config_file, is_use_extended_rules)
    isc_tasks = isc_tasks.isc_tasks
    processed_task_slices_number = 0

    while True:
        if not pathlib.Path(config.task_host_lock_file).exists():
            break

        if task_queue.empty():
            time.sleep(10)
            continue

        itask = task_queue.get()
        if itask[0] == kill_signal:
            msg_text = "%s:%s isc task worker terminate ..." % (worker_host_name, worker_name)
            logging.info(msg_text)
            break

        start_time = datetime.now()
        start_time_str = start_time.strftime(time_fmt)[:-3]

        isc_task_id = itask[0]
        it = isc_tasks[isc_task_id]
        k_size = it.k_m_n[0]
        m_size = it.k_m_n[1]
        n_size = it.k_m_n[2]

        task_details = itask[1]

        base_isets_ids = set(task_details[0])
        task_batch = set(task_details[1])
        task_number = len(task_batch)

        task_name = worker_name + ("-task-%d" % processed_task_slices_number)
        ne_iset_number = len(base_isets_ids) + 1

        task_start = [str(s) for s in base_isets_ids]
        task_start = "{%s}" % ",".join(task_start)
        msg_text = "%s: %d-%d-%d isc task: base iset ids = %s, task size = %d, nonempty iset number %d" % (
            task_name, k_size, m_size, n_size, task_start, task_number, ne_iset_number)
        logging.info(msg_text)


        se_cdt_cnt = 0


        se_conditions_cache = list()
        validator = ISetConditionValidator(lp_type=lp_type, is_use_extended_rules=is_use_extended_rules)

        for iset in task_batch:
            non_ne_ids = copy.deepcopy(base_isets_ids)
            non_ne_ids.add(iset)

            is_contain_valid_rule, is_strongly_equivalent, condition = \
                validator.validate_kmn_extended_iset_condition_from_non_emtpy_iset_ids_return_icondition_obj(
                    non_ne_ids, k_size, m_size, n_size, is_check_valid_rule=is_check_valid_rules)

            if not is_contain_valid_rule and is_strongly_equivalent:
                se_conditions_cache.append(condition)
                se_cdt_cnt += 1

        for sec in se_conditions_cache:
            result_queue.put((condition_signal, isc_task_id, sec))

        end_time = datetime.now()
        end_time_str = end_time.strftime(time_fmt)[:-3]
        msg_text = "%s, end %d-%d-%d isc tasks from %s length %d, start time %s, end time %s, find %d se conditions" % (
            task_name, k_size, m_size, n_size, task_start, task_number, start_time_str, end_time_str, se_cdt_cnt)
        logging.info(msg_text)
        result_queue.put((stat_signal, isc_task_id, ne_iset_number, task_number, (start_time, end_time)))
        processed_task_slices_number += 1

    logging.info("%s processes %d isc task slices" % (worker_name, processed_task_slices_number))


if __name__ == '__main__':
    # init_kmn_isc_task_master_from_config(sleep_time=2)
    init_kmn_isc_task_workers(lp_type="lpmln", is_use_extended_rules=False)

    