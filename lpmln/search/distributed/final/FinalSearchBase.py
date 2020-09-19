
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-19 14:44
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : FinalSearchBase.py
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


class FinalIConditionsSearchBaseWorker:
    @staticmethod
    def init_worker_host_nse_envs(isc_tasks):
        for it in isc_tasks:
            isnse.clear_transport_complete_flag_files(*it.k_m_n, it.min_ne, it.max_ne)
            isnse.create_transport_complete_flag_file(*it.k_m_n, 0)
            nse_1_path = isnse.get_nse_condition_file_path(*it.k_m_n, 0, it.lp_type,
                                                           it.is_use_extended_rules)
            pathlib.Path(nse_1_path).touch()
            isnse.clear_task_terminate_flag_files(*it.k_m_n)



if __name__ == '__main__':
    pass
    