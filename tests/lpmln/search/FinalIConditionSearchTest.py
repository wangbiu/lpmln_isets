
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-18 21:58
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : FinalIConditionSearchTest.py
"""

from lpmln.search.distributed.FinalIConditionSearch import FinalIConditionsSearchHTWorker, FinalIConditionsSearchMaster, SearchQueueManager
from lpmln.itask.ITask import ITaskConfig
master = FinalIConditionsSearchMaster
worker = FinalIConditionsSearchHTWorker
import lpmln.config.GlobalConfig as cfg
config = cfg.load_configuration()
import os

isc_config_file=os.path.join(config.project_base_dir, "isets-tasks.json")

def load_itasks():
    isc_tasks = ITaskConfig(isc_config_file)
    isc_tasks = isc_tasks.isc_tasks
    return isc_tasks



def test_load_nse_condition():
    manager_tuple = SearchQueueManager.init_task_worker_queue_manager()

    task_slice = (0, ({35}, {0, 1, 4, 7, 8, 9, 39, 40, 12, 41, 44, 15, 16, 17, 20}, 6))
    task_id = task_slice[0]
    isc_tasks = load_itasks()
    print(isc_tasks[task_id].k_m_n)
    load_complete = worker.task_worker_load_nse_conditions(isc_tasks[task_id], task_slice[1])
    print("load nse conditions complete", load_complete)
    processed_task_slices = worker.process_task_slice(worker, task_id, isc_tasks[task_id], task_slice[1], manager_tuple)
    print(processed_task_slices)






if __name__ == '__main__':
    pass
    