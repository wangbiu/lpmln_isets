
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-18 21:58
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : FinalIConditionSearchTest.py
"""

from lpmln.search.distributed.final import FinalIConditionsSearchHTWorker, FinalSearchMaster
from lpmln.search.distributed.final.FinalSearchPreWorker import FinalIConditionsSearchPreWorker
from lpmln.search.distributed.final.FinalSearchBase import SearchQueueManager
from lpmln.itask.ITask import ITaskConfig
from lpmln.iset.ISetConditionValidator import ISetConditionValidator
from multiprocessing import Pool
master = FinalSearchMaster
worker = FinalIConditionsSearchHTWorker
pre_worker = FinalIConditionsSearchPreWorker
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
    # load_complete = worker.task_worker_load_nse_conditions(isc_tasks[task_id], task_slice[1])
    # print("load nse conditions complete", load_complete)
    # processed_task_slices = worker.process_task_slice(worker, task_id, isc_tasks[task_id], task_slice[1], manager_tuple)
    # print(processed_task_slices)


def test_nse_process():
    task_slice = ({32, 35, 36}, {0, 1, 4, 7, 8, 9, 39, 40, 12, 41, 44, 15, 16, 17, 20}, 0)
    nse = {35, 7}
    skip, new_ts = pre_worker.process_one_nse_subpart_task_slice(pre_worker, nse, task_slice)
    print(skip)
    print(new_ts)


def test_parallel_validate():
    isc_tasks = load_itasks()
    itask = isc_tasks[1]
    ne_isets = {32, 35, 44}
    p = Pool(100)
    validator = ISetConditionValidator(False)
    is_se, condition = FinalIConditionsSearchHTWorker.parallel_validate_kmn_extended_icondition(
        FinalIConditionsSearchHTWorker, ne_isets, itask, validator)

    print(is_se, condition)

    is_contain_valid_rule, is_strongly_equivalent, condition = \
        validator.validate_kmn_extended_iset_condition_from_non_emtpy_iset_ids_return_icondition_obj(ne_isets, *itask.k_m_n, False)

    print(is_strongly_equivalent, condition)


def test_singleton_icondition():
    kmn = (0, 2, 3)
    ne_isets = {16673, 2323}
    validator = ISetConditionValidator(False)
    is_contain_valid_rule, is_strongly_equivalent, condition = \
        validator.validate_kmn_extended_iset_condition_from_non_emtpy_iset_ids_return_icondition_obj(ne_isets, *kmn, False)
    print(is_strongly_equivalent, condition)






if __name__ == '__main__':
    pass
    