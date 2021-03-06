
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-19 14:43
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : __init__.py.py
"""

from lpmln.search.distributed.final.FinalSearchMaster import FinalIConditionsSearchMaster
from lpmln.search.distributed.final.FinalSearchHTWorker import FinalIConditionsSearchHTWorker
from lpmln.search.distributed.final.FinalSearchPreWorker import FinalIConditionsSearchPreWorker
from lpmln.search.distributed.final.FinalSearchHTWorkerCheckSameRule import FinalIConditionsSearchHTWorkerCheckSameRules
from lpmln.search.distributed.final.FinalISearchHTWorkerSkipHTChecking import FinalIConditionsSearchHTWorkerSkipHT


def init_task_master(isc_config_file="isets-tasks.json", sleep_time=30):
    FinalIConditionsSearchMaster.init_kmn_isc_task_master_from_config(FinalIConditionsSearchMaster, isc_config_file, sleep_time)


def init_task_worker(isc_config_file="isets-tasks.json", is_check_valid_rules=True):
    FinalIConditionsSearchHTWorker.init_kmn_isc_task_workers(FinalIConditionsSearchHTWorker, isc_config_file, is_check_valid_rules)


def extra_init_pretask_worker(isc_config_file="isets-tasks.json", is_check_valid_rules=False):
    FinalIConditionsSearchPreWorker.init_extra_kmn_pre_task_workers(FinalIConditionsSearchPreWorker, isc_config_file, is_check_valid_rules, None)


def init_task_worker_check_same_rules(isc_config_file="isets-tasks.json", is_check_valid_rules=True):
    FinalIConditionsSearchHTWorkerCheckSameRules.init_kmn_isc_task_workers(
        FinalIConditionsSearchHTWorkerCheckSameRules, isc_config_file, is_check_valid_rules, None)


def init_task_worker_skip_ht_checking(isc_config_file="isets-tasks.json", is_check_valid_rules=True):
    FinalIConditionsSearchHTWorkerSkipHT.init_kmn_isc_task_workers(
        FinalIConditionsSearchHTWorkerSkipHT, isc_config_file, is_check_valid_rules, None)

if __name__ == '__main__':
    init_task_worker_check_same_rules()
    pass
    