
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-27 9:55
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : __init__.py.py
"""

from lpmln.search.distributed.rawicondition.RawIConditionSearchMaster import RawIConditionSearchMaster
from lpmln.search.distributed.rawicondition.RawIConditionSearchWorker import RawIConditionSearchWorker


def init_task_master(isc_config_file="isets-tasks.json", sleep_time=30):
    RawIConditionSearchMaster.init_kmn_isc_task_master_from_config(RawIConditionSearchMaster, isc_config_file, sleep_time)


def init_task_worker(isc_config_file="isets-tasks.json", is_check_valid_rules=True):
    RawIConditionSearchWorker.init_extra_kmn_pre_task_workers(RawIConditionSearchWorker, isc_config_file, is_check_valid_rules)

if __name__ == '__main__':
    init_task_worker()
    pass
    