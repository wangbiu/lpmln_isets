
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-27 9:45
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : __init__.py.py
"""

from lpmln.search.distributed.ht.HTCheckingWorker import HTCheckingWorker
from lpmln.search.distributed.ht.HTCheckingMaster import HTCheckingMaster


def init_task_master(isc_config_file="isets-tasks.json", sleep_time=30):
    HTCheckingMaster.init_kmn_isc_task_master_from_config(HTCheckingMaster, isc_config_file, sleep_time)


def init_task_worker(isc_config_file="isets-tasks.json", is_check_valid_rules=True):
    HTCheckingWorker.init_kmn_isc_task_workers(HTCheckingWorker, isc_config_file, is_check_valid_rules, None)


if __name__ == '__main__':
    # init_task_master(sleep_time=1)
    init_task_worker()
    pass
    