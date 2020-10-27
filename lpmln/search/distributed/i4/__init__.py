
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-30 9:11
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : __init__.py.py
"""

from lpmln.search.distributed.i4.I4SearchMaster import I4SearchMaster, I4SearchWorker


def init_task_master(isc_config_file="isets-tasks.json", sleep_time=30):
    I4SearchMaster.init_kmn_isc_task_master_from_config(I4SearchMaster, isc_config_file, sleep_time)


def init_task_worker(isc_config_file="isets-tasks.json", is_check_valid_rules=True):
    I4SearchWorker.init_extra_kmn_pre_task_workers(I4SearchWorker, isc_config_file, is_check_valid_rules)


if __name__ == '__main__':
    # init_task_worker()
    init_task_master(sleep_time=1)
    pass


if __name__ == '__main__':
    pass
    