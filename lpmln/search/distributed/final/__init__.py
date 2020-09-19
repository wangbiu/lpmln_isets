
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-19 14:43
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : __init__.py.py
"""


def init_task_master(isc_config_file="isets-tasks.json", sleep_time=30):
    FinalIConditionsSearchMaster.init_kmn_isc_task_master_from_config(FinalIConditionsSearchMaster, isc_config_file, sleep_time)


def init_task_worker(isc_config_file="isets-tasks.json", is_check_valid_rules=True):
    FinalIConditionsSearchHTWorker.init_kmn_isc_task_workers(FinalIConditionsSearchHTWorker, isc_config_file, is_check_valid_rules)



if __name__ == '__main__':
    pass
    