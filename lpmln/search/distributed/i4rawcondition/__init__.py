
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-30 10:34
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : __init__.py.py
"""


from lpmln.search.distributed.i4rawcondition.I4RawSearchMaster import I4RawSearchMaster, I4RawSearchWorker



def init_task_master(isc_config_file="isets-tasks.json", sleep_time=30):
    I4RawSearchMaster.init_kmn_isc_task_master_from_config(I4RawSearchMaster, isc_config_file, sleep_time)


def init_task_worker(isc_config_file="isets-tasks.json", is_check_valid_rules=True):
    I4RawSearchWorker.init_extra_kmn_pre_task_workers(I4RawSearchWorker, isc_config_file, is_check_valid_rules)


if __name__ == '__main__':
    pass
    