
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-14 15:26
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : AllDistributedSearch.py
"""

import lpmln.search.distributed.DistributedSearch as alg1
import lpmln.search.distributed.PreSkipI4DistributedSearch as alg2
import lpmln.search.distributed.AugumentedPreSkipI4DistributedSearch as alg3
import lpmln.search.distributed.NoWaitNSEAugPreSkipI4DSearch as alg4
import lpmln.search.distributed.EarlyTerminationPreSkipI4DSearch as alg5
import lpmln.search.distributed.AfterSkipNSEEarlyTPreSI4DSearch as alg6
import lpmln.search.distributed.final as alg7


algorithms = [alg1, alg2, alg3, alg4, alg5, alg6, alg7]


def get_distributed_search_algorithm(alg_id):
    alg_id -= 1
    if alg_id < 0:
        return algorithms[-1]
    else:
        return algorithms[alg_id]


def init_task_master(isc_config_file="isets-tasks.json", sleep_time=30, alg_id=-2):
    alg = get_distributed_search_algorithm(alg_id)
    alg.init_task_master(isc_config_file, sleep_time)


def init_task_worker(isc_config_file="isets-tasks.json", is_check_valid_rules=True, alg_id=-2):
    alg = get_distributed_search_algorithm(alg_id)
    alg.init_task_worker(isc_config_file, is_check_valid_rules)


if __name__ == '__main__':
    try:
        # init_task_worker()
        init_task_master(sleep_time=2)
    except Exception as e:
        print(e)
    pass
    