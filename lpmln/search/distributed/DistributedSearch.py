
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-13 14:35
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : DistributedSearch.py
"""

from multiprocessing.managers import BaseManager


class SearchMasterQueueManger(BaseManager):
    pass


class SearchWorkerQueueManger(BaseManager):
    pass


class DistributedSearchIConditions:
    def __init__(self, lp_type="lpmln", is_use_extended_rules=False):
        self.lp_type = lp_type
        self.is_use_extended_rules = is_use_extended_rules

    





if __name__ == '__main__':
    pass
    