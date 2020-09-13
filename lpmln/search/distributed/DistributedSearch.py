
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-13 14:35
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : DistributedSearch.py
"""

from multiprocessing import Pool, Queue
from multiprocessing.managers import BaseManager
import logging
from datetime import datetime
import time
import pathlib

from lpmln.utils.counter.CombinaryCounter import CombinaryCounter
from lpmln.iset.ISetConditionValidator import ISetConditionValidator
import lpmln.iset.ISetUtils as isu
import lpmln.message.Messager as msg
import lpmln.config.GlobalConfig as cfg
import lpmln.config.ISCTaskConfig as isc_cfg


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
    