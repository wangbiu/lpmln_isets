
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-10-08 14:49
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : KMN111IConditionHelper.py
"""

import lpmln.iset.IConditionUtils as iscu
import lpmln.config.GlobalConfig as cfg
import lpmln.iset.ISetUtils as isu
from lpmln.itask.ITaskMeta import ITaskMeta
import lpmln.helper.ISCChecker as isch

config = cfg.load_configuration()


kmn_data = {
    "0-1-0": (0, 1, 0, 1, 7),
    "0-1-1": (0, 1, 1, 1, 16),
    "1-1-0": (1, 1, 0, 1, 20),
    "0-2-1": (0, 2, 1, 1, 33),
    "1-2-0": (1, 2, 0, 1, 42),
    "1-1-1": (1, 1, 1, 1, 45),
    "2-1-0": (2, 1, 0, 1, 54),
}


def check_111_singleton_data():
    isch.check_isc_data("1-1-1", "s", True)


if __name__ == '__main__':
    check_111_singleton_data()
    pass
    