
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-25 21:23
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ISCChecker.py
"""

import lpmln.config.GlobalConfig as cfg
config = cfg.load_configuration()


kmn_data = {
    "0-1-0": (0, 1, 0, 1, 7),
    "0-1-1": (0, 1, 1, 1, 16),
    "1-1-0": (1, 1, 0, 1, 20),
    "0-2-1": (0, 2, 1, 1, 33),
    "1-2-0": (1, 2, 0, 1, 42),
    "1-1-1": (1, 1, 0, 1, 45),
    "2-1-0": (2, 1, 0, 1, 54),
}


def get_isc_file(kmn_key):
    return config.get_isc_results_file_path(*kmn_data[kmn_key])


if __name__ == '__main__':
    for key in kmn_data:
        print(get_isc_file(key))
    pass
    