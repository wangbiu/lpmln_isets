
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-25 21:23
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ISCChecker.py
"""
from mpmath import cond

import lpmln.iset.IConditionUtils as iscu
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


def check_isc_data(kmn_key):
    file = config.get_isc_results_file_path(*kmn_data[kmn_key])
    conditions = iscu.load_iconditions_from_file(file)
    ne_isets = iscu.get_iconditions_ne_isets(conditions)
    print("ne isets: ", ne_isets)
    ne_symbols = iscu.get_iconditions_ne_isets_logic_symbols(conditions)
    print("ne logic symbols: ", ne_symbols)

    for c in conditions:
        print(c, iscu.convert_icondition_2_conjunctive_formula(c, ne_symbols))



if __name__ == '__main__':
    check_isc_data("0-1-1")
    pass
    