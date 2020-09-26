
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-25 21:23
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : ISCChecker.py
"""

import lpmln.iset.IConditionUtils as iscu
import lpmln.config.GlobalConfig as cfg
import lpmln.iset.ISetUtils as isu
from lpmln.itask.ITaskMeta import ITaskMeta

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
    print(kmn_key, "iset conditions: ")
    file = config.get_isc_results_file_path(*kmn_data[kmn_key])
    conditions = iscu.load_iconditions_from_file(file)
    ne_isets = iscu.get_iconditions_ne_isets(conditions)
    print("ne isets: ", ne_isets)
    ignore_ne_isets = iscu.find_common_ne_isets_from_iconditions(conditions)
    print("common ne isets: ", ignore_ne_isets)
    ne_symbols = iscu.get_iconditions_ne_isets_logic_symbols(conditions, ignore_ne_isets)
    print("ne logic symbols: ", ne_symbols)

    for c in conditions:
        print(c, " : ", iscu.convert_icondition_2_conjunctive_formula(c, ne_symbols))


    dnf = iscu.convert_iconditions_2_disjunctive_formula(conditions, ne_symbols)
    print("dnf: ", dnf)
    print("simplified: ", iscu.simplify_iconditions(conditions, ignore_ne_isets))


def check_011_programs():
    program = [
        [{3}, set(), {2}],
        [{2, 3}, set(), {2}]
    ]

    isets = isu.compute_isets_from_program(program, False)
    for s in isets:
        print(isets[s])


def check_011_icondition():
    ne_isets = {35}
    icondition_space_isets = {35, 40, 8, 12, 44, 17}
    meta = ITaskMeta.load_itask_meta_data_from_file(config.isc_meta_data_file)
    meta_key = "0-1-1-lpmln-3"
    meta_data = meta[meta_key]
    condition_comp = iscu.complete_and_analyse_icondition(ne_isets, icondition_space_isets, meta_data)

    rule_sets = iscu.get_rule_sets(2, False)
    rule_set_size = 3

    for i in range(len(rule_sets)):
        if i % rule_set_size == 0:
            print("\n")
        key = rule_sets[i]
        print(key, ":", condition_comp[key])


def check_110_icondition():
    ne_isets = {35}
    icondition_space_isets = {0, 1, 4, 8, 12, 17, 32, 35, 36, 40, 44}
    meta = ITaskMeta.load_itask_meta_data_from_file(config.isc_meta_data_file)
    meta_key = "1-1-0-lpmln-3"
    meta_data = meta[meta_key]
    condition_comp = iscu.complete_and_analyse_icondition(ne_isets, icondition_space_isets, meta_data)

    rule_sets = iscu.get_rule_sets(2, False)
    rule_set_size = 3

    for i in range(len(rule_sets)):
        if i % rule_set_size == 0:
            print("\n")
        key = rule_sets[i]
        print(key, ":", condition_comp[key])


if __name__ == '__main__':
    # check_isc_data("0-1-1")
    # check_isc_data("1-1-0")
    # check_011_programs()
    # check_011_icondition()
    check_110_icondition()
    pass
    