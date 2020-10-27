
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-15 20:08
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : KMNTupleChecker.py
"""


import lpmln.iset.ISetUtils as isu
import lpmln.iset.ISetCompositionUtils as iscm


nse_011 = [
    [0, 35],
    [1, 35],
    [35, 4],
    [35, 7],
    [9, 35],
    [35, 39],
    [41, 35],
    [35, 15],
    [16, 35],
    [35, 20],
]

nse_110 = [
    [35, 39],
    [9, 35],
    [41, 35],
    [35, 15],
    [16, 35],
    [35, 20],
    [35, 7],
    [33, 35],
    [35, 31],
]

nse_021 = [
    [291, 135],
    [8, 291],
    [9, 291],
    [137, 291],
    [136, 291],
    [16, 291],
    [17, 291],
    [291, 143],
    [144, 291],
    [64, 291],
    [65, 291],
    [291, 71],
    [73, 291],
    [291, 79],
    [80, 291],
    [81, 291],
    [0, 291],
    [1, 291],
    [129, 291],
    [128, 291],
    [99, 259],
    [99, 267],
    [99, 275],
    [35, 259],
    [35, 291],
    [35, 267],
    [35, 275],
    [259, 163],
    [259, 291],
    [291, 163],
    [267, 163],
    [275, 163],
    [275, 291],
    [291, 15, 63],
    [291, 15, 127],
    [291, 7, 63],
    [291, 7, 127],
    [99, 291, 15],
    [99, 291, 7],
    [267, 291, 63],
    [267, 291, 127],
]


def check_tuple(ne_isets, rule_number, is_use_extended_rules=False):
    rs = 3
    if is_use_extended_rules:
        rs = 4
    iset_number = 2 ** (rs * rule_number) - 1
    icondition = isu.construct_iset_condition_from_non_emtpy_iset_ids(ne_isets, iset_number)
    isets = isu.construct_isets_from_iset_condition(icondition, is_use_extended_rules, 1)
    rules = isu.construct_rules_from_isets(isets, rule_number, rs)
    for r in rules:
        print(r)


def check_nse_011_tuples():
    for ne_isets in nse_011:
        compostions = [iscm.get_iset_compositions(id+1, 2, False) for id in ne_isets]
        print("nse 0-1-1 condition: ", compostions)
        check_tuple(ne_isets, 2, False)
        print(" ")


def check_nse_110_tuples():
    for ne_isets in nse_110:
        compostions = [iscm.get_iset_compositions(id+1, 2, False) for id in ne_isets]
        print("nse 1-1-0 condition: ", compostions)
        check_tuple(ne_isets, 2, False)
        print(" ")


def check_nse_021_tuples():
    for ne_isets in nse_021:
        compostions = [iscm.get_iset_compositions(id+1, 3, False) for id in ne_isets]
        print("nse 0-2-1 condition: ", compostions)
        check_tuple(ne_isets, 3, False)
        print(" ")

if __name__ == '__main__':
    # check_nse_011_tuples()
    # check_nse_110_tuples()
    check_nse_021_tuples()
    pass
    