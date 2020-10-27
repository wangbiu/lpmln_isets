
# -*- encoding: utf-8 -*-
"""
@Time    : 2020/6/7 15:32
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : TestISetConditionValidator.py
"""

from lpmln.iset.ISetConditionValidator import ISetConditionValidator

non_se_010 = [
    "0,0,0,1,0,0,0",
    "0,0,0,1,1,0,0",
    "0,1,0,1,0,0,0",
    "0,1,0,1,1,0,0",
    "1,0,0,1,0,0,0",
    "1,0,0,1,1,0,0",
    "1,1,0,1,0,0,0",
    "1,1,0,1,1,0,0"
]

se_010 = [
    "0,0,0,0,0,0,0",
    "0,0,0,0,0,0,1",
    "0,0,0,0,0,1,0",
    "0,0,0,0,0,1,1",
    "0,0,0,0,1,0,0",
    "0,0,0,0,1,0,1",
    "0,0,0,0,1,1,0",
    "0,0,0,0,1,1,1",
    "0,0,0,1,0,0,1",
    "0,0,0,1,0,1,0",
    "0,0,0,1,0,1,1",
    "0,0,0,1,1,0,1",
    "0,0,0,1,1,1,0",
    "0,0,0,1,1,1,1",
    "0,0,1,0,0,0,0",
    "0,0,1,0,0,0,1",
    "0,0,1,0,0,1,0",
    "0,0,1,0,0,1,1",
    "0,0,1,0,1,0,0",
    "0,0,1,0,1,0,1",
    "0,0,1,0,1,1,0",
    "0,0,1,0,1,1,1",
    "0,0,1,1,0,0,0",
    "0,0,1,1,0,0,1",
    "0,0,1,1,0,1,0",
    "0,0,1,1,0,1,1",
    "0,0,1,1,1,0,0",
    "0,0,1,1,1,0,1",
    "0,0,1,1,1,1,0",
    "0,0,1,1,1,1,1",
    "0,1,0,0,0,0,0",
    "0,1,0,0,0,0,1",
    "0,1,0,0,0,1,0",
    "0,1,0,0,0,1,1",
    "0,1,0,0,1,0,0",
    "0,1,0,0,1,0,1",
    "0,1,0,0,1,1,0",
    "0,1,0,0,1,1,1",
    "0,1,0,1,0,0,1",
    "0,1,0,1,0,1,0",
    "0,1,0,1,0,1,1",
    "0,1,0,1,1,0,1",
    "0,1,0,1,1,1,0",
    "0,1,0,1,1,1,1",
    "0,1,1,0,0,0,0",
    "0,1,1,0,0,0,1",
    "0,1,1,0,0,1,0",
    "0,1,1,0,0,1,1",
    "0,1,1,0,1,0,0",
    "0,1,1,0,1,0,1",
    "0,1,1,0,1,1,0",
    "0,1,1,0,1,1,1",
    "0,1,1,1,0,0,0",
    "0,1,1,1,0,0,1",
    "0,1,1,1,0,1,0",
    "0,1,1,1,0,1,1",
    "0,1,1,1,1,0,0",
    "0,1,1,1,1,0,1",
    "0,1,1,1,1,1,0",
    "0,1,1,1,1,1,1",
    "1,0,0,0,0,0,0",
    "1,0,0,0,0,0,1",
    "1,0,0,0,0,1,0",
    "1,0,0,0,0,1,1",
    "1,0,0,0,1,0,0",
    "1,0,0,0,1,0,1",
    "1,0,0,0,1,1,0",
    "1,0,0,0,1,1,1",
    "1,0,0,1,0,0,1",
    "1,0,0,1,0,1,0",
    "1,0,0,1,0,1,1",
    "1,0,0,1,1,0,1",
    "1,0,0,1,1,1,0",
    "1,0,0,1,1,1,1",
    "1,0,1,0,0,0,0",
    "1,0,1,0,0,0,1",
    "1,0,1,0,0,1,0",
    "1,0,1,0,0,1,1",
    "1,0,1,0,1,0,0",
    "1,0,1,0,1,0,1",
    "1,0,1,0,1,1,0",
    "1,0,1,0,1,1,1",
    "1,0,1,1,0,0,0",
    "1,0,1,1,0,0,1",
    "1,0,1,1,0,1,0",
    "1,0,1,1,0,1,1",
    "1,0,1,1,1,0,0",
    "1,0,1,1,1,0,1",
    "1,0,1,1,1,1,0",
    "1,0,1,1,1,1,1",
    "1,1,0,0,0,0,0",
    "1,1,0,0,0,0,1",
    "1,1,0,0,0,1,0",
    "1,1,0,0,0,1,1",
    "1,1,0,0,1,0,0",
    "1,1,0,0,1,0,1",
    "1,1,0,0,1,1,0",
    "1,1,0,0,1,1,1",
    "1,1,0,1,0,0,1",
    "1,1,0,1,0,1,0",
    "1,1,0,1,0,1,1",
    "1,1,0,1,1,0,1",
    "1,1,0,1,1,1,0",
    "1,1,0,1,1,1,1",
    "1,1,1,0,0,0,0",
    "1,1,1,0,0,0,1",
    "1,1,1,0,0,1,0",
    "1,1,1,0,0,1,1",
    "1,1,1,0,1,0,0",
    "1,1,1,0,1,0,1",
    "1,1,1,0,1,1,0",
    "1,1,1,0,1,1,1",
    "1,1,1,1,0,0,0",
    "1,1,1,1,0,0,1",
    "1,1,1,1,0,1,0",
    "1,1,1,1,0,1,1",
    "1,1,1,1,1,0,0",
    "1,1,1,1,1,0,1",
    "1,1,1,1,1,1,0",
    "1,1,1,1,1,1,1"
]


def parse_icondition(c):
    icondition = c.split(",")
    icondition = [int(s) for s in icondition]
    return icondition


def test_010_non_se_condition():
    validator = ISetConditionValidator("lpmln")
    cnt = 1
    for c in non_se_010:
        icondition = parse_icondition(c)
        is_contain_valid, is_se_sat, condition = validator.validate_isets_kmn_program_from_iset_condition_return_str(
            icondition, 0, 1, 0, is_check_valid_rule=False)
        if is_se_sat:
            raise RuntimeError("wrong iset condition: %s" % condition)
        else:
            print("%d condition: %s pass!" % (cnt, condition))
        cnt += 1
    print("all test cases pass !")


def test_010_se_condition():
    validator = ISetConditionValidator("lpmln")
    cnt = 1
    for c in se_010:
        icondition = parse_icondition(c)
        is_contain_valid, is_se_sat, condition = validator.validate_isets_kmn_program_from_iset_condition_return_str(
            icondition, 0, 1, 0, is_check_valid_rule=False)
        if not is_se_sat:
            raise RuntimeError("wrong iset condition: %s" % condition)
        else:
            print("%d condition: %s pass!" % (cnt, condition))
        cnt += 1
    print("all test cases pass !")


def test_singleton_condition():
    iset_id_1 = "0b100000100100010"
    iset_id_2 = "0b000100100010100"
    rule_number = 5
    k = 0
    m = 2
    n = 3

    non_empty_ids = [int(iset_id_1, 2) - 1, int(iset_id_2, 2) - 1]
    print(non_empty_ids)

    validator = ISetConditionValidator()
    is_contain_valid, is_se_sat, condition = validator.validate_kmn_extended_iset_condition_from_non_emtpy_iset_ids_return_icondition_str(non_empty_ids, k, m, n)
    print(is_contain_valid, is_se_sat)
    print(condition)


if __name__ == '__main__':
    pass
