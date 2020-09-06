
# -*- encoding: utf-8 -*-
"""
@Time    : 2020-09-06 21:20
@Author  : 王彬
@Email   : wangbiu@foxmail.com
@File    : SingleISetSEChecker.py
"""

from lpmln.iset.ISetConditionValidator import ISetConditionValidator
import lpmln.iset.OptimizationISetsUtils as oisu


def get_non_se_isets(k_size, m_size, n_size, iset_ids, is_use_extended_rules=False, lp_type="lpmln"):
    validator = ISetConditionValidator(is_use_extended_rules=is_use_extended_rules, lp_type=lp_type)
    non_se_isets = list()
    for id in iset_ids:
        is_contain_valid_rule, is_se_sat, icondition = validator.validate_isets_kmn_program_from_non_empty_ids(
            {id}, k_size, m_size, n_size, is_check_valid_rule=False)
        if not is_se_sat:
            non_se_isets.append(id)

    if is_use_extended_rules:
        rule_set_size = 4
    else:
        rule_set_size = 3
    print("%s:%d %d-%d-%d itasks find %d non-se isets: \n\t " % (lp_type, rule_set_size, k_size, m_size, n_size, len(non_se_isets)), non_se_isets)
    return non_se_isets


def get_non_se_test_isets(rule_number, is_use_extended_rules=False):
    empty_iset_ids = oisu.get_empty_indpendent_set_ids(rule_number, is_use_extended_rules)
    if is_use_extended_rules:
        rule_set_size = 4
    else:
        rule_set_size = 3

    iset_number = 2 ** (rule_number * rule_set_size) - 1
    test_iset_ids = list()
    for i in range(iset_number):
        if i not in empty_iset_ids:
            test_iset_ids.append(i)

    print("rule number %d:%d find %d unknown isets \n\t" % (rule_number, rule_set_size, len(test_iset_ids)), test_iset_ids)
    return test_iset_ids


def get_se_single_isets(k_size, m_size, n_size, is_use_extended_rules=False, lp_type="lpmln"):
    rule_number = k_size + m_size + n_size
    if is_use_extended_rules:
        rule_set_size = 4
    else:
        rule_set_size = 3
    test_iset_ids = get_non_se_test_isets(rule_number, is_use_extended_rules)
    non_se_iset_ids = get_non_se_isets(k_size, m_size, n_size, test_iset_ids, is_use_extended_rules, lp_type)
    se_single_isets = list()
    non_se_iset_ids = set(non_se_iset_ids)
    for i in test_iset_ids:
        if i not in non_se_iset_ids:
            se_single_isets.append(i)

    print("%s:%d %d-%d-%d itasks find %d se isets: \n\t " % (
    lp_type, rule_set_size, k_size, m_size, n_size, len(se_single_isets)), se_single_isets)

    print("------------------------------------------------------------------------------")


def batch_get_se_single_isets(is_use_extended_rules=False, lp_type="lpmln"):
    kmns = [
        [0, 1, 1],
        [1, 1, 0],
        [0, 2, 1],
        [1, 1, 1],
        [1, 2, 0],
        [2, 1, 0],
    ]

    for kmn in kmns:
        get_se_single_isets(*kmn, is_use_extended_rules=is_use_extended_rules, lp_type=lp_type)


if __name__ == '__main__':
    # get_non_se_test_isets(2)
    # get_se_single_isets(0, 1, 1)
    # get_se_single_isets(1, 1, 0)
    batch_get_se_single_isets()
    pass
    